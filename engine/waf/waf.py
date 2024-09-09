#
# FinalWall - WAF behavior.
# Author: Dayeya.
#

import pickle
import asyncio
import psutil
from enum import Enum
from typing import Callable
from functools import wraps
from collections import deque

from engine.config import WafConfig
from engine.errors import StateError
from engine.tunnel import Tunnel, TunnelEvent
from engine.time_utils import get_unix_time, get_epoch_time

from engine.proxy_network.anonymity import AccessList
from engine.proxy_network.geolocation import get_geoip_data, get_external_ip
from engine.proxy_network.behavior import recv_from_client, forward_data, recv_from_server

from engine.net.connection import Connection, AsyncStream
from engine.net.aionetwork import create_new_thread, create_new_task, HostAddress, REMOTE_ADDR

from engine.internal.events import (
    Event,
    Classifier,
    SecurityLog,
    AccessLog,
    CONNECTION,
    AUTHORIZED_REQUEST,
    UNAUTHORIZED_REQUEST
)
from engine.internal.tokenization import tokenize
from engine.internal.signature_db import SignatureDb
from engine.internal.rate_limiting import TokenBucketLimiter
from engine.internal.connection_pool import get_connection_pool, get_redis
from engine.internal.core import WafResponseCode
from engine.internal.core.profile import Profile
from engine.internal.core.transaction import Transaction
from engine.internal.core.managers.ban_manager import BanManager
from engine.internal.core.managers.event_manager import EventManager
from engine.internal.core.managers.profile_manager import ProfileManager
from engine.internal.core.blocking.block import create_security_page
from engine.internal.core.vuln_checks import check_transaction, validate_dirty_client, classify_by_flags


class _WafState(Enum):
    """An enum for state handling."""
    CREATED = "Created"
    DEPLOYED = "Deployed"
    WORKING = "Working"
    CLOSED = "Closed"


def register_event(event: TunnelEvent):
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            await func(self, *args, **kwargs)
            if not self.tunneling:
                return

            data = b""
            if event == TunnelEvent.AccessLogUpdate:
                data = self.get_authorized_events()
            elif event == TunnelEvent.SecurityLogUpdate:
                data = {"events": self.get_security_events(), "distribution": self.get_attack_distribution()}
            elif event == TunnelEvent.WafServicesUpdate:
                data = self.get_services_report()
            elif event == TunnelEvent.WafHealthUpdate:
                data = self.get_health()

            async with asyncio.Lock():
                await self.tunnel.register_event(pickle.dumps((event, data)))
        return wrapper
    return decorator


class Waf:
    """
    A class representing a Web application firewall.
    Protects a *single* entity in the network.
    """
    def __init__(
            self,
            conf: WafConfig,
            ucid: int,
            local: bool=False,
            with_tunneling: bool=False,
    ) -> None:
        try:
            SignatureDb()
        except Exception as _database_loading_err:
            print("ERROR: could not initialize database due:", _database_loading_err)

        self.__ucid = ucid
        self.__config = conf
        self.__local = local
        self.__server = None
        self.__deploy_time = None
        self.__health = deque([])
        self.__with_tunneling = with_tunneling
        self.__profile_manager = ProfileManager()
        self.__tunnel = Tunnel(conf.admin["backend_api"])
        self.__connection_pool = get_connection_pool(host=conf.waf["redis_host"], port=conf.waf["redis_port"], db=0)
        self.__rate_limiter = TokenBucketLimiter(self.__connection_pool, conf.ratelimit["max_burst"], conf.ratelimit["interval"])
        self.__ban_manager = BanManager(get_redis(connection_pool=self.__connection_pool, decode_responses=True))
        self.__event_manager = EventManager(get_redis(connection_pool=self.__connection_pool, decode_responses=True))
        self.__acl = AccessList(main_list=[],
                                api=conf.acl["api"],
                                interval=conf.acl["interval"],
                                backup=conf.acl["backup"])
        self.__address = (conf.waf["ip"], conf.waf["port"])
        self.__target = (conf.webserver["ip"], conf.webserver["port"])
        self.__state = _WafState.CREATED
        print(f"INFO: Waf created")

    @property
    def ucid(self) -> int:
        return self.__ucid

    @property
    def local(self) -> bool:
        return self.__local

    @property
    def state(self) -> _WafState:
        return self.__state

    @property
    def config(self) -> WafConfig:
        return self.__config

    @property
    def target(self) -> tuple[str, int]:
        return self.__target

    @property
    def address(self) -> tuple[str, int]:
        return self.__address

    @property
    def tunneling(self):
        return self.__with_tunneling

    @property
    def tunnel(self) -> Tunnel:
        return self.__tunnel

    @property
    def waf_report(self) -> dict:
        return {
            "waf_host": self.__address[0],
            "waf_port": self.__address[1],
            "state": self.__state.name,
            "deploy_time": self.__deploy_time,
            "organization_server_host": self.__target[0],
            "organization_server_port": self.__target[1],
            "environment": "Local" if self.__local else "Public"
        }

    async def __cpu_loop(self):
        """Sends a CPU usage every second."""
        while True:
            self.__health.append(self.get_health())
            if len(self.__health) > 60:
                self.__health.popleft()
            async with asyncio.Lock():
                if self.__with_tunneling and self.__tunnel.connected:
                    event = (TunnelEvent.WafHealthUpdate, list(self.__health))
                    await self.__tunnel.register_event(pickle.dumps(event))

    def can_transfer(self, client: Connection):
        """Checks is the client is rate limited."""
        return self.__rate_limiter.consume(client.ip, self.__config.ratelimit["consumeRate"])

    def __set_connection_profile(self, client: Connection):
        """
        Creates a new profile with a last_event of connection (only if the client is not in the db).
        :param client:
        :return:
        """
        event = Event(kind=CONNECTION, id=tokenize(), log=None, request=None, response=None)
        profile = Profile(
            host=client.ip,
            connection_date=get_unix_time(self.__config.timezone["time_zone"]),
            last_used_port=client.port,
            last_connection_time=get_epoch_time(),
            last_event=event,
            attempted_attacks=0,
            last_attempted_attack=""
        )
        with self.__profile_manager as pm:
            pm.insert_profile(client.hash, profile)

    @register_event(event=TunnelEvent.AccessLogUpdate)
    async def __handle_authorized_client(self, client: Connection, event: Event):
        """
        Handles an authorized client.
        :param client:
        :param event:
        :return:
        """
        # Update the profile.
        with self.__profile_manager as pm:
            profile_updates = {
                "last_used_port": client.port,
                "last_connection_time": event.log.sys_epoch_time,
                "last_event": event
            }
            pm.update_profile(client.hash, profile_updates)

        # Connect to the web server.
        stream = await AsyncStream.open_stream(*self.__target)
        web_server = Connection(stream=stream, addr=HostAddress(*self.__target))

        # Forward the clients request.
        await forward_data(web_server, event.request.raw)
        response = await recv_from_server(web_server)
        await forward_data(client, response)
        self.__event_manager.cache_event(event)

    @register_event(TunnelEvent.SecurityLogUpdate)
    async def __handle_unauthorized_client(self, client: Connection, event: Event, unique_response: WafResponseCode):
        """
        Handles an event made by an unauthorized user.
        :param event:
        :return:
        """
        # Update the profile.
        with self.__profile_manager as pm:
            profile: Profile = pm.get_profile_by_hash(client.hash)
            profile_updates = {
                "last_used_port": client.port,
                "last_connection_time": get_epoch_time(),
                "last_event": event,
                "attempted_attacks": profile.attempted_attacks + 1,
                "last_attempted_attack": ",".join(event.log.classifiers)
            }
            pm.update_profile(client.hash, profile_updates)

        # If this __handle is called, then client is not banned.
        # Ban the client and set correct duration.
        profile = self.__profile_manager.get_profile_by_hash(client.hash)

        if profile.attempted_attacks > self.__config.banning["threshold"]:  # exceeded the threshold.
            banned_at = get_epoch_time()
            ban_duration = profile.attempted_attacks * self.__config.banning["factor"]
            self.__ban_manager.insert_mapping(client.hash, banned_at, float(ban_duration))

        # Build a security page.
        further_information = ""
        security_page_header = ""
        match event.log.classifiers:
            case [Classifier.SqlInjection | Classifier.XSS | Classifier.UnauthorizedAccess | Classifier.BannedAccess]:
                security_page_header = self.__config.securitypage["attack_header"]
                further_information = self.__config.securitypage["attack_additional_info"]
            case [Classifier.Anonymity]:
                security_page_header = self.__config.securitypage["anonymity_header"]
                further_information = self.__config.securitypage["anonymity_additional_info"]
            case [Classifier.BannedGeolocation]:
                security_page_header = self.__config.securitypage["geo_header"]
                further_information = self.__config.securitypage["geo_additional_info"]
            case [Classifier.Anonymity, Classifier.BannedGeolocation]:
                security_page_header = self.__config.securitypage["dirty_header"]
                further_information = self.__config.securitypage["dirty_additional_info"]

        self.__event_manager.cache_event(event)
        security_page: bytes = create_security_page(info={
            "header": security_page_header,
            "further_information": further_information,
            "token": event.id
        }, waf_response=unique_response)
        await forward_data(client, security_page)

    @register_event(TunnelEvent.WafServicesUpdate)
    async def __handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """
        Handles each request by a single client.
        :param reader:
        :param writer:
        :return: None
        """
        # Change the IP of the connection to the external ip of the Waf instance.
        # This change happens only when the Waf instance is configured to be a local cluster.
        ip, port = writer.get_extra_info(REMOTE_ADDR)

        if self.__local:
            ip = get_external_ip()

        client = Connection(stream=AsyncStream(reader, writer), addr=HostAddress(ip, port))
        self.__set_connection_profile(client)

        if not self.can_transfer(client):
            security_log = SecurityLog(
                ip=client.ip,
                port=client.port,
                download=True,
                sys_epoch_time=get_epoch_time(),
                creation_date=get_unix_time(self.__config.timezone["time_zone"]),
                classifiers=[Classifier.RateLimited],
                geolocation=get_geoip_data(client.ip)
            )
            event = Event(
                kind=UNAUTHORIZED_REQUEST,
                id=tokenize(),
                log=security_log,
                request=None,
                response=None,
            )
            await self.__handle_unauthorized_client(client, event, unique_response=WafResponseCode.rateLimited)
            return

        if self.__ban_manager.banned(client.hash):
            security_log = SecurityLog(
                ip=client.ip,
                port=client.port,
                download=True,
                sys_epoch_time=get_epoch_time(),
                creation_date=get_unix_time(self.__config.timezone["time_zone"]),
                classifiers=[Classifier.BannedAccess],
                geolocation=get_geoip_data(client.ip)
            )
            event = Event(
                kind=UNAUTHORIZED_REQUEST,
                id=tokenize(),
                log=security_log,
                request=None,
                response=None,
            )
            await self.__handle_unauthorized_client(client, event, unique_response=WafResponseCode.regular)
            return

        access_list = self.__acl
        banned_countries = self.__config.geoip["banned_countries"]

        flags = validate_dirty_client(client.ip, access_list, banned_countries)

        if flags != 0:
            log_classifiers = classify_by_flags(flags)
            security_log = SecurityLog(
                ip=client.ip,
                port=client.port,
                download=True,
                sys_epoch_time=get_epoch_time(),
                creation_date=get_unix_time(self.__config.timezone["time_zone"]),
                classifiers=log_classifiers,
                geolocation=get_geoip_data(client.ip)
            )
            event = Event(
                kind=UNAUTHORIZED_REQUEST,
                id=tokenize(),
                log=security_log,
                request=None,
                response=None,
            )
            await self.__handle_unauthorized_client(client, event, unique_response=WafResponseCode.regular)
            return

        request = await recv_from_client(client, execption_callback=client.close)
        tx: Transaction = Transaction(
            owner=client.addr,
            real_host_address=None,
            raw=request,
            creation_date=get_unix_time(self.__config.timezone["time_zone"])
        )
        tx.process()

        access_list = self.__acl
        banned_countries = self.__config.geoip["banned_countries"]
        check_result = await check_transaction(tx, access_list, banned_countries)

        if check_result.result:
            security_log = SecurityLog(
                ip=client.ip,
                port=client.port,
                download=True,
                sys_epoch_time=get_epoch_time(),
                creation_date=get_unix_time(self.__config.timezone["time_zone"]),
                classifiers=check_result.classifiers,
                geolocation=get_geoip_data(client.ip)
            )
            event = Event(
                kind=UNAUTHORIZED_REQUEST,
                id=tokenize(),
                log=security_log,
                request=tx,
                response=None,
            )
            await self.__handle_unauthorized_client(client, event, unique_response=WafResponseCode.regular)
            return

        access_log = AccessLog(
            ip=client.ip,
            port=client.port,
            download=True,
            geolocation=get_geoip_data(client.ip),
            sys_epoch_time=get_epoch_time(),
            creation_date=get_unix_time(self.__config.timezone["time_zone"])
        )
        event = Event(
            kind=AUTHORIZED_REQUEST,
            id=str(tx.hash),
            log=access_log,
            request=tx,
            response=None
        )
        await self.__handle_authorized_client(client, event)

    @classmethod
    def get_health(cls) -> float:
        """Returns the current CPU usage."""
        return psutil.cpu_percent(1)

    def get_services_report(self):
        """Returns the report of the services."""
        waf_report = self.waf_report
        redis_report = self.__event_manager.service_report
        sqlite_report = self.__profile_manager.service_report
        return redis_report | sqlite_report | waf_report

    def get_authorized_events(self) -> list:
        """Retrieves all the authorized events until this point in time."""
        return self.__event_manager.get_access_events()

    def get_security_events(self) -> list:
        """Retrieves all the unauthorized events until this point in time."""
        return self.__event_manager.get_security_events()

    def get_attack_distribution(self):
        """Returns a dictionary that maps a vulnerability with its distribution."""
        print(self.get_security_events())
        distributions = [event.log.classifiers[0] for event in self.get_security_events()]
        return {classifier: distributions.count(classifier) for classifier in Classifier}

    def start_acl_loop(self):
        """Starts the Waf ACL loop."""
        self.__acl.activity_loop()

    def start_cpu_loop(self):
        """Starts the health monitor for the frontend."""
        if not self.__with_tunneling:
            return
        asyncio.run(self.__cpu_loop())

    @register_event(TunnelEvent.WafServicesUpdate)
    async def deploy(self) -> None:
        """
        Creates an Asyncio.Server with corresponding configurations.
        Note:
            Waf.deploy() *DOES NOT* run the server.
        :return: None
        """
        if self.__state is _WafState.DEPLOYED:
            raise StateError("Instance already deployed")
        if self.__state is _WafState.WORKING:
            raise StateError("Instance is working, try Waf.close() then Waf.deploy()")
        if self.__state is _WafState.CLOSED:
            raise StateError("Instance is closed, try Waf.restart()")

        # Deploy instance, state is _Waf.CREATED
        self.__server: asyncio.Server = await asyncio.start_server(
            client_connected_cb=self.__handle_connection,
            host=self.__config.waf["ip"],
            port=self.__config.waf["port"],
            start_serving=False
        )
        self.__state = _WafState.DEPLOYED
        print(f"INFO: Waf deployed AT: {self.__address!s}, FOR: {self.__target!s}")

    @register_event(TunnelEvent.WafServicesUpdate)
    async def work(self):
        """
        Starts the main working task of the Waf - making it available to handle connections.
        Note:
            Waf serves forever and will stop when the coroutine is cancelled.
        :return: None
        """
        if self.__state is _WafState.CREATED:
            raise StateError("Instance is created, try Waf.deploy() before running")
        if self.__state is _WafState.WORKING:
            raise StateError("Instance is working")
        if self.__state is _WafState.CLOSED:
            raise StateError("Instance is closed, try Waf.restart()")

        # Run instance, state is _WafState.DEPLOYED.
        async with self.__server:
            acl_thread = create_new_thread(func=self.start_acl_loop, args=(), daemon=True)
            acl_thread.start()

            if self.__with_tunneling:
                cpu_thread = create_new_thread(func=self.start_cpu_loop, args=(), daemon=True)
                cpu_thread.start()

            work = [
                create_new_task(task_name="WAF_WORKER", task=self.__server.serve_forever, args=()),
                create_new_task(task_name="WAF_TUNNEL_CONNECT", task=self.__tunnel.connect, args=())
            ]

            # Start serving.
            print(f"INFO: Waf listening AT {self.__address}")
            self.__state = _WafState.WORKING
            self.__deploy_time = get_unix_time(self.__config.timezone["time_zone"])

            await asyncio.gather(*work if self.__with_tunneling else [work[0]])

    async def restart(self):
        """
        Restarts the Waf.
        :return: None
        """
        if self.__state is not _WafState.CLOSED:
            raise StateError("Instance is not closed")
        await self.deploy()
        await self.work()

    @register_event(TunnelEvent.WafServicesUpdate)
    async def close(self):
        """
        Closes the Waf.
        :return: None
        """
        if self.__state is _WafState.CLOSED:
            return
        self.__server.close()
        await self.__server.wait_closed()
        self.__state = _WafState.CLOSED
        print("INFO: Waf closed")
