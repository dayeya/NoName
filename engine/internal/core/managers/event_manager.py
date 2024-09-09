#
# FinalWall - Event manager for caching events.
# Author: Dayeya
#

import redis
from engine.singleton import Singleton
from engine.time_utils import get_unix_time
from engine.internal.events import Event, AUTHORIZED_REQUEST, UNAUTHORIZED_REQUEST


ACCESS_EVENTS = "access_events"
SECURITY_EVENTS = "security_events"


class EventManager(metaclass=Singleton):
    def __init__(self, r: redis.Redis, logs=True):
        try:
            self.__logs = logs
            self.__connection = r
            self.__last_security_update, self.__last_access_update = 'N/A', 'N/A'
        except redis.exceptions.ConnectionError:
            print("Redis server is not running. Please run scripts/run_redis before deploying.")

    @property
    def service_report(self):
        """Builds a report of the EventManager."""
        access_cache_size, security_cache_size = len(self.get_access_events()), len(self.get_security_events())
        return {
            "cache_size": access_cache_size + security_cache_size,
            "access_events_size": access_cache_size,
            "security_events_size": security_cache_size,
            "last_security_update": self.__last_security_update,
            "last_access_update": self.__last_access_update
        }

    def log(self, msg: str):
        """
        Log `msg` if __logs is True.
        :param msg:
        :return:
        """
        if self.__logs:
            print(msg)

    def redis_alive(self) -> tuple:
        """Checks the connections to the redis server."""
        return self.__connection.ping()

    def cache_event(self, event: Event):
        """Sets a new entry inside the cache of client_hash and its access log."""
        namespace = ''

        if event.kind == AUTHORIZED_REQUEST:
            namespace = ACCESS_EVENTS
            self.__last_access_update = get_unix_time("Asia/Jerusalem")
        if event.kind == UNAUTHORIZED_REQUEST:
            namespace = SECURITY_EVENTS
            self.__last_security_update = get_unix_time("Asia/Jerusalem")

        self.__connection.zadd(namespace, mapping={Event.serialize(event): event.log.sys_epoch_time})

    def get_access_events(self):
        """Retrieves the access, events ordered set from the cache."""
        return [Event.deserialize(event) for event in
                self.__connection.zrange(ACCESS_EVENTS, start=0, end=-1)]

    def get_security_events(self):
        """Retrieves the security events, ordered set from the cache."""
        return [Event.deserialize(event) for event in
                self.__connection.zrange(SECURITY_EVENTS, start=0, end=-1)]
