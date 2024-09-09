#
# FinalWall - Rate limiting system using the Token Bucket algorithm.
# Author: Dayeya
#

import time
import redis

from ..connection_pool import get_redis

TOKEN = "Bucket:{ip}"
RateLimitCode = int


class TokenBucketLimiter:
    def __init__(self, pool: redis.ConnectionPool, max_burst: int, per_time: int):
        self.__r: redis.Redis = get_redis(connection_pool=pool, decode_responses=True)
        try:
            if not self.__r.ping():
                raise redis.ConnectionError
        except redis.ConnectionError:
            print("[TOKEN BUCKET] Redis is closed.")
            self.__r = None

        self.__capacity = max_burst
        self.__rate = max_burst // per_time  # Tokens per minute.

        print("[TOKEN BUCKET] Setup finished.")

    def __get_tokens(self, client_ip: str):
        if not self.__r:
            print("[REDIS CLOSED] Redis client is not available.")
            return
        name = TOKEN.format(ip=client_ip)
        current_time = time.time()

        tokens = self.__r.hget(name, "tokens")
        last_check = self.__r.hget(name, "last_check")

        if tokens is None:
            tokens = self.__capacity
            last_check = current_time
            self.__r.hsetnx(name, "tokens", tokens)
            self.__r.hsetnx(name, "last_check", last_check)
            return self.__capacity

        delta = (current_time - float(last_check))
        tokens = min(self.__capacity, int(int(tokens) + delta * self.__rate))

        self.__r.hset(name, "tokens", tokens)
        self.__r.hset(name, "last_check", current_time)
        return tokens

    def consume(self, client_ip: str, tokens_to_consume: int):
        tokens = self.__get_tokens(client_ip)
        if tokens >= tokens_to_consume:
            name = TOKEN.format(ip=client_ip)
            tokens = self.__r.hget(name, "tokens")
            last_check = time.time()
            self.__r.hset(name, "tokens", int(tokens) - tokens_to_consume)
            self.__r.hset(name, "last_check", last_check)
            return True
        return False
