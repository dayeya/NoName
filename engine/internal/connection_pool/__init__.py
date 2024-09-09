import redis


def get_connection_pool(*args, **kwargs) -> redis.ConnectionPool:
    """Creates a connection pool to redis."""
    return redis.ConnectionPool(*args, **kwargs)


def get_redis(*args, **kwargs) -> redis.StrictRedis:
    """Creates a redis client in a pool."""
    return redis.StrictRedis(*args, **kwargs)
