from typing import AsyncGenerator, Generator, Optional

import redis.asyncio as redis
from redis.asyncio.sentinel import Sentinel
from opal_common.logger import logger
from pydantic import BaseModel


def create_redis_instance(redis_url: Optional[str],
                          sentinel_hosts: Optional[list],
                          sentinel_service_name: Optional[str]):
    _url = redis_url or ""
    _sentinel_hosts = sentinel_hosts or []
    _sentinel_service_name = sentinel_service_name or ""
    if not _url and not _sentinel_hosts:
        raise ValueError("RedisDB: Either redis_url or sentinel_hosts must be provided")
    elif _url and _sentinel_hosts:
        raise ValueError("RedisDB: Only one of redis_url or sentinel_hosts must be provided")
    elif _url:
        return RedisDBUrl(_url)
    elif _sentinel_hosts and _sentinel_service_name:
        return RedisDBSentinel(_sentinel_hosts, _sentinel_service_name)
    else:
        raise ValueError("RedisDB: Invalid configuration")
        

class RedisDBSentinel:
    def __init__(self, sentinel_hosts: list, sentinel_service_name: str):
        self._sentinel_hosts = sentinel_hosts or []
        self._sentinel_service_name = sentinel_service_name or ""
        logger.debug("Connecting to Redis Sentinel: {hosts}, service: {service}",
                    hosts=self._sentinel_hosts, service=self._sentinel_service_name)

        _sentinel = Sentinel(self._sentinel_hosts)
        self._redis = _sentinel.master_for(self._sentinel_service_name)
        self._redis_slave = _sentinel.slave_for(self._sentinel_service_name)
    @property
    def redis_connection(self) -> redis.Redis:
        return self._redis

    async def set(self, key: str, value: BaseModel):
        await self._redis.set(key, self._serialize(value))

    async def set_if_not_exists(self, key: str, value: BaseModel) -> bool:
        """
        :param key:
        :param value:
        :return: True if created, False if key already exists
        """
        return await self._redis.set(key, self._serialize(value), nx=True)

    async def get(self, key: str) -> bytes:
        return await self._redis_slave.get(key)

    async def scan(self, pattern: str) -> AsyncGenerator[bytes, None, None]:
        cur = b"0"
        while cur:
            cur, keys = await self._redis_slave.scan(cur, match=pattern)

            for key in keys:
                value = await self._redis_slave.get(key)
                yield value

    async def delete(self, key: str):
        await self._redis.delete(key)

    def _serialize(self, value: BaseModel) -> str:
        return value.json()


class RedisDBUrl:
    """Small utility class to persist objects in Redis."""

    def __init__(self, redis_url: str):
        self._url = redis_url
        logger.debug("Connecting to Redis: {url}", url=self._url)

        self._redis = redis.Redis.from_url(self._url)

    @property
    def redis_connection(self) -> redis.Redis:
        return self._redis

    async def set(self, key: str, value: BaseModel):
        await self._redis.set(key, self._serialize(value))

    async def set_if_not_exists(self, key: str, value: BaseModel) -> bool:
        """
        :param key:
        :param value:
        :return: True if created, False if key already exists
        """
        return await self._redis.set(key, self._serialize(value), nx=True)

    async def get(self, key: str) -> bytes:
        return await self._redis.get(key)

    async def scan(self, pattern: str) -> AsyncGenerator[bytes, None, None]:
        cur = b"0"
        while cur:
            cur, keys = await self._redis.scan(cur, match=pattern)

            for key in keys:
                value = await self._redis.get(key)
                yield value

    async def delete(self, key: str):
        await self._redis.delete(key)

    def _serialize(self, value: BaseModel) -> str:
        return value.json()