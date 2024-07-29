from typing import Generator

import redis.asyncio as redis
from redis.asyncio.sentinel import Sentinel
from opal_common.logger import logger
from pydantic import BaseModel


class RedisDB:
    """Small utility class to persist objects in Redis."""

    def __init__(self, redis_url: str, sentinel_hosts: list, sentinel_service_name: str):
        self._url = redis_url or ""
        self._sentinel_hosts = sentinel_hosts or []
        self._sentinel_service_name = sentinel_service_name or ""
        self._redis_slave = None
        if not self._url and not self._sentinel_hosts:
            raise ValueError("RedisDB: Either redis_url or sentinel_hosts must be provided")
        elif self._url and self._sentinel_hosts:
            raise ValueError("RedisDB: Only one of redis_url or sentinel_hosts must be provided")
        elif self._url:
            logger.debug("Connecting to Redis: {url}", url=self._url)

            self._redis = redis.Redis.from_url(self._url)
        elif self._sentinel_hosts:
            logger.debug("Connecting to Redis Sentinel: {hosts}, service: {service}",
                        hosts=self._sentinel_hosts, service=self._sentinel_service_name)

            self._sentinel = Sentinel(self._sentinel_hosts)
            self._redis = self._sentinel.master_for(self._sentinel_service_name)
            self._redis_slave = self._sentinel.slave_for(self._sentinel_service_name)

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
        if self._redis_slave:
            return await self._redis_slave.get(key)
        return await self._redis.get(key)

    async def scan(self, pattern: str) -> Generator[bytes, None, None]:
        cur = b"0"
        while cur:
            if self._redis_slave:
                cur, keys = await self._redis_slave.scan(cur, match=pattern)
            else:
                cur, keys = await self._redis.scan(cur, match=pattern)

            for key in keys:
                if self._redis_slave:
                    value = await self._redis_slave.get(key)
                else:
                    value = await self._redis.get(key)
                yield value

    async def delete(self, key: str):
        await self._redis.delete(key)

    def _serialize(self, value: BaseModel) -> str:
        return value.json()