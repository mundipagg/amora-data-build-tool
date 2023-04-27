def patch_online_store():
    """
    Forces Feast to update Redis TTL

    issue: https://github.com/feast-dev/feast/issues/3275
    """
    from functools import wraps

    from feast.infra.online_stores.helpers import _redis_key
    from feast.infra.online_stores.redis import RedisOnlineStore

    def expire_all(func):
        @wraps(func)
        def wrapper(self, config, table, data, progress):
            func(self, config, table, data, progress)

            online_store_config = config.online_store

            client = self._get_client(online_store_config)
            project = config.project

            with client.pipeline(transaction=False) as pipe:
                for entity_key, _, _, _ in data:
                    redis_key_bin = _redis_key(
                        project,
                        entity_key,
                        entity_key_serialization_version=config.entity_key_serialization_version,
                    )
                    if online_store_config.key_ttl_seconds:
                        pipe.expire(
                            name=redis_key_bin,
                            time=online_store_config.key_ttl_seconds,
                        )
                pipe.execute()

        return wrapper

    RedisOnlineStore.online_write_batch = expire_all(
        RedisOnlineStore.online_write_batch
    )
