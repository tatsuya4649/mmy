from aiomysql.cursors import Cursor, _DictCursorMixin


class ProxyCursor(Cursor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def execute(self, key: str, query, args=None):
        if self._connection is None:
            raise RuntimeError("_connection attr is None")

        self._connection.create_keydata_from_key(key)
        return await super().execute(query, args)


class DictCursor(_DictCursorMixin, ProxyCursor):
    """A cursor which returns results as a dictionary"""
