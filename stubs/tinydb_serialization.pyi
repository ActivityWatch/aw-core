from typing import Callable
from tinydb.storages import Storage


class Serializer:
    ...


class SerializationMiddleware(Storage):
    def __init__(self, storage: Callable[..., Storage]) -> None:
        ...

    def register_serializer(self, serializer: Serializer, name: str):
        ...
