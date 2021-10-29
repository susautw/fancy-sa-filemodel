from typing import Optional, Union, TextIO, BinaryIO

from . import Storage, StorageManager, FileModel


class File:
    _storage: Storage
    _stream: Optional[Union[TextIO, BinaryIO]] = None
    _name: str
    is_binary: bool
    _meta: FileModel

    def __init__(
            self,
            name: str,
            meta: FileModel,
            stream: Union[TextIO, BinaryIO] = None,
            storage: Storage = None,
            *, is_binary: bool = False
    ):
        self._name = name
        self._stream = stream
        if storage is None:
            storage = StorageManager.get_instance().storage
        self._storage = storage
        self.is_binary = is_binary
        self._meta = meta

    def get_name(self) -> str:
        return self._name

    def rename(self, name: str) -> None:
        self._name = name

    def get_stream(self) -> Union[TextIO, BinaryIO]:
        if self._stream is None:
            self._stream = self._storage.get_stream(self)
        return self._stream

    def get_meta(self) -> FileModel:
        return self._meta
