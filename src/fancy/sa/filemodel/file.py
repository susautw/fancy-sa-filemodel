from typing import Optional

from . import Storage, StorageManager, FileModel
from . import Stream


class File:
    _storage: Storage
    _stream: Optional[Stream] = None
    _name: str
    _model: FileModel

    def __init__(
            self,
            name: str,
            model: FileModel,
            stream: Stream = None,
            storage: Storage = None,
            *, is_binary: bool
    ):
        self._name = name
        self._stream = stream
        if storage is None:
            storage = StorageManager.get_instance().storage
        self._storage = storage
        self.is_binary = is_binary
        self._model = model

    def get_name(self) -> str:
        return self._name

    def rename(self, name: str) -> None:
        self._name = name

    def get_stream(self) -> Stream:
        if self._stream is None:
            self._stream = self._storage.get_stream(self)
        return self._stream

    def get_model(self) -> FileModel:
        return self._model
