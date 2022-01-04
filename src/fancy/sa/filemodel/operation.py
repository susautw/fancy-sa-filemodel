from abc import ABC
from typing import TYPE_CHECKING
from . import Storage

if TYPE_CHECKING:
    from . import File


class Operation(ABC):
    def on_attach_context(self): ...
    def do(self): ...
    def undo(self): ...


class Insert(Operation):
    _file: "File"
    _storage: Storage

    def __init__(self, file: "File", storage: Storage):
        self._file = file
        self._storage = storage

    def on_attach_context(self):
        self._storage.store(self._file)

    def undo(self):
        self._storage.delete(self._file, missing_ok=True)


class Update(Operation):
    _file: "File"
    _new_name: str
    _storage: Storage

    def __init__(self, file: "File", new_name: str, storage: Storage):
        self._file = file
        self._new_name = new_name
        self._storage = storage

    def on_attach_context(self):
        self._storage.rename(self._file, self._new_name)

    def undo(self):
        current_file = self._file.get_model().file
        self._storage.rename(current_file, self._file.get_name())


class Delete(Operation):
    _file: "File"
    _storage: Storage

    def __init__(self, file: "File", storage: Storage):
        self._file = file
        self._storage = storage

    def on_attach_context(self):
        self._storage.mark_as_deleted(self._file, missing_ok=True)

    def do(self):
        self._storage.delete(self._file, missing_ok=True)

    def undo(self):
        self._storage.unmark_delete(self._file, missing_ok=True)
