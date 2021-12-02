from abc import abstractmethod, ABC
from copy import copy
from pathlib import Path
from shutil import copyfileobj
from typing import TYPE_CHECKING, IO

if TYPE_CHECKING:
    from . import File


class Storage(ABC):
    @abstractmethod
    def get_stream(self, file: "File") -> IO:
        """
        :raises OSError
        """

    @abstractmethod
    def store(self, file: "File") -> None:
        """
        :raises OSError
        """

    @abstractmethod
    def delete(self, file: "File", missing_ok=False) -> None:
        """
        :raises OSError
        """

    @abstractmethod
    def rename(self, file: "File", name: str) -> "File":
        """
        :raises OSError
        """


class FileStorage(Storage):
    _base_path: Path
    _seek_to_start_before_store: bool

    def __init__(self, base_path: Path, seek_to_start_before_store=True):
        self._base_path = base_path
        self._seek_to_start_before_store = seek_to_start_before_store

    def get_stream(self, file: "File") -> IO:
        mode = 'rb' if file.is_binary else 'r'
        return (self._base_path / file.get_name()).open(mode)

    def store(self, file: "File") -> None:
        mode = 'wb' if file.is_binary else 'w'
        fn = self._base_path / file.get_name()
        if fn.exists():
            raise FileExistsError(fn)
        if self._seek_to_start_before_store and file.get_stream().seekable():
            file.get_stream().seek(0)
        with fn.open(mode) as fp:
            copyfileobj(file.get_stream(), fp)

    def delete(self, file: "File", missing_ok=False) -> None:
        (self._base_path / file.get_name()).unlink(missing_ok=missing_ok)

    def rename(self, file: "File", name: str) -> "File":
        (self._base_path / file.get_name()).rename(name)
        new_file = copy(file)
        new_file.rename(name)
        file.get_model().file = new_file
        return new_file
