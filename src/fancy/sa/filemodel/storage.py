from abc import abstractmethod, ABC
from copy import copy
from datetime import datetime
from pathlib import Path
from shutil import copyfileobj
from typing import TYPE_CHECKING, IO, Optional

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
    def mark_as_deleted(self, file: "File", missing_ok=False) -> None:
        """
        :raises OSError
        """

    @abstractmethod
    def delete(self, file: "File", missing_ok=False) -> None:
        """
        :raises OSError
        """

    @abstractmethod
    def unmark_delete(self, file: "File", missing_ok=True) -> None:
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
    _tmp_deleted_path: Path
    _deleted_path: Optional[Path]
    _seek_to_start_before_store: bool
    _rename_instead_delete: bool

    def __init__(self, base_path: Path, seek_to_start_before_store=True, rename_instead_delete: bool = False):
        self._base_path = self.get_directory(base_path)
        self._tmp_deleted_path = self.get_directory(base_path / "tmp_deleted")
        if rename_instead_delete:
            self._deleted_path = self.get_directory(base_path / "deleted")
        self._seek_to_start_before_store = seek_to_start_before_store
        self._rename_instead_delete = rename_instead_delete

    def get_directory(self, target: Path) -> Path:
        target.mkdir(parents=True, exist_ok=True)
        return target

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

    def mark_as_deleted(self, file: "File", missing_ok=False) -> None:
        try:
            (self._base_path / file.get_name()).rename(self._tmp_deleted_path / file.get_name())
        except FileNotFoundError:
            if not missing_ok:
                raise

    def delete(self, file: "File", missing_ok=False) -> None:
        try:
            tmp_deleted = self._tmp_deleted_path / file.get_name()
            if self._rename_instead_delete:
                tmp_deleted.rename(
                    self._deleted_path / f"deleted_at_{datetime.now().isoformat(timespec='seconds')}_{file.get_name()}"
                )
            else:
                tmp_deleted.unlink()
        except FileNotFoundError:
            if not missing_ok:
                raise

    def unmark_delete(self, file: "File", missing_ok=True) -> None:
        try:
            (self._tmp_deleted_path / file.get_name()).rename(self._base_path / file.get_name())
        except FileNotFoundError:
            if not missing_ok:
                raise

    def rename(self, file: "File", name: str) -> "File":
        (self._base_path / file.get_name()).rename(name)
        new_file = copy(file)
        new_file.rename(name)
        file.get_model().file = new_file
        return new_file
