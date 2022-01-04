from abc import abstractmethod, ABC
from copy import copy
from datetime import datetime
from pathlib import Path
from shutil import copyfileobj
from typing import TYPE_CHECKING, Optional

from . import Stream

if TYPE_CHECKING:
    from . import File


class Storage(ABC):
    @abstractmethod
    def get_stream(self, file: "File") -> Stream:
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
    def delete_marked(self, file: "File", missing_ok=False) -> None:
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
    def rename(self, file: "File", path: str) -> "File":
        """
        :raises OSError
        """


class FileStorage(Storage):
    _base_path: Path
    _tmp_deleted_path: Path
    _deleted_path: Optional[Path]
    _seek_to_start_before_store: bool
    _rename_instead_delete: bool
    _clean_empty_sub_directory: bool

    _base_directories: set[Path]

    def __init__(
            self,
            base_path: Path,
            seek_to_start_before_store=True,
            rename_instead_delete: bool = False,
            clean_empty_sub_directory: bool = True
    ):
        self._base_path = self.create_directory(base_path)
        self._tmp_deleted_path = self.create_directory(base_path / "tmp_deleted")
        self._base_directories = {self._base_path, self._tmp_deleted_path}
        if rename_instead_delete:
            self._deleted_path = self.create_directory(base_path / "deleted")
            self._base_directories.add(self._deleted_path)
        self._seek_to_start_before_store = seek_to_start_before_store
        self._rename_instead_delete = rename_instead_delete
        self._clean_empty_sub_directory = clean_empty_sub_directory

    def create_directory(self, target: Path) -> Path:
        target.mkdir(parents=True, exist_ok=True)
        return target

    def get_stream(self, file: "File") -> Stream:
        mode = 'rb' if file.is_binary else 'r'
        return (self._base_path / file.get_name()).open(mode)

    def store(self, file: "File") -> None:
        mode = 'wb' if file.is_binary else 'w'
        fn = self._base_path / file.get_name()
        self.create_directory(fn.parent)
        if fn.exists():
            raise FileExistsError(fn)
        if self._seek_to_start_before_store and file.get_stream().seekable():
            file.get_stream().seek(0)
        with fn.open(mode) as fp:
            copyfileobj(file.get_stream(), fp)

    def mark_as_deleted(self, file: "File", missing_ok=False) -> None:
        try:
            deleted_path = self._tmp_deleted_path / file.get_name()
            self.create_directory(deleted_path.parent)
            (self._base_path / file.get_name()).rename(deleted_path)
        except FileNotFoundError:
            if not missing_ok:
                raise

    def delete_marked(self, file: "File", missing_ok=False) -> None:
        self._delete(self._tmp_deleted_path / file.get_name(), file, missing_ok)
        self._clean_directory_if_empty((self._base_path / file.get_name()).parent)

    def delete(self, file: "File", missing_ok=False) -> None:
        self._delete(self._base_path / file.get_name(), file, missing_ok)

    def _delete(self, fn: Path, file: "File", missing_ok=False) -> None:
        try:
            if self._rename_instead_delete:
                deleted_path = self._deleted_path / file.get_name()
                deleted_path.with_stem(f"deleted_at_{datetime.now().isoformat(timespec='seconds')}_{deleted_path.stem}")
                self.create_directory(deleted_path.parent)
                fn.rename(self._deleted_path / deleted_path)
            else:
                fn.unlink()
            self._clean_directory_if_empty(fn.parent)
        except FileNotFoundError:
            if not missing_ok:
                raise

    def unmark_delete(self, file: "File", missing_ok=True) -> None:
        try:
            fn = self._base_path / file.get_name()
            self.create_directory(fn.parent)
            tmp_deleted_path = self._tmp_deleted_path / file.get_name()
            tmp_deleted_path.rename(fn)
            self._clean_directory_if_empty(tmp_deleted_path.parent)
        except FileNotFoundError:
            if not missing_ok:
                raise

    def rename(self, file: "File", path: str) -> "File":
        origin_path = self._base_path / file.get_name()
        new_path = self._base_path / path
        self.create_directory(new_path.parent)

        origin_path.rename(new_path)
        self._clean_directory_if_empty(origin_path.parent)
        new_file = copy(file)
        new_file.rename(path)
        file.get_model().file = new_file
        return new_file

    def _clean_directory_if_empty(self, directory: Path) -> None:
        """
        Delete directory, if it is empty and not base directories, recursively.
        """
        if (
                directory.is_dir() and
                self._clean_empty_sub_directory and
                directory not in self._base_directories
        ):
            for _ in directory.iterdir():
                return  # directory is not empty
            else:
                parent_dir = directory.parent
                directory.unlink(missing_ok=True)
                self._clean_directory_if_empty(parent_dir)
