from typing import TYPE_CHECKING, Optional, Union, BinaryIO, TextIO

Stream = Union[BinaryIO, TextIO]

if TYPE_CHECKING:
    from . import File


class FileModel:
    _file: Optional["File"] = None
    _stream: Optional[Stream] = None

    @property
    def file(self) -> "File":
        from . import File
        if self._file is None:
            self._file = File(
                name=self.generate_filename(),
                model=self,
                stream=self.init_stream(),
                is_binary=self.is_binary()
            )
        return self._file

    @file.setter
    def file(self, file: "File") -> None:
        self._file = file

    def generate_filename(self) -> str:
        """
        Generate a unique filename for this file model.
        `If you want to generate filename by id, the id should be assigned in __init__`
        :return:
        """
        raise NotImplementedError()

    def init_stream(self) -> Optional[Stream]:
        """
        Initialize the stream while creating a filemodel.File.
        `If the file model is loading from database, this method should return None.`
        """
        raise NotImplementedError()

    def is_binary(self) -> bool:
        """
        If return True, this file will be opened as the binary mode.
        """
        raise NotImplementedError()

    def get_stream(self) -> Stream:
        return self.file.get_stream()
