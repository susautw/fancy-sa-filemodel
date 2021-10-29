from typing import TextIO, BinaryIO, Union, TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from . import File


class FileModel:
    _file: Optional["File"] = None
    _stream: Optional[Union[TextIO, BinaryIO]] = None

    @property
    def file(self) -> "File":
        from . import File
        if self._file is None:
            self._file = File(
                name=self.generate_filename(),
                meta=self,
                stream=self._init_stream(),
                is_binary=self.is_binary()
            )
        return self._file

    @file.setter
    def file(self, file: "File") -> None:
        self._file = file

    def generate_filename(self) -> str:
        raise NotImplementedError()

    def _init_stream(self) -> Optional[Union[TextIO, BinaryIO]]:
        raise NotImplementedError()

    def is_binary(self) -> bool:
        raise NotImplementedError()

    def get_stream(self):
        return self.file.get_stream()
