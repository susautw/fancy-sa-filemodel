import argparse
from pathlib import Path
from typing import Optional, Union, TextIO, BinaryIO, IO

from sqlalchemy import Column, Integer, String, create_engine, select, Boolean
from sqlalchemy.orm import declarative_base, scoped_session, sessionmaker, Session

from fancy.sa import filemodel

Base = declarative_base()

session: Union[Session, scoped_session] = scoped_session(sessionmaker())


def main():
    args = get_arg_parser().parse_args()
    filemodel.StorageManager.initialize(
        filemodel.FileStorage(args.files)
    )

    engine = create_engine(args.db)
    Base.metadata.create_all(engine)
    session.configure(bind=engine)

    create_files(args.file)
    session.remove()

    results: list[File] = list(session.execute(select(File)).scalars().all())
    for file in results:
        print(file)
        print(file.name)

    if results:
        with results[0].get_stream() as fp:
            print(fp.read())


def create_files(fn: Path):
    with fn.open() as fp:
        ext = fn.suffix[1:]
        with session.begin():  # simply committed
            file = File(fp, name="committed", ext=ext)
            session.add(file)

        with session.begin():  # simply rolled backed
            file = File(fp, name="rolled-back", ext=ext)
            session.add(file)
            session.rollback()

        with session.begin():
            file = File(fp, name="file1", ext=ext)
            session.add(file)
            session.flush()
            with session.begin_nested():
                file2 = File(fp, name="file2(nested1)", ext=ext)
                with session.begin_nested():
                    file3 = File(fp, name="file3(nested2)", ext=ext)
                    session.add(file3)
                with session.begin_nested():
                    file4 = File(fp, name="file4(nested2)_rolled_back", ext=ext)
                    with session.begin_nested():
                        file5 = File(fp, name="file5(nested3)_rolled_back", ext=ext)
                        session.add(file5)
                    session.add(file4)
                    session.rollback()
                session.add(file2)


def get_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("file", type=Path)
    parser.add_argument("--files", default="./files", type=Path)
    parser.add_argument("--db", default="sqlite://")

    return parser


class File(Base, filemodel.FileModel):
    __tablename__ = "file"
    id = Column(Integer, primary_key=True)
    name = Column(String(32), unique=True, nullable=False)
    ext = Column(String(10), nullable=False)
    binary = Column(Boolean, nullable=False)

    _fp: IO = None

    def __init__(self, fp: IO, **kwargs):
        self._fp = fp
        binary: bool = kwargs.pop("binary", isinstance(fp.read(0), bytes))
        super(File, self).__init__(binary=binary, **kwargs)

    def generate_filename(self) -> str:
        return f'{self.name}.{self.ext}'

    def init_stream(self) -> Optional[IO]:
        return self._fp

    def is_binary(self) -> bool:
        return self.binary


if __name__ == '__main__':
    main()
