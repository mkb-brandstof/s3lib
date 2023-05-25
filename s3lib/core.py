"""Class to extend pathlib.Path for the S3 filesystem.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import (
    Union,
    TextIO,
    BinaryIO,
    List,
    Iterator,
    Any,
)
import pathlib
import codecs
import os
import io

import boto3


Base = pathlib.WindowsPath if os.name == "nt" else pathlib.PosixPath


class S3Path(Base):
    """Extension of pathlib.Path for the S3 filesystem."""

    def __new__(cls, *args: str, **kwargs) -> S3Path:
        return super()._from_parts(args)

    @property
    def bucket(self) -> str:
        """Returns name of bucket.

        Returns:
            str: Bucket name.
        """
        return super().__str__().split("/", 1)[0]

    @property
    def key(self) -> str:
        """Returns the current directory key.

        Returns:
            str: The directory key.
        """
        try:
            _key = f"{super().__str__().split('/', 1)[1]}"
        except IndexError:
            _key = ""
        if self.suffix == "":
            _key += "/"
        return _key

    def __repr__(self) -> str:
        return f"S3Path({str(self)})"

    def __str__(self) -> str:
        return f"s3://{super().__str__()}".replace("\\", "/")

    @contextmanager
    def open(
        self, mode: str = "rb"
    ) -> Union[TextIO, BinaryIO]:  # pylint: disable=arguments-differ
        """Open the file under the current path to read or write.

        Args:
            mode (str, optional): Defaults to "rb".

        Yields:
            TextIO | BinaryIO: The file.
        """
        assert mode in {"rb", "w", "wb"}
        s3_client = boto3.client("s3")
        if mode == "rb":
            yield s3_client.get_object(Bucket=self.bucket, Key=self.key)["Body"]
        elif mode == "wb":
            file = io.BytesIO()
            yield file
            file.seek(0)
            s3_client.upload_fileobj(file, self.bucket, self.key)
        else:
            stream_writer = codecs.getwriter("utf-8")
            file = io.BytesIO()
            yield stream_writer(file)
            file.seek(0)
            s3_client.upload_fileobj(file, self.bucket, self.key)

    @property
    def parent(self) -> S3Path:
        """Returns the parent path.

        Returns:
            S3Path: The parent path.
        """
        return self.__class__(super().parent)

    @property
    def parents(self) -> List[S3Path]:
        """Returns a list of all parent paths.

        Returns:
            List[S3Path]: All parent paths.
        """
        return [self.__class__(parent) for parent in super().parents]

    def iterdir(self) -> Iterator[S3Path]:
        """Iterate over current directory.

        Yields:
            S3Path: Path in current directory.
        """
        s3_client = boto3.resource("s3")
        bucket = s3_client.Bucket(self.bucket)
        for obj in bucket.objects.filter(Prefix=self.key):
            yield self.__class__(self.bucket, obj.key)

    def rglob(self, pattern: str = None) -> Iterator[S3Path]:
        """Iterate over current directory recursively.

        Yields:
            S3Path: Path in current directory.
        """
        # TODO: implement rglob correctly
        s3_client = boto3.resource("s3")
        bucket = s3_client.Bucket(self.bucket)
        for obj in bucket.objects.filter(Prefix=self.key):
            yield self.__class__(self.bucket, obj.key)

    def mkdir(self, *args: Any, **kwargs: Any) -> None:
        """Not implemented for S3Path.
        Callable because of compatibility with pathlib.Path usage.
        """
        return NotImplemented

    def exists(self) -> bool:
        """Check if the current path exists.

        Returns:
            bool: True if path exists.
        """
        return self in {
            parent for path in self.parent.iterdir() for parent in path.parents
        }

    def is_file(self) -> bool:
        """Check if path is file.

        Returns:
            bool: True if path is file.
        """
        if self.suffix == "":
            return False
        return self in self.parent.iterdir()

    def unlink(self) -> None:  # pylint: disable=arguments-differ
        """Delete the current file."""
        assert self.is_file()
        s3_client = boto3.resource("s3")
        bucket = s3_client.Bucket(self.bucket)
        bucket.objects.filter(Prefix=self.key).delete()

    def rmdir(self, rm_files: bool = False) -> None:
        """Delete the current directory.

        Args:
            rm_files (bool, optional): Whether to also remove all files in the directory.
                                       Defaults to False.
        """
        assert not self.is_file()
        if not self.exists():
            return
        if not rm_files:
            for path in self.iterdir():
                assert not path.is_file()
        s3_client = boto3.resource("s3")
        bucket = s3_client.Bucket(self.bucket)
        bucket.objects.filter(Prefix=self.key).delete()

    def copy(self, *, dst: S3Path) -> None:
        """Copy contents of path to destination.

        Args:
            dst (S3Path): The destination path.
        """
        s3_client = boto3.resource("s3")
        for path in self.iterdir():
            if path.stem.startswith("_"):
                continue
            copy_source = {"Bucket": path.bucket, "Key": path.key}
            s3_client.meta.client.copy(
                copy_source, dst.bucket, path.key.replace(self.key, dst.key)
            )
