from abc import ABC, abstractmethod
from typing import Tuple, Callable


class ProviderBase(ABC):
    """
    Provider should provide a FS api to support the server
    (listing viewing and adding files, etc), regardless of what the actual backing is
    (OS file system, zip, online service such as GDrive or DropBox, or even another FTP server)
    """

    @abstractmethod
    def get_work_dir(self):
        """
        Get the current working director
        """

    @abstractmethod
    def change_work_dir(self, new_dir: str) -> bool:
        """
        Change the current working director, return if succeeded
        """

    @abstractmethod
    def listdir(self):
        """
        List the current directory in the format of 'ls -l' with CRLF line separators
        """

    @abstractmethod
    def get_size(self, f: str) -> Tuple[bool, str]:
        """
        Get the file size of f
        :returns A tuple (succeeded, result_size)
        """

    @abstractmethod
    def get_file(self, f: str) -> bytes:
        """
        Get the binary content of f
        """

    @abstractmethod
    def save_file(self, f: str, get_file_chunk: Callable[[int], bytes]):
        """
        Create file f, using get_file_chunk till it returns None
        """

    @abstractmethod
    def rename_file(self, from_name: str, to_name: str):
        """
        Rename from_name to to_name
        """

    @abstractmethod
    def del_file(self, f: str):
        """
        Deletes file f
        """

    @abstractmethod
    def mkdir(self, f: str):
        """
        Create directory f
        """

    @abstractmethod
    def rmdir(self, f: str):
        """
        Removes directory f
        """
