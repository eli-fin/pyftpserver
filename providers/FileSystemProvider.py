import os
import time
from providers import ProviderBase
from typing import Tuple, Callable


class FileSystemProvider(ProviderBase):
    def __init__(self, root_dir: str):
        self.root_dir = root_dir.replace('\\', '/')
        self.working_dir = self.root_dir
        if not os.path.isdir(self.root_dir):
            raise Exception('Invalid root dir: ' + self.root_dir)

    def get_work_dir(self) -> str:
        return self.working_dir

    def change_work_dir(self, new_dir: str) -> bool:
        new_dir = self.normalize_path(new_dir)
        if os.path.isdir(new_dir):
            self.working_dir = '/'+new_dir[len(self.root_dir):]
            return True
        return False

    def listdir(self):
        file_list = os.listdir(self.normalize_path(self.working_dir))
        ret = ''

        current_year = time.gmtime().tm_year
        for f in file_list:
            f_path = self.normalize_path(f)
            f_stat = os.stat(f_path)
            f_gmtime = time.gmtime(f_stat.st_mtime)
            if f_gmtime.tm_year == current_year:
                time_str = time.strftime('%b %e %H:%M', f_gmtime)
            else:
                time_str = time.strftime('%b %e %Y', f_gmtime)

            if os.path.isdir(f_path):
                f_str = f'drwxr-xr-x 2 0 0 {f_stat.st_size} {time_str} {f}\r\n'
            else:
                f_str = f'-rw-r--r-- 1 0 0 {f_stat.st_size} {time_str} {f}\r\n'

            ret += f_str

        return ret

    def get_size(self, f) -> Tuple[bool, str]:
        f = self.normalize_path(f)
        if os.path.isfile(f):
            return True, str(os.stat(f).st_size)
        else:
            return False, ''

    def normalize_path(self, path: str):
        """
        Get an absolute full path (for the OS, not the FTP server)
        """
        assert len(path), 'Empty path'
        if not path[0] == '/':
            path = self.working_dir + '/' + path
        return (self.root_dir + '/' + path).replace('//', '/').replace('//', '/')

    def get_file(self, f: str) -> bytes:
        f = self.normalize_path(f)
        return open(f, 'rb').read()

    def save_file(self, f: str, get_file_chunk: Callable[[int], bytes]):
        chunk_size = 1024*1024*50  # 50 mb

        f = self.normalize_path(f)
        data = b' '
        f = open(f, 'wb')
        i = 0
        while data:
            data = get_file_chunk(chunk_size)
            if data:
                print('FS save_file chunk ' + str(i))
                f.write(data)
                i += 1
        f.close()

    def rename_file(self, from_name: str, to_name: str):
        from_name = self.normalize_path(from_name)
        to_name = self.normalize_path(to_name)
        os.rename(from_name, to_name)

    def del_file(self, f: str):
        f = self.normalize_path(f)
        os.unlink(f)

    def mkdir(self, f: str):
        f = self.normalize_path(f)
        os.mkdir(f)

    def rmdir(self, f: str):
        f = self.normalize_path(f)
        os.rmdir(f)
