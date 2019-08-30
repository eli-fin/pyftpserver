import time
import json
import urllib.request
import http.client
from providers import ProviderBase
from typing import Tuple, Callable

# dropbox api doc: https://www.dropbox.com/developers/documentation/http/documentation


class DropboxProvider(ProviderBase):
    def __init__(self, token: str):
        """
        To get a token, go to https://www.dropbox.com/developers/apps and create an API app (full dropbox),
        then click 'Generated access token'
        """

        self.token = token
        self.working_dir = '/'
        # Create the opener and remove default error handlers which raise for non 2XX status codes
        self.opener = urllib.request.build_opener()
        for processor in self.opener.process_response['https']:
            if isinstance(processor, urllib.request.HTTPErrorProcessor):
                self.opener.process_response['https'].remove(processor)
                break  # there's only one such handler by default

    def make_api_request(self, api, data, method):
        """ Make a dropbox request, return (code, body as json) """

        headers = {'Authorization': f'Bearer {self.token}', 'Content-Type': 'application/json'}
        req = urllib.request.Request('https://api.dropboxapi.com/2/' + api, data=json.dumps(data).encode(),
                                     headers=headers, method=method)
        resp: http.client.HTTPResponse = self.opener.open(req)

        return resp.getcode(), resp.read()

    def make_content_request(self, api, api_arg,  method, headers=None, data: bytes=None):
        """ Make a dropbox request, return (code, body as json) """

        _headers = {'Authorization': f'Bearer {self.token}', 'Dropbox-API-Arg': json.dumps(api_arg)}
        _headers.update(headers or {})
        req = urllib.request.Request('https://content.dropboxapi.com/2/' + api, headers=_headers,
                                     method=method, data=data)
        resp: http.client.HTTPResponse = self.opener.open(req)

        return resp.getcode(), resp.read()

    def get_work_dir(self) -> str:
        return self.working_dir

    def change_work_dir(self, new_dir: str) -> bool:
        new_dir = self.normalize_path(new_dir)

        # check if this folder exists
        if new_dir == '/':
            return True

        code, data = self.make_api_request('files/get_metadata', {'path': new_dir}, 'POST')
        if code != 200:
            print(f'change_work_dir error: code={code}, data={data}')
            return False

        if json.loads(data)['.tag'] == 'folder':
            self.working_dir = new_dir
            return True
        return False

    def listdir(self):
        code, data = self.make_api_request('files/list_folder',
                                           {'path': '' if self.working_dir == '/' else self.working_dir}, 'POST')
        if code != 200:
            raise Exception(f'listdir error: code={code}, data={data}')

        data = json.loads(data)
        if data['has_more']:
            print('listdir warning: data has_more')

        ret = ''
        current_year = time.gmtime().tm_year
        for f in data['entries']:
            if f['.tag'] == 'folder':
                f_gmtime = time.gmtime(0)
                f_size = 0
            else:
                f_gmtime = time.strptime(f['client_modified'], "%Y-%m-%dT%H:%M:%SZ")
                f_size = f["size"]
            if f_gmtime.tm_year == current_year:
                time_str = time.strftime('%b %e %H:%M', f_gmtime)
            else:
                time_str = time.strftime('%b %e %Y', f_gmtime)

            if f['.tag'] == 'folder':
                f_str = f'drwxr-xr-x 2 0 0 {f_size} {time_str} {f["name"]}\r\n'
            else:
                f_str = f'-rw-r--r-- 1 0 0 {f_size} {time_str} {f["name"]}\r\n'

            ret += f_str

        return ret

    def get_size(self, f) -> Tuple[bool, str]:
        f = self.normalize_path(f)

        if f == '/':
            return False, ''

        code, data = self.make_api_request('files/get_metadata', {'path': f}, 'POST')
        if code != 200:
            print(f'get_size error: code={code}, data={data}')
            return False, ''
        data = json.loads(data)

        if data['.tag'] == 'file':
            return True, data['size']
        return False, ''

    def normalize_path(self, path: str):
        """
        Get an absolute full path
        """
        assert len(path), 'Empty path'
        if not path[0] == '/':
            path = self.working_dir + '/' + path
        ret = path.replace('//', '/')
        return ret if ret == '/' else ret.rstrip('/')

    def get_file(self, f: str) -> bytes:
        f = self.normalize_path(f)
        code, data = self.make_content_request('files/download', {'path': f}, 'POST')
        if code != 200:
            raise Exception(f'get_file error: code={code}, data={data}')
        return data

    def save_file(self, f: str, get_file_chunk: Callable[[int], bytes]):
        upload_chunk_size = 1024*1024*10  # 10 mb

        f = self.normalize_path(f)

        # start upload session
        headers = {'Content-Type': 'application/octet-stream'}
        code, data = self.make_content_request('files/upload_session/start', {'close': False}, 'POST', headers=headers)
        if code != 200:
            raise Exception(f'save_file start error: code={code}, data={data}')
        session_id = json.loads(data)['session_id']

        # upload in chunks
        i = 0
        f_bytes = b' '
        f_len = 0
        while f_bytes:
            f_bytes = get_file_chunk(upload_chunk_size)
            f_len += len(f_bytes)
            if f_bytes:
                print('Dropbox save_file chunk ' + str(i))
                api_args = {'cursor': {'session_id': session_id, 'offset': i*upload_chunk_size}, 'close': False}
                code, data = self.make_content_request('files/upload_session/append_v2', api_args, 'POST',
                                                       headers=headers, data=f_bytes)
                if code != 200:
                    raise Exception(f'save_file append error: code={code}, data={data}')
                i += 1

        # end session
        api_args = {'cursor': {'session_id': session_id, 'offset': f_len}, 'commit': {'path': f}}
        code, data = self.make_content_request('files/upload_session/finish', api_args, 'POST', headers=headers)
        if code != 200:
            raise Exception(f'save_file finish error: code={code}, data={data}')

    def rename_file(self, from_name: str, to_name: str):
        from_name = self.normalize_path(from_name)
        to_name = self.normalize_path(to_name)
        code, data = self.make_api_request('files/move_v2', {'from_path': from_name, 'to_path': to_name}, 'POST')
        if code != 200:
            print(f'rename_file error: code={code}, data={data}')
            raise Exception()

    def del_file(self, f: str):
        f = self.normalize_path(f)
        code, data = self.make_api_request('files/delete_v2', {'path': f}, 'POST')
        if code != 200:
            print(f'del_file error: code={code}, data={data}')
            raise Exception()

    def mkdir(self, f: str):
        f = self.normalize_path(f)
        code, data = self.make_api_request('files/create_folder_v2', {'path': f}, 'POST')
        if code != 200:
            print(f'mkdir error: code={code}, data={data}')
            raise Exception()

    def rmdir(self, f: str):
        self.del_file(f)
