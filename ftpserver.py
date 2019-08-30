"""
FTP server (required passive mode)
"""

import sys
import time
import socket
import threading
import traceback
from typing import Callable, Tuple
from providers import ProviderBase, FileSystemProvider, DropboxProvider


class Server:
    """
    The server class, listens and accepts clients
    """

    SERVER_NAME = 'Eli\'s ftp server'

    def __init__(self, bind_addr, provider_factory: Callable[[], ProviderBase]):
        self.crucial_error = False  # Other code can check this value, to know if there's been a serious error
        self.bind_addr = bind_addr
        self.provider_factory = provider_factory

    def serve(self):
        """ Wrap _serve, since this should run on a separate thread, we should be able to quit on error"""
        try:
            self._serve()
        except:
            traceback.print_exc()
            self.crucial_error = True

    def _serve(self):
        """
        Start serving
        """

        control_connection = socket.socket()
        control_connection.bind(self.bind_addr)
        control_connection.listen()
        client_id = 0

        print(Server.SERVER_NAME + " listening at " + str(control_connection.getsockname()))

        # For each client, open a handler thread
        while True:
            client_id += 1
            client = control_connection.accept()[0]
            print(f'New client: {client_id} - {client.getpeername()}')
            threading.Thread(
                target=Server.command_loop,
                args=(client, self.provider_factory(), client_id),
                name=f'Client{client_id}',
                daemon=True).start()

    @staticmethod
    def read_till_crlf(client: socket.socket) -> str:
        """
        Read a line till CRLF (or failure) from client and return a unicode string
        """

        max_buf_len = 1000

        buf = b''
        is_prv_cr = False
        while True:
            char = client.recv(1)
            if not char and len(buf):
                raise Exception('Unexpected socket termination')
            elif not char:
                break
            if len(buf) > max_buf_len:
                raise Exception(f'Data exceeded limit ({max_buf_len})')

            buf += char

            if char == b'\r':
                is_prv_cr = True
            elif is_prv_cr and char == b'\n':
                break
            else:
                is_prv_cr = False

        return str(buf[:-2], 'ascii')

    @staticmethod
    def read_n_till_close(client: socket.socket, n: int) -> bytes:
        """ Read from socket n bytes or till it's closed """

        buf = b''
        while True:
            char = client.recv(n - len(buf))
            if not char:
                break
            buf += char

        return buf

    @staticmethod
    def command_loop(client: socket.socket, provider: ProviderBase, client_id):
        """
        Command handling loop. Wait for command, handle it (if needed, delegate to provider)
        """

        def reply(status: int, data: str):
            """ Send a reply to the client (also convert the response to bytes) """
            data = bytes(f'{str(status)} {data}\r\n', 'ascii')
            print(f'{client_id} - Replying {data}')
            client.send(data)

        passive_client = None  # This stores the passive mode socket

        def passive_reply(before_reply: Tuple[int, str], data: Callable[[], str or bytes],
                          after_reply: Tuple[int, str]):
            """ Send a reply over passive data connection """

            nonlocal passive_client
            if not passive_client:
                reply(425, 'Enter passive mode first')
            else:
                reply(*before_reply)
                d = data()
                if type(d) is not bytes:
                    d = bytes(d, 'ascii')
                passive_client.send(d)
                passive_client.close()
                passive_client = None
                reply(*after_reply)

        def passive_read(before_read: Tuple[int, str], handle: Callable[[Callable], None],
                         after_read: Tuple[int, str]) -> 1:
            """ Read data from passive data connection and handle """

            nonlocal passive_client
            if not passive_client:
                reply(425, 'Enter passive mode first')
            else:
                reply(*before_read)
                def get_chunk(n): return Server.read_n_till_close(passive_client, n)
                handle(get_chunk)
                passive_client.close()
                passive_client = None
                reply(*after_read)

        reply(220, 'Hello, ' + Server.SERVER_NAME)
        while True:  # Handle command till end or error
            line = Server.read_till_crlf(client)
            if not line:
                break  # client closed socket
            print(f'{client_id} - Got line: {line}')
            if ' ' in line:
                cmd, args = line.split(' ', 1)
            else:
                cmd, args = line, None

            # Handle each command
            try:
                if cmd == 'CWD':
                    if provider.change_work_dir(args):
                        reply(250, 'dir changed to ' + args)
                    else:
                        reply(550, 'No such directory')
                elif cmd == 'DELE':
                    provider.del_file(args)
                    reply(257, f'"{args}" File deleted')
                elif cmd == 'LIST':
                    passive_reply((150, 'Sending current directory listing'), provider.listdir,
                                  (226, 'Sent directory listing'))
                elif cmd in ('MKD', 'XMKD'):
                    provider.mkdir(args)
                    reply(257, f'"{args}" Directory created')
                elif cmd in ('NOOP', 'noop'):
                    reply(200, 'NOOP OK')
                elif cmd in ('OPTS', 'opts'):
                    reply(501, 'opts-bad')
                elif cmd == 'PASV':
                    # Create the passive socket, notify client and wait for connection (with 1 sec timeout)
                    passive_server = socket.socket()
                    passive_server.bind((client.getsockname()[0], 0))
                    passive_server.setblocking(False)
                    passive_server.settimeout(1)
                    passive_server.listen(1)

                    passive_server_ip = passive_server.getsockname()[0].replace('.', ',')
                    passive_server_port = passive_server.getsockname()[1]
                    passive_server_port = f'{passive_server_port>>8},{passive_server_port&0xff}'
                    reply(227, f'Entering passive mode ({passive_server_ip},{passive_server_port})')

                    print(f'{client_id} - Waiting for passive connection')
                    passive_client = passive_server.accept()[0]
                    passive_server.close()
                    print(f'{client_id} - Got passive client: {passive_client.getpeername()}')
                elif cmd in ('PWD', 'XPWD'):
                    reply(257, '"' + provider.get_work_dir() + '" Is the current dir')
                elif cmd == 'QUIT':
                    reply(221, 'OK Bye')
                elif cmd in ('RMD', 'XRMD'):
                    provider.rmdir(args)
                    reply(250, f'"{args}" Directory removed')
                elif cmd == 'RNFR':
                    from_name = args
                    reply(350, 'Ready to rename')
                    line = Server.read_till_crlf(client)
                    print(f'{client_id} - Got line: {line}')
                    if not line or not line.startswith('RNTO'):
                        raise Exception("Expecting RNTO command after RNFR")
                    cmd, args = line.split(' ', 1)
                    to_name = args
                    provider.rename_file(from_name, to_name)
                    reply(250, 'Renamed file')
                elif cmd == 'RETR':
                    if not provider.get_size(args)[0]:  # This means it's a folder
                        reply(550, 'Not a file')
                    else:
                        passive_reply((150, f'Sending binary file {args}'), lambda: provider.get_file(args),
                                      (226, 'File transfer complete'))
                elif cmd == 'SIZE':
                    ret = provider.get_size(args)
                    if ret[0]:
                        reply(213, ret[1])
                    else:
                        reply(550, 'Could not get file size')
                elif cmd == 'STOR':
                    file_name = args
                    passive_read((150, 'OK, send data'), lambda get_chunk: provider.save_file(file_name, get_chunk),
                                 (226, 'File store complete'))
                elif cmd == 'SYST':
                    reply(213, 'WIN32')
                elif cmd == 'TYPE':
                    if args and args in ('I', 'A'):
                        reply(200, 'OK')
                    else:
                        print(f'{client_id} - Unknown TYPE arg: {args}')
                        reply(501, 'Unknown TYPE arg')
                elif cmd == 'USER':
                    reply(230, 'Login successful')
                else:
                    reply(501, 'Unknown cmd')
            except:
                print(f'{client_id} - ' + traceback.format_exc())
                reply(500, 'Error executing cmd')

        # Clean up
        if passive_client:
            passive_client.close()
        if line:
            client.close()
            print(f'{client_id} - Server closing connection')
        else:
            print(f'{client_id} - Client closed connection')


def print_help():
    msg = \
        Server.SERVER_NAME + ': A Python FTP server with various file system providers\n' \
        '-----------------------------------------------------------------------------\n' \
        'usage: <script> -address=<addr> -provider=<provider> -args=<provider args>\n\n' \
        'where:\n' \
        '   address:    an address for the server to listen to, in the form of <ip>:<port>\n' \
        '               (default = 0.0.0.0:8080)\n' \
        '   provider:   the file system provider. this can be:\n' \
        '                   fs      - plain old disk file system\n' \
        '                   dropbox - a dropbox storage account\n' \
        '   args:       the arguments required for the chosen provider\n' \
        '                   for provider \'fs\'      - pass the the root directory to serve\n' \
        '                   for provider \'dropbox\' - this should be the dropbox account token\n' \
        '                       (to get a token, go to: https://www.dropbox.com/developers/apps\n' \
        '                        and create an API app, full dropbox, then click \'Generated access token)\'\n' \
        '\n' \
        'NOTE: This server was written for ad-hok usage and does not handle scale/security at all.\n' \
        '      Do NOT use in a non-controlled environment.\n' \
        '      Regardless, use at your own responsibility\n'
    print(msg)


def parse_args(args: list) -> Tuple[Tuple[str, int], Callable]:
    """ Parse args and return (address, provider factory which can be called to construct a provider)"""
    bind_addr = ('0.0.0.0', 8080)
    provider = None
    provider_args = None

    if not len(args):
        raise Exception('No args')

    for arg in args:
        if arg.startswith('-address='):
            addr = arg.split('=', 1)[1]
            if ':' not in addr:
                raise Exception('Invalid address \''+addr+'\'')
            ip, port = addr.split(':', 1)
            if not (port.isnumeric() and 0 <= int(port) < 2**16-1):
                raise Exception('Invalid port \''+port+'\'')
            port = int(port)
            bind_addr = (ip, port)
        elif arg.startswith('-provider='):
            provider_str = arg.split('=', 1)[1]
            if provider_str == 'fs':
                provider = FileSystemProvider
            elif provider_str == 'dropbox':
                provider = DropboxProvider
            else:
                raise Exception('Invalid provider \''+provider_str+'\'')
        elif arg.startswith('-args='):
            provider_args = arg.split('=', 1)[1]
        else:
            raise Exception('Unknown/invalid arg \''+arg+'\'')

    if not provider:
        raise Exception('Missing provider')
    if not provider_args:
        raise Exception('Missing provider args')

    return bind_addr, lambda: provider(provider_args)


def main():
    try:
        bind_addr, provider_fact = parse_args(sys.argv[1:])  # skip script name
    except Exception as e:
        print('Arguments error: ' + str(e) + '\n')
        print_help()
        return

    server = Server(bind_addr, provider_fact)
    threading.Thread(target=server.serve, daemon=True).start()

    should_exit = False

    def wait_for_exit_from_user():
        nonlocal should_exit
        input('Enter to exit at any time...\n\n')
        should_exit = True

    threading.Thread(target=wait_for_exit_from_user, daemon=True).start()

    while True:
        time.sleep(0.5)
        if should_exit or server.crucial_error:
            sys.exit(0)  # Must call exit, cause daemon threads waiting for socket don't stop


if __name__ == '__main__':
    main()
