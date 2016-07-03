# coding=utf-8
"""
shootback: a fast and reliable tunnel implant Gzip and AES
"""
import socket
import traceback
from ColorfulPyPrint import *
from time import sleep
import threading
import zlib

# The following values are Their DEFAULT value.
# Please rename the 'config.sample.py' to 'config.py' and modify values in that file.
# Explains are in that file, too.
# Master Config
master_local_listen_addr = ('127.0.0.1', 1080)
master_remote_listen_addr = ('127.0.0.1', 20822)
master_connection_pool_size = 80

# Slaver Config
master_addr = ('127.0.0.1', 20822)
slaver_target_addr = ('127.0.0.1', 80)

# Common Config
hello_before_transfer = True
hello_timeout = 0.8  # a small value
slaver_unused_connection_pool_size = 20

secret_key_slaver = 'Love'
secret_key_master = 'Lucia'

compress_during_transfer = True
compress_level = 7

verbose_level = 2

compress_pkg_header = b'\x05\x05\x05\x05\x05\x05\x05\x05'
compress_pkg_footer = b'\x07\x07\x07\x07\x07\x07\x07\x07'
RECV_BUFF_SIZE = 65536
SLAVER_CONNECTION_TTL = 120
MSG_BEGIN_TRANSFER = b'+++begin---'
MSG_CLOSE_SOCKET = b'+++bye---'
from config import *

if compress_level == 0:
    compress_during_transfer = False
compress_pkg_footer_len = len(compress_pkg_footer)
slaver_verify_string = b'+++' + secret_key_slaver.encode() + b'---'
master_verify_string = b'+++' + secret_key_master.encode() + b'---'
socket.setdefaulttimeout(SLAVER_CONNECTION_TTL + 30)
ColorfulPyPrint_set_verbose_level(verbose_level)


def close_socket(sock):
    try:
        sock.shutdown(socket.SHUT_RDWR)
        sock.close()
    except:
        pass


def try_remove_connection_from_pool(pool, socket):
    if (pool is not None) and (socket in pool):
        pool.remove(socket)
        return True
    else:
        return False


def extract_pkg_from_data(byte_data):
    complete_pkg = []

    z_packages = byte_data.split(compress_pkg_header)
    len_pkg = len(z_packages)
    dbgprint('Len_Sep:', len_pkg)
    if len_pkg < 2:
        errprint('ReceiveError,CannotFindGzipHeader', byte_data)
        raise ValueError('ReceiveError,CannotFindGzipHeader')

    else:
        data_to_preserve = z_packages.pop()
        for z_pkg in z_packages:
            z_pkg = z_pkg[:-compress_pkg_footer_len]  # Remove footer mark
            if not z_pkg:
                continue
            dbgprint('AfterGzip', len(z_pkg), v=4)
            decompressed_data = zlib.decompress(z_pkg)
            dbgprint('BeforeGzip', len(decompressed_data), v=4)
            complete_pkg.append(decompressed_data)

        if data_to_preserve[-compress_pkg_footer_len:] == compress_pkg_footer:  # last pkg is complete
            data_to_preserve = data_to_preserve[:-compress_pkg_footer_len]
            dbgprint('AfterGzip', len(data_to_preserve), v=4)
            decompressed_data = zlib.decompress(data_to_preserve)
            dbgprint('BeforeGzip', len(decompressed_data), v=4)
            complete_pkg.append(decompressed_data)
            data_to_preserve = b''
        else:
            data_to_preserve = compress_pkg_header + data_to_preserve

    return complete_pkg, data_to_preserve


def simplex_data_transfer(sock_from, sock_to, connect_pool=None, this_socket=None, first_is_compress=False):
    """

    :type this_socket: socket.socket
    :type sock_to: socket.socket
    :type sock_from: socket.socket
    """
    dbgprint('SimplexTransfer:', sock_from.getpeername(), sock_to.getpeername(), v=4)
    dbgprint(connect_pool, this_socket, first_is_compress)
    is_total_size_zero = True
    is_abort = False
    wait_count = 60
    ups_data = b''
    try:
        while wait_count:
            buff = sock_from.recv(RECV_BUFF_SIZE)
            if compress_during_transfer and first_is_compress:
                ups_data += buff
            else:
                ups_data = buff

            try_remove_connection_from_pool(connect_pool, this_socket)

            if not ups_data:
                sock_to.sendall(ups_data)
                wait_count -= 1
                sleep(0.1)
                continue

            if not is_total_size_zero:
                is_total_size_zero = True
            if not compress_during_transfer:
                sock_to.sendall(ups_data)
            else:
                if first_is_compress:
                    pkgs, ups_data = extract_pkg_from_data(ups_data)
                    for pkg in pkgs:
                        sock_to.sendall(pkg)
                else:
                    dbgprint('BeforeGzip', len(ups_data))
                    ups_data = zlib.compress(ups_data, compress_level)
                    dbgprint('AfterGzip', len(ups_data))
                    sock_to.sendall(compress_pkg_header + ups_data + compress_pkg_footer)

    except ConnectionResetError:
        dbgprint('SocksShutdown')
        try:
            sock_to.shutdown(socket.SHUT_WR)
            sock_from.shutdown(socket.SHUT_RD)
        except:
            # traceback.print_exc()
            pass
    except ConnectionAbortedError:
        # traceback.print_exc()
        is_abort = True
    except socket.timeout:
        warnprint('SocketTimeout')
        try_remove_connection_from_pool(connect_pool, this_socket)
        is_abort = True
    except OSError:
        is_abort = True
    except:
        traceback.print_exc()
        is_abort = True

    try_remove_connection_from_pool(connect_pool, this_socket)
    if is_total_size_zero or is_abort:
        sleep(0.5)
        close_socket(sock_to)
        close_socket(sock_from)
    dbgprint('ExitSimplex')
    exit()


def duplex_data_transfer(up_sock, dwn_sock, connect_pool=None, this_socket=None, first_is_compress=False):
    """

    :type up_sock: socket.socket
    :type dwn_sock: socket.socket
    """
    infoprint('Transfer between', up_sock.getpeername(), 'and', dwn_sock.getpeername(), first_is_compress)
    try:
        t_ud = threading.Thread(
            target=simplex_data_transfer, args=(up_sock, dwn_sock),
            kwargs=dict(
                connect_pool=connect_pool,
                this_socket=this_socket,
                first_is_compress=first_is_compress
            ),
        )
        t_du = threading.Thread(
            target=simplex_data_transfer, args=(dwn_sock, up_sock),
            kwargs=dict(
                connect_pool=connect_pool,
                this_socket=this_socket,
                first_is_compress=not first_is_compress
            ),
        )
        t_ud.start()
        t_du.start()
        t_ud.join()
        t_du.join()
    except:
        errprint('ErrInDuplex:')
        traceback.print_exc()
    finally:
        close_socket(up_sock)
        close_socket(dwn_sock)
