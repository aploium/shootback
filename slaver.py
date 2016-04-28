# coding=utf-8
from collections import deque
from common_func import *
from time import sleep
import traceback


def open_sock(target_addr):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(target_addr)
    return s


def wait_and_response_verify(master_sock, connection_pool):
    """

        :type connection_pool: deque
        :type master_sock: socket.socket
    """
    # dbgprint('WaitForVerify:', master_sock.getpeername())
    try:
        data = master_sock.recv(RECV_BUFF_SIZE)
        # dbgprint('VerifyRecv', data)
        if data == master_verify_string:  # Good Master
            master_sock.send(slaver_verify_string)
            # dbgprint('VerifySent')
        else:
            warnprint('BadMaster')
            try_remove_connection_from_pool(connection_pool, master_sock)
            close_socket(master_sock)
            return
    except ConnectionResetError:
        traceback.print_exc()
        dbgprint('SocksShutdown')
        try_remove_connection_from_pool(connection_pool, master_sock)
        close_socket(master_sock)
    except:
        try_remove_connection_from_pool(connection_pool, master_sock)
        close_socket(master_sock)
        traceback.print_exc()
    else:
        infoprint('VerifyOK,GoodMaster', master_sock.getpeername())
        if not hello_before_transfer:
            try:
                dwn_sock = open_sock(slaver_target_addr)
            except:
                errprint('UnableToConnectDownstream')
                close_socket(master_sock)
            else:
                duplex_data_transfer(master_sock, dwn_sock, connection_pool, master_sock, True)
        else:
            wait_and_response_hello(master_sock, connection_pool)
    exit()


def wait_and_response_hello(master_sock, connection_pool):
    try:
        data = master_sock.recv(RECV_BUFF_SIZE)
        if data == MSG_BEGIN_TRANSFER:
            master_sock.send(MSG_BEGIN_TRANSFER)
            try_remove_connection_from_pool(connection_pool, master_sock)
        elif data == MSG_CLOSE_SOCKET:
            dbgprint('MasterSentCloseMsg')
            try_remove_connection_from_pool(connection_pool, master_sock)
            close_socket(master_sock)
            return
        else:
            warnprint('RequireServerHello,Received:', data)
            try_remove_connection_from_pool(connection_pool, master_sock)
            close_socket(master_sock)
            return
    except ConnectionResetError:
        dbgprint('SocksShutdown')
        try_remove_connection_from_pool(connection_pool, master_sock)
        close_socket(master_sock)
    except Exception as e:
        warnprint(e)
        traceback.print_exc()
        try_remove_connection_from_pool(connection_pool, master_sock)
        close_socket(master_sock)
    else:
        dbgprint('GetHello', master_sock.getpeername())
        try:
            dwn_sock = open_sock(slaver_target_addr)
        except:
            errprint('UnableToConnectDownstream')
            close_socket(master_sock)
        else:
            duplex_data_transfer(master_sock, dwn_sock, first_is_compress=True)
    finally:
        try_remove_connection_from_pool(connection_pool, master_sock)


def main():
    connection_pool = deque()
    while True:
        try:
            if len(connection_pool) < slaver_unused_connection_pool_size:
                dbgprint('AvailableConnectionPoolSize:', len(connection_pool))
                master_sock = open_sock(master_addr)
                connection_pool.append(master_sock)
                t = threading.Thread(target=wait_and_response_verify,
                                     args=(master_sock, connection_pool))
                t.start()
            else:
                sleep(0.2)

        except Exception as e:
            traceback.print_exc()
            errprint(e)
            sleep(1)


if __name__ == '__main__':
    main()
