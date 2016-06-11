# coding=utf-8
from collections import deque
import os
from time import time
from common_func import *


def pop_an_slaver_from_pool(slaver_pool):
    slaver_wait_retry_count = 10
    while not len(slaver_pool):
        if not slaver_wait_retry_count:
            errprint('ZeroSlaver!! Abort retry.')
            return None
        warnprint('ZeroSlaverPoolError, retry: ', slaver_wait_retry_count)
        slaver_wait_retry_count -= 1
        sleep(0.5)
    else:
        return (slaver_pool.popleft())[0]


def verify_slaver(slaver_socket):
    """

    :type slaver_socket: socket.socket
    """
    # dbgprint('Verify:', slaver_socket.getpeername())
    try:
        slaver_socket.send(master_verify_string)  # identity master myself
        # dbgprint('VerifySendOK')
        data = slaver_socket.recv(RECV_BUFF_SIZE)
        # dbgprint('Recv:', data)
        if data == slaver_verify_string:  # Good Slaver
            # dbgprint('VerifyOK', slaver_socket.getpeername())
            return True
        else:
            close_socket(slaver_socket)
            return False
    except ConnectionResetError:
        traceback.print_exc()
        dbgprint('SocksShutdown')
        close_socket(slaver_socket)
        return False
    except:
        warnprint('UnableToVerifySlaver', slaver_socket.getpeername())
        traceback.print_exc()
        close_socket(slaver_socket)


def send_hello_and_transfer(client_sock, slaver_pool):
    slaver_sock = pop_an_slaver_from_pool(slaver_pool)
    while not is_hello_ok(slaver_sock):
        slaver_sock = pop_an_slaver_from_pool(slaver_pool)
        if slaver_sock is None:
            close_socket(client_sock)
            exit()
    else:
        try:
            duplex_data_transfer(client_sock, slaver_sock)
        finally:
            close_socket(client_sock)
            close_socket(slaver_sock)
    exit()


def is_hello_ok(slaver_socket):
    """

    :type slaver_socket: socket.socket
    """
    # dbgprint('Verify:', slaver_socket.getpeername())
    try:
        dbgprint('SendingHelloTo', slaver_socket.getpeername())
        slaver_socket.settimeout(hello_timeout)
        slaver_socket.send(MSG_BEGIN_TRANSFER)  # identity master myself
        # dbgprint('VerifySendOK')
        data = slaver_socket.recv(RECV_BUFF_SIZE)
        slaver_socket.settimeout(SLAVER_CONNECTION_TTL)
        dbgprint('HelloReceived:', data)
        # dbgprint('Recv:', data)
        if data == MSG_BEGIN_TRANSFER:  # Good Slaver
            # dbgprint('VerifyOK', slaver_socket.getpeername())
            return True
        else:
            close_socket(slaver_socket)
            return False
    except ConnectionResetError:
        traceback.print_exc()
        dbgprint('SocksShutdown')
        close_socket(slaver_socket)
        return False
    except Exception as e:
        warnprint('HelloSendErr', e)
        traceback.print_exc()
        close_socket(slaver_socket)
        return False


def local_listen(local_listen_addr, slaver_pool):
    # try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(local_listen_addr)
    s.listen(master_connection_pool_size)

    infoprint('OutboundListening', local_listen_addr, 'users should connect to this addr')
    while True:
        try:
            client_sock, addr = s.accept()

            if not hello_before_transfer:
                slaver_sock = pop_an_slaver_from_pool(slaver_pool)
                if slaver_sock is None:
                    close_socket(client_sock)
                    continue
                infoprint('SlaverPoolLen:', len(slaver_pool))
                t = threading.Thread(target=duplex_data_transfer, args=(client_sock, slaver_sock))
                t.start()
                # except Exception as e:
                #     errprint('local_listen', e)
                #     exit()

            else:
                t = threading.Thread(target=send_hello_and_transfer, args=(client_sock, slaver_pool))
                t.start()
        except Exception as e:
            errprint('OutboundListeningErr', e, 'retry after 1 second')
            traceback.print_exc()
            sleep(1)


def remote_listen(remote_listen_addr, slaver_pool):
    """

    :type slaver_pool: deque
    """

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(remote_listen_addr)
    s.listen(master_connection_pool_size)
    infoprint('InboundListening', remote_listen_addr, 'slavers should connect to this addr')
    while True:
        try:
            sock, addr = s.accept()
            if verify_slaver(sock):
                slaver_pool.append((sock, time()))
                infoprint('WeGotAnSlaver:', addr, 'Total:', len(slaver_pool))
                # t = threading.Thread(target=duplex_data_transfer, args=(up_sock, up_addr, dwn_sock))
                # t.start()
        except Exception as e:
            errprint('RemoteListenErr', e, 'retry after 1 second')
            traceback.print_exc()


def ttl_checker(slaver_pool):
    """

    :type slaver_pool: list
    """
    slaver_to_remove = None
    while True:
        for slaver in slaver_pool:
            if slaver[1] + SLAVER_CONNECTION_TTL < time():
                slaver_to_remove = slaver
                try:
                    dbgprint('SlaverTimeoutRemove', slaver_to_remove[0].getpeername(), 'Total', len(slaver_pool) - 1)
                    if hello_before_transfer:
                        slaver[0].send(MSG_CLOSE_SOCKET)
                        sleep(0.2)
                    close_socket(slaver[0])
                except Exception as e:
                    dbgprint('SlaverAlreadyClosed', e)
                break
        if try_remove_connection_from_pool(slaver_pool, slaver_to_remove):
            sleep(0.5)
        else:
            sleep(3)


def main():
    slaver_pool = deque()
    r = threading.Thread(target=remote_listen, args=(master_remote_listen_addr, slaver_pool))
    l = threading.Thread(target=local_listen, args=(master_local_listen_addr, slaver_pool))
    slaver_ttl_checker = threading.Thread(target=ttl_checker, args=(slaver_pool,), daemon=True)
    r.start()
    l.start()
    slaver_ttl_checker.start()


if __name__ == '__main__':
    main()
    while True:
        sleep(10)
