#!/usr/bin/env python3
# coding=utf-8
import sys
import time
import collections
import logging
import socket
import threading

log = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.DEBUG,
    format='[%(levelname)s %(asctime)s %(funcName)s] %(message)s',
)

communicate_addr = ("localhost", 12345)
customer_listen_addr = ("localhost", 12344)
target_addr = ("localhost", 8123)

slaver_pool = collections.deque()
communicating_pool = {}
spare_slaver_pool = {}

RECV_BUFFER_SIZE = 2 ** 12  # 4096


def format_socket_pair(socket_pair):
    return "{}:{}".format(*socket_pair)


class ConnectionPair:
    def __init__(self, conn1: socket.SocketType, conn2):
        self.conn = [conn1, conn2]
        self.remote_addr = [conn1.getpeername(), conn2.getpeername()]
        self.local_addr = [conn1.getsockname(), conn2.getsockname()]

    def duplex_transfer(self):
        t0 = threading.Thread(target=self._simplex_transfer, args=(0,))
        t1 = threading.Thread(target=self._simplex_transfer, args=(1,))
        t0.start()
        t1.start()
        t0.join()
        t1.join()

        del t0
        del t1

        self._base_closed(0)
        self._base_closed(1)

    def _base_closed(self, conn_no):
        conn = self.conn[conn_no]
        if conn is not None:
            conn.close()
            self.conn[conn_no] = None  # 移除掉socket
            del conn

    def _rd_closed(self, conn_no):
        self._base_closed(conn_no)
        if self.conn[1 - conn_no] is not None:
            self.conn[1 - conn_no].shutdown(socket.SHUT_WR)

    def _wr_closed(self, conn_no):
        self._base_closed(conn_no)
        if self.conn[1 - conn_no] is not None:
            self.conn[1 - conn_no].shutdown(socket.SHUT_RD)

    def _rd_shutdowned(self, conn_no):
        self.conn[conn_no].shutdown(socket.SHUT_RD)
        if self.conn[1 - conn_no] is not None:
            self.conn[1 - conn_no].shutdown(socket.SHUT_WR)

    def _simplex_transfer(self, from_no):
        conn_from = self.conn[from_no]
        conn_to = self.conn[1 - from_no]

        log.debug("simplex_transfer {}-->{}".format(
            self.remote_addr[from_no], self.remote_addr[1 - from_no]
        ))

        while True:
            try:
                buff = conn_from.recv(RECV_BUFFER_SIZE)
            except:
                self._rd_closed(from_no)
                break

            if not buff:
                self._rd_shutdowned(from_no)
                break

            try:
                conn_to.send(buff)
            except:
                self._wr_closed(1 - from_no)
                break

    def __str__(self):
        return "ConnectionPair<conn1[R:{} L:{}] conn2[R:{} L:{}]>".format(
            self.remote_addr[0], self.local_addr[0],
            self.remote_addr[1], self.local_addr[1],
        )

    def __repr__(self):
        return self.__str__()


def serving_customer(conn_customer, conn_slaver):
    addr_customer = conn_customer.getpeername()

    ConnectionPair(conn_customer, conn_slaver).duplex_transfer()

    del communicating_pool[addr_customer]


def master_waiting_slaver(communicate_addr):
    # global slaver_pool

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(communicate_addr)
    sock.listen(10)
    while True:
        conn, addr = sock.accept()
        log.info("Got slaver {}".format(addr))
        slaver_pool.append({
            "addr_slaver": addr,
            "conn_slaver": conn,
        })


def master_waiting_customer(customer_listen_addr):
    # global slaver_pool

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(customer_listen_addr)
    sock.listen(10)
    while True:
        conn_customer, addr = sock.accept()
        log.info("Serving customer: {}".format(addr))
        dict_slaver = slaver_pool.popleft()
        conn_slaver = dict_slaver["conn_slaver"]
        t = threading.Thread(target=serving_customer,
                             args=(conn_customer, conn_slaver)
                             )
        communicating_pool[addr] = {
            "addr_customer": addr,
            "conn_customer": conn_customer,
            "conn_slaver": conn_slaver,
        }
        t.start()


def slaver_connect_master(communicate_addr):
    # global slaver_pool
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(communicate_addr)

    log.info("Connected master[{}] at {}".format(
        sock.getpeername(),
        sock.getsockname(),
    ))

    spare_slaver_pool[sock.getsockname()] = {
        "conn_slaver": sock,
    }

    return sock


def slaver_connect_target(target_addr):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(target_addr)

    log.debug("connected to target[{}] at: {}".format(
        sock.getpeername(),
        sock.getsockname(),
    ))

    return sock


def slaver_handling_command(conn_slaver, target_addr):
    # global spare_slaver_pool, communicating_pool

    addr_slaver = conn_slaver.getsockname()
    addr_master = conn_slaver.getpeername()

    buff = conn_slaver.recv(RECV_BUFFER_SIZE)  # 等待master发来的首段数据

    communicating_pool[addr_slaver] = spare_slaver_pool.pop(addr_slaver)

    log.info("GotMasterCommand from: {}".format(addr_master))

    conn_target = slaver_connect_target(target_addr)

    communicating_pool[addr_slaver]["conn_target"] = conn_target

    conn_target.send(buff)  # 把第一段数据发给target

    ConnectionPair(conn_slaver, conn_target).duplex_transfer()

    del communicating_pool[addr_slaver]


def slaver_forever(communicate_addr, target_addr, max_spare_count=5):
    # global spare_slaver_pool

    while True:
        if len(spare_slaver_pool) >= max_spare_count:
            time.sleep(0.1)
        else:
            conn_slaver = slaver_connect_master(communicate_addr)
            threading.Thread(target=slaver_handling_command,
                             args=(conn_slaver, target_addr),
                             ).start()

            log.info("connected to master[{}] at {} total: {}".format(
                conn_slaver.getpeername(),
                conn_slaver.getsockname(),
                len(spare_slaver_pool),
            ))


def run_master(communicate_addr, customer_listen_addr):
    log.info("running as master, slaver from: {} customer_from: {}".format(
        communicate_addr, customer_listen_addr
    ))

    ts = threading.Thread(target=master_waiting_slaver, args=(communicate_addr,))
    tc = threading.Thread(target=master_waiting_customer, args=(customer_listen_addr,))
    ts.start()
    tc.start()
    ts.join()
    tc.join()


def run_slaver(communicate_addr, target_addr, max_spare_count=5):
    log.info("running as slaver, master addr: {} target: {}".format(
        communicate_addr, target_addr
    ))
    slaver_forever(communicate_addr, target_addr, max_spare_count=max_spare_count)


def main():
    log.info("shootback2 running")
    if sys.argv[1] == "-m":
        run_master(communicate_addr, customer_listen_addr)
    elif sys.argv[1] == "-s":
        run_slaver(communicate_addr, target_addr, max_spare_count=5)


if __name__ == '__main__':
    main()
