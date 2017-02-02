#!/usr/bin/env python3
# coding=utf-8
import sys
import time
import binascii
import struct
import collections
import logging
import socket
import select
import threading

import traceback

log = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.DEBUG,
    format='[%(levelname)s %(asctime)s %(funcName)s] %(message)s',
)

# slaver_pool = collections.deque()
# working_pool = {}
# spare_slaver_pool = {}

RECV_BUFFER_SIZE = 2 ** 12  # 4096 bytes
SECRET_KEY = "shootback"
SPARE_SLAVER_TTL = 600  # 600s
INTERNAL_VERSION = 0x0001


def fmt_addr(socket):
    return "{}:{}".format(*socket)


def try_close(closable):
    try:
        closable.close()
    except:
        pass


class SocketBridge:
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
            try_close(conn)
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
        return "SocketBridge<conn1[R:{} L:{}] conn2[R:{} L:{}]>".format(
            self.remote_addr[0], self.local_addr[0],
            self.remote_addr[1], self.local_addr[1],
        )

    def __repr__(self):
        return self.__str__()


class HandshakePackage:
    """

    握手包结构 总长64bytes
    使用 big-endian

    体积   名称        数据类型           描述
    1    pkg_ver       char         握手包版本, 目前只能为 0x01
    1    pkg_type       char           包类型 *1
    2    prgm_ver    unsigned short    程序版本 *2
    8      N/A         padding          预留
    4    crc32_sec   unsigned int     CRC32 *3
    48     N/A        padding          预留

    *1: 包类型:
        0x00: 预留
        0x01: Master-->Slaver 的握手包
        0x02: Slaver-->Master 的握手响应包

    *2: 默认即为 INTERNAL_VERSION

    *3: 此处的CRC32, 对 Master-->Slaver 来说是 CRC32(SECRET_KEY)
                  而对 Slaver-->Master  来说是 CRC32(Reversed(SECRET_KEY))

    """
    PACKAGE_SIZE = 2 ** 6  # 64 bytes

    # 密匙的CRC32
    SECRET_KEY_CRC32 = binascii.crc32(SECRET_KEY.encode('utf-8')) & 0xffffffff
    SECRET_KEY_REVERSED_CRC32 = binascii.crc32(SECRET_KEY[::-1].encode('utf-8')) & 0xffffffff

    # type
    TYPE_HANDSHAKE_MASTER_TO_SLAVER = b'\x01'
    TYPE_HANDSHAKE_SLAVER_TO_MASTER = b'\x02'

    # format
    FORMAT = "!c c H 8x I 48x"

    def __init__(self, pkg_ver=b'\x01', pkg_type=b'\x00',
                 prgm_ver=INTERNAL_VERSION, crc32_sec=0,
                 content=None
                 ):
        self.pkg_ver = pkg_ver
        self.pkg_type = pkg_type
        self.prgm_ver = prgm_ver
        self.crc32_sec = crc32_sec
        if content:
            self.content = content
        else:
            self._build_bytes()

    def _build_bytes(self):
        self.content = struct.pack(
            self.FORMAT,
            self.pkg_ver,
            self.pkg_type,
            self.prgm_ver,
            self.crc32_sec,
        )

    def verify(self, pkg_type=None):
        if pkg_type is not None and self.pkg_type != pkg_type:
            return False

        if self.pkg_type == self.TYPE_HANDSHAKE_MASTER_TO_SLAVER:
            return self.crc32_sec == self.SECRET_KEY_CRC32

        elif self.pkg_type == self.TYPE_HANDSHAKE_SLAVER_TO_MASTER:
            return self.crc32_sec == self.SECRET_KEY_REVERSED_CRC32

        else:
            return True

    @classmethod
    def decode_only(cls, content):
        if len(content) != cls.PACKAGE_SIZE:
            raise ValueError("content size should be {}, but {}".format(
                cls.PACKAGE_SIZE, len(content)
            ))
        pkg_ver, pkg_type, prgm_ver, crc32_sec = struct.unpack(cls.FORMAT, content)
        return cls(
            pkg_ver=pkg_ver, pkg_type=pkg_type,
            prgm_ver=prgm_ver, crc32_sec=crc32_sec,
            content=content,
        )

    @classmethod
    def decode_verify(cls, content, pkg_type=None):
        try:
            pkg = cls.decode_only(content)
        except:
            return None, False
        else:
            return pkg, pkg.verify(pkg_type=pkg_type)

    @classmethod
    def build_master_to_slaver(cls):
        return cls(
            pkg_type=cls.TYPE_HANDSHAKE_MASTER_TO_SLAVER,
            crc32_sec=cls.SECRET_KEY_CRC32,
        )

    @classmethod
    def build_slaver_to_master(cls):
        return cls(
            pkg_type=cls.TYPE_HANDSHAKE_SLAVER_TO_MASTER,
            crc32_sec=cls.SECRET_KEY_REVERSED_CRC32,
        )

    def __str__(self):
        return """HandshakePackage<pkg_ver: {} pkg_type:{} prgm_ver:{} crc32_sec:{}>""".format(
            self.pkg_ver,
            self.pkg_type,
            self.prgm_ver,
            self.crc32_sec
        )

    def __repr__(self):
        return self.__str__()


class Master:
    def __init__(self, customer_listen_addr, communicate_addr=None,
                 slaver_pool=None, working_pool=None):
        self.thread_pool = {}
        self.thread_pool["spare_slaver"] = {}
        self.thread_pool["working_slaver"] = {}
        self.thread_pool["customer"] = {}

        self.communicate_addr = communicate_addr

        if slaver_pool:
            # 使用外部slaver_pool, 就不再初始化listen
            self.external_slaver = True
            self.thread_pool["listen_slaver"] = None
        else:
            # 自己listen来获取slaver
            self.external_slaver = False
            self.slaver_pool = collections.deque()
            self.thread_pool["listen_slaver"] = threading.Thread(
                target=self._listen_slaver,
                name="listen_slaver-{}".format(fmt_addr(self.communicate_addr)),
            )

        self.customer_listen_addr = customer_listen_addr
        self.thread_pool["listen_customer"] = threading.Thread(
            target=self._listen_customer,
            name="listen_customer-{}".format(fmt_addr(self.customer_listen_addr)),
        )

        self.working_pool = working_pool or {}

    def start_daemon(self):
        if not self.external_slaver:
            self.thread_pool["listen_slaver"].start()
        self.thread_pool["listen_customer"].start()

    def join(self):
        if not self.external_slaver:
            self.thread_pool["listen_slaver"].join()
        self.thread_pool["listen_customer"].join()

    def _handshake(self, conn_slaver):
        conn_slaver.send(HandshakePackage.build_master_to_slaver().content)

        rlist, _, elist = select.select([conn_slaver], [], [conn_slaver], 1)
        if not rlist or elist:
            # 超时或出错
            return False

        buff = conn_slaver.recv(HandshakePackage.PACKAGE_SIZE)
        pkg, verify = HandshakePackage.decode_verify(buff, HandshakePackage.TYPE_HANDSHAKE_SLAVER_TO_MASTER)

        log.debug("HsPkg from {}: {}".format(conn_slaver.getpeername(), pkg))

        return verify

    def _serving_customer(self, conn_customer, conn_slaver):
        addr_customer = conn_customer.getpeername()

        SocketBridge(conn_customer, conn_slaver).duplex_transfer()

        del self.thread_pool["customer"][addr_customer]
        del self.working_pool[addr_customer]

        log.info("customer complete: {}".format(addr_customer))

    def _listen_slaver(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(self.communicate_addr)
        sock.listen(10)
        while True:
            conn, addr = sock.accept()
            self.slaver_pool.append({
                "addr_slaver": addr,
                "conn_slaver": conn,
            })
            log.info("Got slaver {} Total: {}".format(
                addr, len(self.slaver_pool)
            ))

    def _get_workable_slaver(self):
        while True:
            try:
                dict_slaver = self.slaver_pool.popleft()
            except:
                log.error("!!NO SLAVER AVAILABLE!!")
                return None

            conn_slaver = dict_slaver["conn_slaver"]

            try:
                hs = self._handshake(conn_slaver)
            except:
                traceback.print_exc()
                hs = False

            if hs:
                return conn_slaver
            else:
                log.warning("slaver handshake failed: {}".format(dict_slaver["addr_slaver"]))
                try_close(conn_slaver)
                del conn_slaver
                del dict_slaver
                time.sleep(0.05)

    def _listen_customer(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(self.customer_listen_addr)
        sock.listen(10)
        while True:
            conn_customer, addr_customer = sock.accept()
            log.info("Serving customer: {} Total customers: {}".format(
                addr_customer, len(self.working_pool) + 1
            ))

            conn_slaver = self._get_workable_slaver()
            if conn_slaver is None:
                log.warning("Closing customer[{}] because no available slaver found".format(
                    addr_customer))
                try_close(conn_customer)
                del conn_customer
                continue

            self.working_pool[addr_customer] = {
                "addr_customer": addr_customer,
                "conn_customer": conn_customer,
                "conn_slaver": conn_slaver,
            }

            t = threading.Thread(target=self._serving_customer,
                                 name="customer-{}".format(fmt_addr(addr_customer)),
                                 args=(conn_customer, conn_slaver),
                                 )
            self.thread_pool["customer"][addr_customer] = t
            t.start()


class Slaver:
    def __init__(self, communicate_addr, target_addr, max_spare_count=5):
        self.communicate_addr = communicate_addr
        self.target_addr = target_addr
        self.max_spare_count = max_spare_count

        self.spare_slaver_pool = {}
        self.working_pool = {}

    def _connect_master(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(self.communicate_addr)

        log.info("Connected master[{}] at {}".format(
            sock.getpeername(),
            sock.getsockname(),
        ))

        self.spare_slaver_pool[sock.getsockname()] = {
            "conn_slaver": sock,
        }

        return sock

    def _connect_target(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(self.target_addr)

        log.debug("connected to target[{}] at: {}".format(
            sock.getpeername(),
            sock.getsockname(),
        ))

        return sock

    def _waiting_handshake(self, conn_slaver):
        rlist, _, elist = select.select([conn_slaver], [], [conn_slaver], SPARE_SLAVER_TTL)
        if not rlist or elist:
            # 超时或出错
            return False

        buff = conn_slaver.recv(HandshakePackage.PACKAGE_SIZE)
        pkg, verify = HandshakePackage.decode_verify(buff, HandshakePackage.TYPE_HANDSHAKE_MASTER_TO_SLAVER)
        log.debug("HsPkg from {}: {}".format(conn_slaver.getpeername(), pkg))

        if not verify:
            return False

        try:
            conn_slaver.send(HandshakePackage.build_slaver_to_master().content)
        except:
            return False

        return True

    def _handling_command(self, conn_slaver):
        addr_slaver = conn_slaver.getsockname()
        addr_master = conn_slaver.getpeername()

        try:
            hs = self._waiting_handshake(conn_slaver)
        except:
            traceback.print_exc()
            hs = False
        if not hs:
            # 握手失败或超时等情况
            log.warning("bad handshake from: {}".format(addr_master))
            del self.spare_slaver_pool[addr_slaver]
            try_close(conn_slaver)
            del conn_slaver
            log.warning("a slaver[{}] abort due to handshake error or timeout".format(addr_slaver))
            return
        else:
            log.info("Success master handshake from: {}".format(addr_master))

        self.working_pool[addr_slaver] = self.spare_slaver_pool.pop(addr_slaver)
        try:
            conn_target = self._connect_target()
        except:
            log.error("unable to connect target")
            try_close(conn_slaver)
            del conn_slaver
            del self.working_pool[addr_slaver]
            return
        self.working_pool[addr_slaver]["conn_target"] = conn_target

        SocketBridge(conn_slaver, conn_target).duplex_transfer()

        del self.working_pool[addr_slaver]

        log.info("complete: {}".format(addr_slaver))

    def serve_forever(self):
        delay = 0
        MAX_DELAY = 15

        while True:
            if len(self.spare_slaver_pool) >= self.max_spare_count:
                time.sleep(0.1)
                continue

            try:
                conn_slaver = self._connect_master()
            except:
                log.warning("unable to connect master\n {}".format(traceback.format_exc()))
                time.sleep(delay)
                if delay < MAX_DELAY:
                    delay += 1
                continue
            else:
                if delay:
                    delay = 0

            try:
                threading.Thread(target=self._handling_command,
                                 args=(conn_slaver,),
                                 ).start()

                log.info("connected to master[{}] at {} total: {}".format(
                    conn_slaver.getpeername(),
                    conn_slaver.getsockname(),
                    len(self.spare_slaver_pool),
                ))
            except:
                log.error("unable create Thread:\n {}".format(traceback.format_exc()))
                time.sleep(delay)
                if delay < MAX_DELAY:
                    delay += 1
                continue
            else:
                if delay:
                    delay = 0


def run_master(communicate_addr, customer_listen_addr):
    log.info("running as master, slaver from: {} customer_from: {}".format(
        communicate_addr, customer_listen_addr
    ))

    Master(customer_listen_addr, communicate_addr).start_daemon()


def run_slaver(communicate_addr, target_addr, max_spare_count=5):
    log.info("running as slaver, master addr: {} target: {}".format(
        communicate_addr, target_addr
    ))

    Slaver(communicate_addr, target_addr, max_spare_count=max_spare_count).serve_forever()


def main():
    log.info("shootback running")

    communicate_addr = ("localhost", 12345)
    customer_listen_addr = ("localhost", 12344)
    # target_addr = ("localhost", 1080)
    target_addr = ("122.237.8.58", 2223)

    if sys.argv[1] == "-m":
        run_master(communicate_addr, customer_listen_addr)
    elif sys.argv[1] == "-s":
        run_slaver(communicate_addr, target_addr, max_spare_count=5)


if __name__ == '__main__':
    main()
