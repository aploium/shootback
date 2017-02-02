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


def select_recv(conn, size, timeout=None):
    rlist, _, elist = select.select([conn], [], [conn], timeout)
    if not rlist or elist:
        # 超时或出错
        return None

    return conn.recv(size)


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

    def _rd_closed(self, conn_no):
        self._base_closed(conn_no)
        if self.conn[1 - conn_no] is not None:
            try:
                self.conn[1 - conn_no].shutdown(socket.SHUT_WR)
            except:
                pass

    def _wr_closed(self, conn_no):
        self._base_closed(conn_no)
        if self.conn[1 - conn_no] is not None:
            try:
                self.conn[1 - conn_no].shutdown(socket.SHUT_RD)
            except:
                pass

    def _rd_shutdowned(self, conn_no):
        try:
            self.conn[conn_no].shutdown(socket.SHUT_RD)
        except:
            pass
        if self.conn[1 - conn_no] is not None:
            try:
                self.conn[1 - conn_no].shutdown(socket.SHUT_WR)
            except:
                pass

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


class CtrlPkg:
    """

    控制包结构 总长64bytes      CtrlPkg.FORMAT_PKG
    使用 big-endian

    体积   名称        数据类型           描述
    1    pkg_ver       char         版本, 目前只能为 0x01
    1    pkg_type    signed char        包类型 *1
    2    prgm_ver    unsigned short    程序版本 *2
    20      N/A       padding          预留
    40    data        bytes           数据区 *3

    *1: 包类型. 除心跳外, 所有负数包代表由Slaver发出, 正数包由Master发出
        -1: Slaver-->Master 的握手响应包       PTYPE_HS_S2M
         0: 心跳包                            PTYPE_HEART_BEAT
        +1: Master-->Slaver 的握手包          PTYPE_HS_M2S

    *2: 默认即为 INTERNAL_VERSION

    *3: 数据区中的内容由各个类型的包自身定义

    -------------- 数据区定义 ------------------
    包类型: -1 (Slaver-->Master 的握手响应包)
        体积   名称           数据类型         描述
         4    crc32_s2m   unsigned int     简单鉴权用 CRC32(Reversed(SECRET_KEY))
       其余为空
       *注意: -1握手包是把 SECRET_KEY 字符串翻转后取CRC32, +1握手包不预先反转

    包类型: 0 (心跳)
       数据区为空

    包理性: +1 (Master-->Slaver 的握手包)
        体积   名称           数据类型         描述
         4    crc32_m2s   unsigned int     简单鉴权用 CRC32(SECRET_KEY)
       其余为空

    """
    PACKAGE_SIZE = 2 ** 6  # 64 bytes

    # 密匙的CRC32
    SECRET_KEY_CRC32 = binascii.crc32(SECRET_KEY.encode('utf-8')) & 0xffffffff
    SECRET_KEY_REVERSED_CRC32 = binascii.crc32(SECRET_KEY[::-1].encode('utf-8')) & 0xffffffff

    # Package Type
    PTYPE_HS_S2M = -1  # Handshake Slaver to Master
    PTYPE_HEART_BEAT = 0  # 心跳
    PTYPE_HS_M2S = +1  # Handshake Master to Slaver

    # formats
    FORMAT_PKG = "!c b H 20x 40s"
    FORMATS_DATA = {
        PTYPE_HS_S2M: "I 36x",
        PTYPE_HEART_BEAT: "40x",
        PTYPE_HS_M2S: "I 36x",
    }

    def __init__(self, pkg_ver=b'\x01', pkg_type=0,
                 prgm_ver=INTERNAL_VERSION, data=(),
                 raw=None,
                 ):
        self.pkg_ver = pkg_ver
        self.pkg_type = pkg_type
        self.prgm_ver = prgm_ver
        self.data = data
        if raw:
            self.raw = raw
        else:
            self._build_bytes()

    def _build_bytes(self):
        self.raw = struct.pack(
            self.FORMAT_PKG,
            self.pkg_ver,
            self.pkg_type,
            self.prgm_ver,
            self.data_encode(self.pkg_type, self.data),
        )

    @classmethod
    def data_decode(cls, ptype, data_raw):
        return struct.unpack(cls.FORMATS_DATA[ptype], data_raw)

    @classmethod
    def data_encode(cls, ptype, data):
        return struct.pack(cls.FORMATS_DATA[ptype], *data)

    def verify(self, pkg_type=None):
        if pkg_type is not None and self.pkg_type != pkg_type:
            return False

        elif self.pkg_type == self.PTYPE_HS_S2M:
            # Slaver-->Master 的握手响应包
            return self.data[0] == self.SECRET_KEY_REVERSED_CRC32

        elif self.pkg_type == self.PTYPE_HEART_BEAT:
            # 心跳
            return True

        elif self.pkg_type == self.PTYPE_HS_M2S:
            # Master-->Slaver 的握手包
            return self.data[0] == self.SECRET_KEY_CRC32

        else:
            return True

    @classmethod
    def decode_only(cls, raw):
        if len(raw) != cls.PACKAGE_SIZE:
            raise ValueError("content size should be {}, but {}".format(
                cls.PACKAGE_SIZE, len(raw)
            ))
        pkg_ver, pkg_type, prgm_ver, data_raw = struct.unpack(cls.FORMAT_PKG, raw)
        data = cls.data_decode(pkg_type, data_raw)

        return cls(
            pkg_ver=pkg_ver, pkg_type=pkg_type,
            prgm_ver=prgm_ver,
            data=data,
            raw=raw,
        )

    @classmethod
    def decode_verify(cls, raw, pkg_type=None):
        try:
            pkg = cls.decode_only(raw)
        except:
            return None, False
        else:
            return pkg, pkg.verify(pkg_type=pkg_type)

    @classmethod
    def pbuild_hs_m2s(cls):
        return cls(
            pkg_type=cls.PTYPE_HS_M2S,
            data=(cls.SECRET_KEY_CRC32,),
        )

    @classmethod
    def pbuild_hs_s2m(cls):
        return cls(
            pkg_type=cls.PTYPE_HS_S2M,
            data=(cls.SECRET_KEY_REVERSED_CRC32,),
        )

    @classmethod
    def pbuild_heart_beat(cls):
        return cls(
            pkg_type=cls.PTYPE_HEART_BEAT,
        )

    def __str__(self):
        return """CtrlPkg<pkg_ver: {} pkg_type:{} prgm_ver:{} data:{}>""".format(
            self.pkg_ver,
            self.pkg_type,
            self.prgm_ver,
            self.data,
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

        self.thread_pool["heart_beat_daemon"] = threading.Thread(
            target=self._heart_beat_daemon,
            name="heart_beat_daemon-{}".format(fmt_addr(self.customer_listen_addr)),
        )

        self.working_pool = working_pool or {}

    def start_daemon(self):
        if not self.external_slaver:
            self.thread_pool["listen_slaver"].start()
        self.thread_pool["heart_beat_daemon"].start()
        self.thread_pool["listen_customer"].start()

    def join(self):
        if not self.external_slaver:
            self.thread_pool["listen_slaver"].join()
        self.thread_pool["heart_beat_daemon"].join()
        self.thread_pool["listen_customer"].join()

    def _serving_customer(self, conn_customer, conn_slaver):
        addr_customer = conn_customer.getpeername()

        try:
            SocketBridge(conn_customer, conn_slaver).duplex_transfer()
        finally:
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

    @staticmethod
    def _is_heart_beat_alive(conn_slaver):
        conn_slaver.send(CtrlPkg.pbuild_heart_beat().raw)

        buff = select_recv(conn_slaver, CtrlPkg.PACKAGE_SIZE, 2)
        if buff is None:
            return False

        pkg, verify = CtrlPkg.decode_verify(buff, CtrlPkg.PTYPE_HEART_BEAT)

        return verify

    def _heart_beat_daemon(self):
        default_delay = 5 + SPARE_SLAVER_TTL // 5
        delay = default_delay
        log.info("heart beat daemon start, delay: {}s".format(delay))
        while True:
            time.sleep(delay)
            log.debug("heart_beat_daemon: hello! im weak")

            slaver_count = len(self.slaver_pool)
            if not slaver_count:
                log.warning("heart_beat_daemon: sorry, no slaver available, keep sleeping")
                delay = default_delay
                continue
            else:
                delay = 1 + SPARE_SLAVER_TTL // (slaver_count + 1)

            slaver = self.slaver_pool.popleft()

            try:
                hb = self._is_heart_beat_alive(slaver["conn_slaver"])
            except:
                hb = False
            if not hb:
                log.warning("heart beat failed: {}".format(slaver["addr_slaver"]))
                del slaver["conn_slaver"]

            else:
                log.debug("heart beat success: {}".format(slaver["addr_slaver"]))
                self.slaver_pool.append(slaver)

    @staticmethod
    def _handshake(conn_slaver):
        conn_slaver.send(CtrlPkg.pbuild_hs_m2s().raw)

        buff = select_recv(conn_slaver, CtrlPkg.PACKAGE_SIZE, 2)
        if buff is None:
            return False

        pkg, verify = CtrlPkg.decode_verify(buff, CtrlPkg.PTYPE_HS_S2M)

        log.debug("HsPkg from {}: {}".format(conn_slaver.getpeername(), pkg))

        return verify

    def _get_workable_slaver(self):
        try_count = 3
        while True:
            try:
                dict_slaver = self.slaver_pool.popleft()
            except:
                log.error("!!NO SLAVER AVAILABLE!!")
                if try_count:
                    time.sleep(0.1)
                    try_count -= 1
                    continue
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

                time.sleep(0.05)
                return None

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

                continue

            self.working_pool[addr_customer] = {
                "addr_customer": addr_customer,
                "conn_customer": conn_customer,
                "conn_slaver": conn_slaver,
            }

            try:
                t = threading.Thread(target=self._serving_customer,
                                     name="customer-{}".format(fmt_addr(addr_customer)),
                                     args=(conn_customer, conn_slaver),
                                     )
                self.thread_pool["customer"][addr_customer] = t
                t.start()
            except:
                try:
                    try_close(conn_customer)

                    del self.thread_pool["customer"][addr_customer]

                except:
                    pass
                continue


class Slaver:
    """
    slaver socket阶段
        连接master->等待->心跳(重复)--->握手-正式传输数据->退出
    """

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
        while True:  # 可能会有一段时间的心跳包

            buff = select_recv(conn_slaver, CtrlPkg.PACKAGE_SIZE, SPARE_SLAVER_TTL)

            pkg, verify = CtrlPkg.decode_verify(buff)  # type: CtrlPkg,bool

            if not verify:
                return False

            log.debug("CtrlPkg from {}: {}".format(conn_slaver.getpeername(), pkg))

            if pkg.pkg_type == CtrlPkg.PTYPE_HEART_BEAT:
                # 心跳包

                try:
                    conn_slaver.send(CtrlPkg.pbuild_heart_beat().raw)
                except:
                    return False

            elif pkg.pkg_type == CtrlPkg.PTYPE_HS_M2S:
                # 拿到了开始传输的握手包, 进入工作阶段

                break

        try:
            conn_slaver.send(CtrlPkg.pbuild_hs_s2m().raw)
        except:
            return False

        return True

    def _handling_command(self, conn_slaver):
        addr_slaver = conn_slaver.getsockname()
        addr_master = conn_slaver.getpeername()

        try:
            hs = self._waiting_handshake(conn_slaver)
        except:
            log.debug("slaver waiting handshake failed\n {}".format(traceback.print_exc()))
            hs = False
        if not hs:
            # 握手失败或超时等情况
            log.warning("bad handshake from: {}".format(addr_master))
            del self.spare_slaver_pool[addr_slaver]
            try_close(conn_slaver)

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

            del self.working_pool[addr_slaver]
            return
        self.working_pool[addr_slaver]["conn_target"] = conn_target

        try:
            SocketBridge(conn_slaver, conn_target).duplex_transfer()
        finally:
            del self.working_pool[addr_slaver]

        log.info("complete: {}".format(addr_slaver))

    def serve_forever(self):
        err_delay = 0
        spare_delay = 0.1
        DEFAULT_SPARE_DELAY = 0.1
        MAX_ERR_DELAY = 15

        while True:
            if len(self.spare_slaver_pool) >= self.max_spare_count:
                time.sleep(spare_delay)
                spare_delay = (spare_delay + DEFAULT_SPARE_DELAY) / 2.0
                continue
            else:
                spare_delay = 0.0

            try:
                conn_slaver = self._connect_master()
            except:
                log.warning("unable to connect master\n {}".format(traceback.format_exc()))
                time.sleep(err_delay)
                if err_delay < MAX_ERR_DELAY:
                    err_delay += 1
                continue
            else:
                if err_delay:
                    err_delay = 0

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
                time.sleep(err_delay)

                if err_delay < MAX_ERR_DELAY:
                    err_delay += 1
                continue
            else:
                if err_delay:
                    err_delay = 0


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
    target_addr = ("93.184.216.34", 80)  # www.example.com

    if sys.argv[1] == "-m":
        run_master(communicate_addr, customer_listen_addr)
    elif sys.argv[1] == "-s":
        run_slaver(communicate_addr, target_addr, max_spare_count=5)


if __name__ == '__main__':
    main()
