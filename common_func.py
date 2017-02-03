#!/usr/bin/env python
# coding=utf-8
from __future__ import print_function, unicode_literals, division, absolute_import
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
import functools

log = logging.getLogger(__name__)

# slaver_pool = collections.deque()
# working_pool = {}
# spare_slaver_pool = {}

RECV_BUFFER_SIZE = 2 ** 14  # 16384 bytes
SECRET_KEY = "shootback"
SPARE_SLAVER_TTL = 600  # 600s
INTERNAL_VERSION = 0x0002
__version__ = (2, 1, 0, INTERNAL_VERSION)


def version_info():
    return "{}.{}.{}-r{}".format(*__version__)


def configure_logging(level):
    logging.basicConfig(
        level=level,
        format='[%(levelname)s %(asctime)s %(funcName)s] %(message)s',
    )


def fmt_addr(socket):
    return "{}:{}".format(*socket)


def split_host(x):
    try:
        host, port = x.split(":")
        port = int(port)
    except:
        raise ValueError(
            "wrong syntax, format host:port is "
            "required, not {}".format(x))
    else:
        return host, port


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
    def __init__(self):
        self.conn_rd = set()
        self.map = {}
        self.callbacks = {}

    def add_conn_pair(self, conn1, conn2, callback=None):
        self.conn_rd.update((conn1, conn2))
        self.map[conn1] = conn2
        self.map[conn2] = conn1
        if callback is not None:
            self.callbacks[conn1] = callback

    def start_as_daemon(self):
        t = threading.Thread(target=self.start)
        t.daemon = True
        t.start()
        log.info("SocketBridge daemon started")
        return t

    def start(self):
        while True:
            try:
                self._start()
            except:
                log.error("FATAL ERROR! SocketBridge failed {}".format(
                    traceback.format_exc()
                ))

    def _start(self):
        buff = memoryview(bytearray(RECV_BUFFER_SIZE))
        while True:
            if not self.conn_rd:
                time.sleep(0.1)
                continue

            r, w, e = select.select(self.conn_rd, [], [])

            for s in r:
                try:
                    rec_len = s.recv_into(buff, RECV_BUFFER_SIZE)
                except:
                    self._rd_close(s)
                    continue

                if not rec_len:
                    self._rd_close(s)
                    continue
                try:
                    self.map[s].send(buff[:rec_len])
                except:
                    log.debug(e)
                    continue

    def _rd_close(self, conn, once=False):
        if conn in self.conn_rd:
            self.conn_rd.remove(conn)
        try:
            conn.shutdown(socket.SHUT_RD)
        except:
            pass

        if not once:
            self._wr_close(self.map[conn], True)

        if self.map.get(conn) not in self.conn_rd:
            self._terminate(conn)

    def _wr_close(self, conn, once=False):
        if not once:
            self._rd_close(self.map.get(conn), True)
        try:
            conn.shutdown(socket.SHUT_WR)
        except:
            pass

    def _terminate(self, conn):
        try_close(conn)
        if conn in self.map:
            _mapped_conn = self.map[conn]
            try_close(_mapped_conn)
            if _mapped_conn in self.map:
                del self.map[_mapped_conn]
        else:
            _mapped_conn = None
        del self.map[conn]

        if conn in self.callbacks:
            self.callbacks[conn]()
            del self.callbacks[conn]
        elif _mapped_conn and _mapped_conn in self.callbacks:
            self.callbacks[_mapped_conn]()
            del self.callbacks[_mapped_conn]


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

    _PREBUILT_PKG = {}

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
    def recalc_crc32(cls):
        cls.SECRET_KEY_CRC32 = binascii.crc32(SECRET_KEY.encode('utf-8')) & 0xffffffff
        cls.SECRET_KEY_REVERSED_CRC32 = binascii.crc32(SECRET_KEY[::-1].encode('utf-8')) & 0xffffffff

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
    def _prebuilt_pkg(cls, pkg_type, fallback):
        if pkg_type not in cls._PREBUILT_PKG:
            pkg = fallback(force_rebuilt=True)
            cls._PREBUILT_PKG[pkg_type] = pkg

        return cls._PREBUILT_PKG[pkg_type]

    @classmethod
    def pbuild_hs_m2s(cls, force_rebuilt=False):
        # because py27 do not have functools.lru_cache, so we must write our own
        if force_rebuilt:
            return cls(
                pkg_type=cls.PTYPE_HS_M2S,
                data=(cls.SECRET_KEY_CRC32,),
            )
        else:
            return cls._prebuilt_pkg(cls.PTYPE_HS_M2S, cls.pbuild_hs_m2s)

    @classmethod
    def pbuild_hs_s2m(cls, force_rebuilt=False):
        if force_rebuilt:
            return cls(
                pkg_type=cls.PTYPE_HS_S2M,
                data=(cls.SECRET_KEY_REVERSED_CRC32,),
            )
        else:
            return cls._prebuilt_pkg(cls.PTYPE_HS_S2M, cls.pbuild_hs_s2m)

    @classmethod
    def pbuild_heart_beat(cls, force_rebuilt=False):
        if force_rebuilt:
            return cls(
                pkg_type=cls.PTYPE_HEART_BEAT,
            )
        else:
            return cls._prebuilt_pkg(cls.PTYPE_HEART_BEAT, cls.pbuild_heart_beat)

    def __str__(self):
        return """CtrlPkg<pkg_ver: {} pkg_type:{} prgm_ver:{} data:{}>""".format(
            self.pkg_ver,
            self.pkg_type,
            self.prgm_ver,
            self.data,
        )

    def __repr__(self):
        return self.__str__()
