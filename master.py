#!/usr/bin/env python3
# coding=utf-8
import os
import tempfile
import queue
import atexit
from common_func import *

_listening_sockets = []  # for close at exit
__author__ = "Aploium <i@z.codes>"
__website__ = "https://github.com/aploium/shootback"

_DEFAULT_SSL_KEY = '''\
-----BEGIN PRIVATE KEY-----
MIICdQIBADANBgkqhkiG9w0BAQEFAASCAl8wggJbAgEAAoGBAMqPYWmoQJaHwu3/
0Q5R8RFpOzFwys/5xgypGMbFmW0oSpiVMA0u+lakxb7Z46RU/Y/kV4XUjB9+uDcI
FlU520jWJVZ9g09gMKaPyDjxdKueKl2KDfn7i4unBQwouqzipPWXv0L8RnHF1gzG
XgN/Dh6+5ZoY53JvUZaFbQhjjtkjAgMBAAECgYB8WRjL6+X6gs0/ndOQnu0GaztT
VpKqqgLSstvq6lMNl7ZzhOJCtZwopG5ggxIkR6iBNQQlvB1pGDmuTuCm4SWjsXsS
x2iDvPTlx9Y1ke9SWszZ6mOvumeHLnnxU0tp+ECySphQN5XxzH2yHzeXVTWpIim6
ADFrtbltaPFLghZbQQJBAOT0tU+QLtlaKit6iY1QrZntsVzeCZYxh2ktz4Hsk8RN
K4U4FCoWz3rJrkj1gFIrgoSgQ6+VzjXzFDbKJeP7b38CQQDifIBhi9JAVKlPgklG
e595EYa3J4a601AAIxYVVwyfn8CiUUHCHKM+roJywh0uvKVXN6sn1Wi4zU8H8BQ1
9KhdAkAnIY/PfmQTb+6fKb1SssRI97AFoElhKyvqlRLPMOD8fvf+N9xyaR2i7c9k
1tjMsnUHN+D5pI/u9pGw35HkSjf/AkB4rpCV6bQhtTr2c9zpoqvKDi2zYGtpF3oU
aJ22x0ihsbUqiJO6hBn0J3a5AXgdVEXh4Hbh5dRETJnlB+ctDO29AkBXDgUXbltm
CRtey5ZwnlAKI8jwx3q4jaldTCJrwThfgNdlvWPnKjPhIYG/OogLAm82grruSzOQ
a2xDsGiXCZoM
-----END PRIVATE KEY-----
'''
_DEFAULT_SSL_CERT = '''\
-----BEGIN CERTIFICATE-----
MIICmzCCAgSgAwIBAgIJANe4OK2h+u9iMA0GCSqGSIb3DQEBCwUAMGUxCzAJBgNV
BAYTAlVTMRMwEQYDVQQIDApTb21lLVN0YXRlMS0wKwYDVQQKDCRodHRwczovL2dp
dGh1Yi5jb20vYXBsb2l1bS9zaG9vdGJhY2sxEjAQBgNVBAMMCWxvY2FsaG9zdDAe
Fw0xODEwMjQxNjA5MDVaFw0yODEwMjExNjA5MDVaMGUxCzAJBgNVBAYTAlVTMRMw
EQYDVQQIDApTb21lLVN0YXRlMS0wKwYDVQQKDCRodHRwczovL2dpdGh1Yi5jb20v
YXBsb2l1bS9zaG9vdGJhY2sxEjAQBgNVBAMMCWxvY2FsaG9zdDCBnzANBgkqhkiG
9w0BAQEFAAOBjQAwgYkCgYEAyo9haahAlofC7f/RDlHxEWk7MXDKz/nGDKkYxsWZ
bShKmJUwDS76VqTFvtnjpFT9j+RXhdSMH364NwgWVTnbSNYlVn2DT2Awpo/IOPF0
q54qXYoN+fuLi6cFDCi6rOKk9Ze/QvxGccXWDMZeA38OHr7lmhjncm9RloVtCGOO
2SMCAwEAAaNTMFEwHQYDVR0OBBYEFEJESWZQ80wg2+1HmLW8nFtYyg9NMB8GA1Ud
IwQYMBaAFEJESWZQ80wg2+1HmLW8nFtYyg9NMA8GA1UdEwEB/wQFMAMBAf8wDQYJ
KoZIhvcNAQELBQADgYEAJD3ZWeLJaxWyCVSsKSxFZfTyYMhKSI9UjssDU48ENbfz
hTdU2KxHFmRlTIdl02BcvpjiCHOCYxGkSBXctpXAHp9oU8fpzNIdekmgmZD2GjHS
NbE3PCsO3NNtnc3Oj96HCww2MeC0ro9j0uDkyl+0zx47UeFUDqqSFJZ3bQa8j0w=
-----END CERTIFICATE-----
'''


@atexit.register
def close_listening_socket_at_exit():
    log.info("exiting...")
    for s in _listening_sockets:
        log.info("closing: {}".format(s))
        try_close(s)


def try_bind_port(sock, addr):
    while True:
        try:
            sock.bind(addr)
        except Exception as e:
            log.error(("unable to bind {}, {}. If this port was used by the recently-closed shootback itself\n"
                       "then don't worry, it would be available in several seconds\n"
                       "we'll keep trying....").format(addr, e))
            log.debug(traceback.format_exc())
            time.sleep(3)
        else:
            break


class Master(object):
    def __init__(self, customer_listen_addr, communicate_addr=None,
                 slaver_pool=None, working_pool=None,
                 ssl=False
                 ):
        """

        :param customer_listen_addr: equals to the -c/--customer param
        :param communicate_addr: equals to the -m/--master param
        """
        self.thread_pool = {}
        self.thread_pool["spare_slaver"] = {}
        self.thread_pool["working_slaver"] = {}

        self.working_pool = working_pool or {}

        self.socket_bridge = SocketBridge()

        # a queue for customers who have connected to us,
        #   but not assigned a slaver yet
        self.pending_customers = queue.Queue()

        if ssl:
            self.ssl_context = self._make_ssl_context()
            self.ssl_avail = self.ssl_context is not None
        else:
            self.ssl_avail = False
            self.ssl_context = None

        self.communicate_addr = communicate_addr

        _fmt_communicate_addr = fmt_addr(self.communicate_addr)

        if slaver_pool:
            # 若使用外部slaver_pool, 就不再初始化listen
            # 这是以后待添加的功能
            self.external_slaver = True
            self.thread_pool["listen_slaver"] = None
        else:
            # 自己listen来获取slaver
            self.external_slaver = False
            self.slaver_pool = collections.deque()
            # prepare Thread obj, not activated yet
            self.thread_pool["listen_slaver"] = threading.Thread(
                target=self._listen_slaver,
                name="listen_slaver-{}".format(_fmt_communicate_addr),
                daemon=True,
            )

        # prepare Thread obj, not activated yet
        self.customer_listen_addr = customer_listen_addr
        self.thread_pool["listen_customer"] = threading.Thread(
            target=self._listen_customer,
            name="listen_customer-{}".format(_fmt_communicate_addr),
            daemon=True,
        )

        # prepare Thread obj, not activated yet
        self.thread_pool["heart_beat_daemon"] = threading.Thread(
            target=self._heart_beat_daemon,
            name="heart_beat_daemon-{}".format(_fmt_communicate_addr),
            daemon=True,
        )

        # prepare assign_slaver_daemon
        self.thread_pool["assign_slaver_daemon"] = threading.Thread(
            target=self._assign_slaver_daemon,
            name="assign_slaver_daemon-{}".format(_fmt_communicate_addr),
            daemon=True,
        )

    def serve_forever(self):
        if not self.external_slaver:
            self.thread_pool["listen_slaver"].start()
        self.thread_pool["heart_beat_daemon"].start()
        self.thread_pool["listen_customer"].start()
        self.thread_pool["assign_slaver_daemon"].start()
        self.thread_pool["socket_bridge"] = self.socket_bridge.start_as_daemon()

        while True:
            time.sleep(10)

    def _make_ssl_context(self):
        if ssl is None:
            log.warning('ssl module is NOT valid in this machine! Fallback to plain')
            return None

        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.check_hostname = False
        ctx.load_default_certs(ssl.Purpose.SERVER_AUTH)
        ctx.verify_mode = ssl.CERT_NONE

        _certfile = tempfile.mktemp()
        open(_certfile, 'w').write(_DEFAULT_SSL_CERT)
        _keyfile = tempfile.mktemp()
        open(_keyfile, 'w').write(_DEFAULT_SSL_KEY)
        ctx.load_cert_chain(_certfile, _keyfile)
        os.remove(_certfile)
        os.remove(_keyfile)

        return ctx

    def _transfer_complete(self, addr_customer):
        """a callback for SocketBridge, do some cleanup jobs"""
        log.info("customer complete: {}".format(addr_customer))
        del self.working_pool[addr_customer]

    def _serve_customer(self, conn_customer, conn_slaver):
        """put customer and slaver sockets into SocketBridge, let them exchange data"""
        self.socket_bridge.add_conn_pair(
            conn_customer, conn_slaver,
            functools.partial(  # it's a callback
                # 这个回调用来在传输完成后删除工作池中对应记录
                self._transfer_complete,
                conn_customer.getpeername()
            )
        )

    @staticmethod
    def _send_heartbeat(conn_slaver):
        """send and verify heartbeat pkg"""
        conn_slaver.send(CtrlPkg.pbuild_heart_beat().raw)

        pkg, verify = CtrlPkg.recv(
            conn_slaver, expect_ptype=CtrlPkg.PTYPE_HEART_BEAT)  # type: CtrlPkg,bool

        if not verify:
            return False

        if pkg.prgm_ver < 0x000B:
            # shootback before 2.2.5-r10 use two-way heartbeat
            #   so there is no third pkg to send
            pass
        else:
            # newer version use TCP-like 3-way heartbeat
            #   the older 2-way heartbeat can't only ensure the
            #   master --> slaver pathway is OK, but the reverse
            #   communicate may down. So we need a TCP-like 3-way
            #   heartbeat
            conn_slaver.send(CtrlPkg.pbuild_heart_beat().raw)

        return verify

    def _heart_beat_daemon(self):
        """

        每次取出slaver队列头部的一个, 测试心跳, 并把它放回尾部.
            slaver若超过 SPARE_SLAVER_TTL 秒未收到心跳, 则会自动重连
            所以睡眠间隔(delay)满足   delay * slaver总数  < TTL
            使得一轮循环的时间小于TTL,
            保证每个slaver都在过期前能被心跳保活
        """
        default_delay = 5 + SPARE_SLAVER_TTL // 12
        delay = default_delay
        log.info("heart beat daemon start, delay: {}s".format(delay))
        while True:
            time.sleep(delay)
            # log.debug("heart_beat_daemon: hello! im weak")

            # ---------------------- preparation -----------------------
            slaver_count = len(self.slaver_pool)
            if not slaver_count:
                log.warning("heart_beat_daemon: sorry, no slaver available, keep sleeping")
                # restore default delay if there is no slaver
                delay = default_delay
                continue
            else:
                # notice this `slaver_count*2 + 1`
                # slaver will expire and re-connect if didn't receive
                #   heartbeat pkg after SPARE_SLAVER_TTL seconds.
                # set delay to be short enough to let every slaver receive heartbeat
                #   before expire
                delay = 1 + SPARE_SLAVER_TTL // max(slaver_count * 2 + 1, 12)

            # pop the oldest slaver
            #   heartbeat it and then put it to the end of queue
            slaver = self.slaver_pool.popleft()
            addr_slaver = slaver["addr_slaver"]

            # ------------------ real heartbeat begin --------------------
            start_time = time.perf_counter()
            try:
                hb_result = self._send_heartbeat(slaver["conn_slaver"])
            except Exception as e:
                log.warning("error during heartbeat to {}: {}".format(
                    fmt_addr(addr_slaver), e))
                log.debug(traceback.format_exc())
                hb_result = False
            finally:
                time_used = round((time.perf_counter() - start_time) * 1000.0, 2)
            # ------------------ real heartbeat end ----------------------

            if not hb_result:
                log.warning("heart beat failed: {}, time: {}ms".format(
                    fmt_addr(addr_slaver), time_used))
                try_close(slaver["conn_slaver"])
                del slaver["conn_slaver"]

                # if heartbeat failed, start the next heartbeat immediately
                #   because in most cases, all 5 slaver connection will
                #   fall and re-connect in the same time
                delay = 0

            else:
                log.debug("heartbeat success: {}, time: {}ms".format(
                    fmt_addr(addr_slaver), time_used))
                self.slaver_pool.append(slaver)

    def _handshake(self, conn_slaver):
        """
        handshake before real data transfer
        it ensures:
            1. client is alive and ready for transmission
            2. client is shootback_slaver, not mistakenly connected other program
            3. verify the SECRET_KEY, establish SSL
            4. tell slaver it's time to connect target

        handshake procedure:
            1. master hello --> slaver
            2. slaver verify master's hello
            3. slaver hello --> master
            4. (immediately after 3) slaver connect to target
            4. master verify slaver
            5. [optional] establish SSL
            6. enter real data transfer

        Args:
            conn_slaver (socket.socket)
        Return:
            socket.socket|ssl.SSLSocket: socket obj(may be ssl-socket) if handshake success, else None
        """
        conn_slaver.send(CtrlPkg.pbuild_hs_m2s(ssl_avail=self.ssl_avail).raw)

        buff = select_recv(conn_slaver, CtrlPkg.PACKAGE_SIZE, 2)
        if buff is None:
            return None

        pkg, correct = CtrlPkg.decode_verify(buff, CtrlPkg.PTYPE_HS_S2M)  # type: CtrlPkg,bool

        if not correct:
            return None

        if not self.ssl_avail or pkg.data[1] == CtrlPkg.SSL_FLAG_NONE:
            if self.ssl_avail:
                log.warning('client %s not enabled SSL, fallback to plain.', conn_slaver.getpeername())
            return conn_slaver
        else:
            ssl_conn_slaver = self.ssl_context.wrap_socket(conn_slaver, server_side=True)  # type: ssl.SSLSocket
            log.debug('ssl established slaver: %s', ssl_conn_slaver.getpeername())
            return ssl_conn_slaver

    def _get_an_active_slaver(self):
        """get and activate an slaver for data transfer"""
        try_count = 100
        while True:
            try:
                dict_slaver = self.slaver_pool.popleft()
            except:
                if try_count:
                    time.sleep(0.02)
                    try_count -= 1
                    if try_count % 10 == 0:
                        log.error("!!NO SLAVER AVAILABLE!!  trying {}".format(try_count))
                    continue
                return None

            conn_slaver = dict_slaver["conn_slaver"]

            try:
                # this returned conn may be ssl-socket or plain socket
                actual_conn = self._handshake(conn_slaver)
            except Exception as e:
                log.warning("Handshake failed. %s %s", dict_slaver["addr_slaver"], e)
                log.debug(traceback.format_exc())
                actual_conn = None

            if actual_conn is not None:
                return actual_conn
            else:
                log.warning("slaver handshake failed: %s", dict_slaver["addr_slaver"])
                try_close(conn_slaver)

                time.sleep(0.02)

    def _assign_slaver_daemon(self):
        """assign slaver for customer"""
        while True:
            # get a newly connected customer
            conn_customer, addr_customer = self.pending_customers.get()

            try:
                conn_slaver = self._get_an_active_slaver()
            except:
                log.error('error in getting slaver', exc_info=True)
                continue
            if conn_slaver is None:
                log.warning("Closing customer[%s] because no available slaver found", addr_customer)
                try_close(conn_customer)

                continue
            else:
                log.debug("Using slaver: %s for %s", conn_slaver.getpeername(), addr_customer)

            self.working_pool[addr_customer] = {
                "addr_customer": addr_customer,
                "conn_customer": conn_customer,
                "conn_slaver": conn_slaver,
            }

            try:
                self._serve_customer(conn_customer, conn_slaver)
            except:
                log.error('error adding to socket_bridge', exc_info=True)
                try_close(conn_customer)
                try_close(conn_slaver)
                continue

    def _listen_slaver(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try_bind_port(sock, self.communicate_addr)
        sock.listen(10)
        _listening_sockets.append(sock)
        log.info("Listening for slavers: {}".format(
            fmt_addr(self.communicate_addr)))
        while True:
            conn, addr = sock.accept()
            self.slaver_pool.append({
                "addr_slaver": addr,
                "conn_slaver": conn,
            })
            log.info("Got slaver {} Total: {}".format(
                fmt_addr(addr), len(self.slaver_pool)
            ))

    def _listen_customer(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try_bind_port(sock, self.customer_listen_addr)
        sock.listen(20)
        _listening_sockets.append(sock)
        log.info("Listening for customers: {}".format(
            fmt_addr(self.customer_listen_addr)))
        while True:
            conn_customer, addr_customer = sock.accept()
            log.info("Serving customer: {} Total customers: {}".format(
                addr_customer, self.pending_customers.qsize() + 1
            ))

            # just put it into the queue,
            #   let _assign_slaver_daemon() do the else
            #   don't block this loop
            self.pending_customers.put((conn_customer, addr_customer))


def run_master(communicate_addr, customer_listen_addr, ssl=False):
    log.info("shootback {} running as master".format(version_info()))
    log.info("author: {}  site: {}".format(__author__, __website__))
    log.info("slaver from: {} customer from: {}".format(
        fmt_addr(communicate_addr), fmt_addr(customer_listen_addr)))

    Master(customer_listen_addr, communicate_addr, ssl=ssl).serve_forever()


def argparse_master():
    import argparse
    parser = argparse.ArgumentParser(
        description="""shootback (master) {ver}
A fast and reliable reverse TCP tunnel. (this is master)
Help access local-network service from Internet.
https://github.com/aploium/shootback""".format(ver=version_info()),
        epilog="""
Example1:
tunnel local ssh to public internet, assume master's ip is 1.2.3.4
  Master(this pc):                master.py -m 0.0.0.0:10000 -c 0.0.0.0:10022
  Slaver(another private pc):     slaver.py -m 1.2.3.4:10000 -t 127.0.0.1:22
  Customer(any internet user):    ssh 1.2.3.4 -p 10022
  the actual traffic is:  customer <--> master(1.2.3.4 this pc) <--> slaver(private network) <--> ssh(private network)

Example2:
Tunneling for www.example.com
  Master(this pc):                master.py -m 127.0.0.1:10000 -c 127.0.0.1:10080
  Slaver(this pc):                slaver.py -m 127.0.0.1:10000 -t example.com:80
  Customer(this pc):              curl -v -H "host: example.com" 127.0.0.1:10080

Tips: ANY service using TCP is shootback-able.  HTTP/FTP/Proxy/SSH/VNC/...
""",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("-m", "--master", required=True,
                        metavar="host:port",
                        help="listening for slavers, usually an Public-Internet-IP. Slaver comes in here  eg: 2.3.3.3:10000")
    parser.add_argument("-c", "--customer", required=True,
                        metavar="host:port",
                        help="listening for customers, 3rd party program connects here  eg: 10.1.2.3:10022")
    parser.add_argument("-k", "--secretkey", default="shootback",
                        help="secretkey to identity master and slaver, should be set to the same value in both side")
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="verbose output")
    parser.add_argument("-q", "--quiet", action="count", default=0,
                        help="quiet output, only display warning and errors, use two to disable output")
    parser.add_argument("-V", "--version", action="version", version="shootback {}-master".format(version_info()))
    parser.add_argument("--ttl", default=300, type=int, dest="SPARE_SLAVER_TTL",
                        help="standing-by slaver's TTL, default is 300. "
                             "In master side, this value affects heart-beat frequency. "
                             "Default value is optimized for most cases")
    parser.add_argument('--ssl', action='store_true', help='[experimental] try using ssl for data encryption. '
                                                           'It may be enabled by default in future version')

    return parser.parse_args()


def main_master():
    global SPARE_SLAVER_TTL
    global SECRET_KEY

    args = argparse_master()

    if args.verbose and args.quiet:
        print("-v and -q should not appear together")
        exit(1)

    communicate_addr = split_host(args.master)
    customer_listen_addr = split_host(args.customer)

    SECRET_KEY = args.secretkey
    CtrlPkg.recalc_crc32()

    SPARE_SLAVER_TTL = args.SPARE_SLAVER_TTL
    if args.quiet < 2:
        if args.verbose:
            level = logging.DEBUG
        elif args.quiet:
            level = logging.WARNING
        else:
            level = logging.INFO
        configure_logging(level)

    run_master(communicate_addr, customer_listen_addr, ssl=args.ssl)


if __name__ == '__main__':
    main_master()
