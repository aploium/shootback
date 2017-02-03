#!/usr/bin/env python3
# coding=utf-8
from common_func import *


class Master:
    def __init__(self, customer_listen_addr, communicate_addr=None,
                 slaver_pool=None, working_pool=None):
        self.thread_pool = {}
        self.thread_pool["spare_slaver"] = {}
        self.thread_pool["working_slaver"] = {}

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
                daemon=True,
            )

        self.customer_listen_addr = customer_listen_addr
        self.thread_pool["listen_customer"] = threading.Thread(
            target=self._listen_customer,
            name="listen_customer-{}".format(fmt_addr(self.customer_listen_addr)),
            daemon=True,
        )

        self.thread_pool["heart_beat_daemon"] = threading.Thread(
            target=self._heart_beat_daemon,
            name="heart_beat_daemon-{}".format(fmt_addr(self.customer_listen_addr)),
            daemon=True,
        )
        self.thread_pool["heart_beat_daemon"].daemon = True

        self.working_pool = working_pool or {}

        self.socket_bridge = SocketBridge()

    def serve_forever(self):
        if not self.external_slaver:
            self.thread_pool["listen_slaver"].start()
        self.thread_pool["heart_beat_daemon"].start()
        self.thread_pool["listen_customer"].start()
        self.thread_pool["socket_bridge"] = self.socket_bridge.start_as_daemon()

        while True:
            time.sleep(10)

    def join(self):
        if not self.external_slaver:
            self.thread_pool["listen_slaver"].join()
        self.thread_pool["heart_beat_daemon"].join()
        self.thread_pool["listen_customer"].join()

    def _transfer_complete(self, addr_customer):
        log.info("customer complete: {}".format(addr_customer))
        del self.working_pool[addr_customer]

    def _serve_customer(self, conn_customer, conn_slaver):
        self.socket_bridge.add_conn_pair(
            conn_customer, conn_slaver,
            functools.partial(
                # 这个回调用来在传输完成后删除工作池中对应记录
                self._transfer_complete,
                conn_customer.getpeername()
            )
        )

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

    def _get_an_active_slaver(self):
        try_count = 3
        while True:
            try:
                dict_slaver = self.slaver_pool.popleft()
            except:
                log.error("!!NO SLAVER AVAILABLE!!  trying {}".format(try_count))
                if try_count:
                    time.sleep(0.1)
                    try_count -= 1
                    continue
                return None

            conn_slaver = dict_slaver["conn_slaver"]

            try:
                hs = self._handshake(conn_slaver)
            except Exception as e:
                log.warning("Handshake failed: {}".format(e))
                log.debug(traceback.format_exc())
                hs = False

            if hs:
                return conn_slaver
            else:
                log.warning("slaver handshake failed: {}".format(dict_slaver["addr_slaver"]))
                try_close(conn_slaver)

                time.sleep(0.05)

    def _listen_customer(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(self.customer_listen_addr)
        sock.listen(20)
        while True:
            conn_customer, addr_customer = sock.accept()
            log.info("Serving customer: {} Total customers: {}".format(
                addr_customer, len(self.working_pool) + 1
            ))

            conn_slaver = self._get_an_active_slaver()
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
                self._serve_customer(conn_customer, conn_slaver)
            except:
                try:
                    try_close(conn_customer)
                except:
                    pass
                continue


def run_master(communicate_addr, customer_listen_addr):
    log.info("{} running as master, slaver from: {} customer_from: {}".format(
        version_info(), communicate_addr, customer_listen_addr
    ))

    Master(customer_listen_addr, communicate_addr).serve_forever()


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
  Customer(any internet user):    ssh 1.2.3.4:10022
  the actual traffic is:  customer <--> master(1.2.3.4 this pc) <--> slaver(private network) <--> ssh(private network)

Example2:
Tunneling for www.example.com
  Master(this pc):                master.py -m 127.0.0.1:10000 -c 127.0.0.1:10080
  Slaver(this pc):                slaver.py -m 127.0.0.1:10000 -t example.com:80
  Customer(this pc):              curl -v -H "host: example.com" localhost:10080

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
    parser.add_argument("--ttl", default=600, type=int, dest="SPARE_SLAVER_TTL",
                        help="standing-by slaver's TTL, default is 600. "
                             "In master side, this value affects heart-beat frequency. "
                             "Default value is optimized for most cases")

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

    log.info("shootback master running")
    log.info("Listening for slavers: {}".format(fmt_addr(communicate_addr)))
    log.info("Listening for customers: {}".format(fmt_addr(customer_listen_addr)))

    # communicate_addr = ("localhost", 10000)
    # customer_listen_addr = ("localhost", 10080)
    # target_addr = ("localhost", 1080)
    # target_addr = ("93.184.216.34", 80)  # www.example.com

    run_master(communicate_addr, customer_listen_addr)


if __name__ == '__main__':
    main_master()
