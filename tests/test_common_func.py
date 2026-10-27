import select as select_module
import socket
import threading
import time
import unittest

import common_func


class SelectRecvTests(unittest.TestCase):
    def setUp(self):
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(1)
        self.sender = socket.create_connection(self.listener.getsockname())
        self.receiver, _ = self.listener.accept()

    def tearDown(self):
        self.receiver.close()
        self.sender.close()
        self.listener.close()

    def test_reads_complete_control_packet_across_tcp_fragments(self):
        payload = common_func.CtrlPkg.pbuild_heart_beat().raw

        def send_fragments():
            try:
                for index in range(len(payload)):
                    self.sender.sendall(payload[index:index + 1])
                    time.sleep(0.01)
            except socket.error:
                pass

        writer = threading.Thread(target=send_fragments)
        writer.daemon = True
        writer.start()
        package, verified = common_func.CtrlPkg.recv(
            self.receiver, timeout=2.0,
            expect_ptype=common_func.CtrlPkg.PTYPE_HEART_BEAT,
        )
        writer.join(2.0)
        self.assertFalse(writer.is_alive(), "synthetic sender did not finish")
        self.assertTrue(verified)
        self.assertEqual(payload, package.raw)

    def test_fallback_select_timeout_does_not_block_in_recv(self):
        original_selectors = common_func.selectors
        original_select = getattr(common_func, "select", None)
        self.receiver.settimeout(0.2)
        common_func.selectors = None
        common_func.select = select_module
        try:
            try:
                common_func.select_recv(self.receiver, 64, timeout=0.02)
            except RuntimeError as error:
                self.assertEqual("recv timeout", str(error))
            else:
                self.fail("select_recv did not raise the expected timeout")
        finally:
            common_func.selectors = original_selectors
            if original_select is None:
                del common_func.select
            else:
                common_func.select = original_select


if __name__ == "__main__":
    unittest.main()
