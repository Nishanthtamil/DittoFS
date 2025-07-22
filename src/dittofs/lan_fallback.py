import socket
from zeroconf import Zeroconf, ServiceBrowser, ServiceListener

SERVICE_TYPE = "_dittofs._tcp.local."
PORT = 8765

class LANTransport:
    def serve(self, payload: bytes):
        # advertise via mDNS (blocking)
        from zeroconf import ServiceInfo, Zeroconf
        info = ServiceInfo(
            SERVICE_TYPE,
            f"DittoFS-{socket.gethostname()}._dittofs._tcp.local.",
            port=PORT,
            properties={},
            server=f"{socket.gethostname()}.local.",
            addresses=[socket.inet_aton("127.0.0.1")],
        )
        zc = Zeroconf()
        zc.register_service(info)

        # blocking TCP server
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("0.0.0.0", PORT))
            s.listen(1)
            print("Serving on port", PORT)
            conn, _ = s.accept()
            conn.sendall(payload)
            conn.close()

    def fetch(self, timeout=5):
        """Browse the first service and connect."""
        found = []

        class Listener(ServiceListener):
            def add_service(self, zc, type_, name):
                info = zc.get_service_info(type_, name)
                if info:
                    found.append(info)

        zc = Zeroconf()
        browser = ServiceBrowser(zc, SERVICE_TYPE, Listener())
        import time
        time.sleep(min(timeout, 2))  # quick scan
        zc.close()
        if not found:
            return None
        info = found[0]
        host, port = info.parsed_addresses()[0], info.port
        with socket.create_connection((host, port), timeout=timeout) as s:
            return s.recv(1024)