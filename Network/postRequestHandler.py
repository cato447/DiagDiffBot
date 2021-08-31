from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import logging
class BasicHandler(BaseHTTPRequestHandler):
    def __init__(self, request: bytes, client_address, server) -> None:
        self.queue = server.queue
        self.logger = logging.getLogger(__name__)
        super().__init__(request, client_address, server)

    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def log_message(self, format: str, *args) -> None:
        return

    def do_POST(self):
        self._set_headers()
        try:
            self.data_string = self.rfile.read(int(self.headers['Content-Length']))
            data = json.loads(self.data_string)
            try:
                self.logger.info(f"recieved button input: {data['context']['action']}")
            except:
                self.logger.info(f"Recieved post request without ['context']['action'] key")

        except:
            data = "{}"
        self.queue.put(data)

class QueueServer(HTTPServer):
    def __init__(self, server_address, RequestHandlerClass, bind_and_activate: bool, queue) -> None:
        super().__init__(server_address, RequestHandlerClass, bind_and_activate=bind_and_activate)
        self.queue = queue

class PostRequestHandler():
    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def run(self, queue, isAlive, addr="localhost", port=8000, server_class=QueueServer, handler_class=BasicHandler):
        server_address = (addr, port)
        httpd = server_class(server_address, handler_class, True, queue)

        self.logger.info(f"Starting httpd server on {addr}:{port}")
        while isAlive:
            httpd.handle_request()
        httpd.server_close()
