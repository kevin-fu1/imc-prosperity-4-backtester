import webbrowser
from functools import partial
from http.server import SimpleHTTPRequestHandler, HTTPServer
from pathlib import Path
from time import monotonic
from typing import Any
from urllib.parse import urlsplit


class HTTPRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, target_path: str, **kwargs) -> None:
        self.target_path = target_path
        super().__init__(*args, **kwargs)

    def do_GET(self):
        response = super().do_GET()
        if urlsplit(self.path).path == self.target_path:
            self.server.shutdown_flag = True
        return response

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def end_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("Access-Control-Allow-Private-Network", "true")
        self.send_header("Cache-Control", "no-store")
        return super().end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        return


class CustomHTTPServer(HTTPServer):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.shutdown_flag = False


class Visualizer(object):
    def open(self, output_file: Path):
        http_handler = partial(
            HTTPRequestHandler,
            directory=str(output_file.parent),
            target_path=f"/{output_file.name}",
        )
        http_server = CustomHTTPServer(("127.0.0.1", 0), http_handler)
        http_server.timeout = 1
        visualizer_url = (
            "https://kevin-fu1.github.io/imc-prosperity-4-visualizer/"
            f"?open=http://127.0.0.1:{http_server.server_port}/{output_file.name}"
        )
        webbrowser.open(visualizer_url)

        deadline = monotonic() + 30
        try:
            while not http_server.shutdown_flag and monotonic() < deadline:
                http_server.handle_request()
        finally:
            http_server.server_close()

        if not http_server.shutdown_flag:
            print(
                "Visualizer did not connect within 30 seconds. "
                f"Open the saved log manually if needed: {output_file}"
            )
