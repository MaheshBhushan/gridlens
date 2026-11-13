"""The range-request zip extractor against a local HTTP server that honours Range headers."""

import http.server
import io
import threading
import zipfile

import pytest

from gridlens.download import download_parts


def make_zip() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for i in range(3):
            body = "LCLid,stdorToU,DateTime,KWH/hh (per half hour) \n" + "".join(
                f"MAC00000{i},Std,2012-10-12 00:{m:02d}:00.0000000,0.{i}{m} \n" for m in (0, 30)
            ) * 500
            zf.writestr(f"Small LCL Data/part_{i}.csv", body)
        zf.writestr("Small LCL Data/readme.txt", "not a csv")
    return buf.getvalue()


@pytest.fixture()
def server():
    payload = make_zip()

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_HEAD(self):
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()

        def do_GET(self):
            start, end = self.headers["Range"].removeprefix("bytes=").split("-")
            chunk = payload[int(start) : int(end) + 1]
            self.send_response(206)
            self.send_header("Content-Length", str(len(chunk)))
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(payload)}")
            self.end_headers()
            self.wfile.write(chunk)

    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{httpd.server_port}/data.zip", payload
    httpd.shutdown()


def test_extracts_first_parts_only(server, tmp_path):
    url, payload = server
    paths = download_parts(tmp_path, parts=2, url=url)
    assert [p.name for p in paths] == ["part_0.csv", "part_1.csv"]
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        for p in paths:
            assert p.read_bytes() == zf.read(f"Small LCL Data/{p.name}")
    assert not (tmp_path / "part_2.csv").exists()
    # Second call is a no-op for files already present with the right size.
    assert download_parts(tmp_path, parts=2, url=url) == paths
