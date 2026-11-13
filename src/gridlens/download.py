"""Download a real sample of the Low Carbon London smart meter dataset.

The London Datastore publishes the full dataset (167M rows, ~760 MB zipped) as a
zip of 168 CSV parts of one million rows each. The server supports HTTP range
requests, so this module reads the zip central directory remotely and extracts
only the first ``parts`` members (about 4.7 MB compressed each). Stdlib only, so
it runs unchanged on a laptop, in CI or as a Databricks job task.

Source: UK Power Networks, SmartMeter Energy Consumption Data in London
Households, London Datastore, CC BY 4.0.
"""

from __future__ import annotations

import argparse
import io
import struct
import urllib.request
import zipfile
import zlib
from pathlib import Path

LCL_PARTITIONED_URL = (
    "https://data.london.gov.uk/download/vqm0d/04feba67-f1a3-4563-98d0-f3071e3d56d1/"
    "Partitioned%20LCL%20Data.zip"
)

# The datastore's CDN rejects urllib's default User-Agent with 403.
HEADERS = {"User-Agent": "gridlens-downloader/0.1 (+https://github.com/MaheshBhushan/gridlens)"}


class HttpRangeFile(io.RawIOBase):
    """Read-only, seekable file object backed by HTTP range requests."""

    def __init__(self, url: str, timeout: float = 120.0) -> None:
        self.url = url
        self.timeout = timeout
        self.pos = 0
        req = urllib.request.Request(url, method="HEAD", headers=HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            self.size = int(resp.headers["Content-Length"])
            if resp.headers.get("Accept-Ranges", "").lower() != "bytes":
                raise RuntimeError(f"{url} does not support HTTP range requests")

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            self.pos = offset
        elif whence == io.SEEK_CUR:
            self.pos += offset
        elif whence == io.SEEK_END:
            self.pos = self.size + offset
        else:
            raise ValueError(f"bad whence {whence}")
        return self.pos

    def readinto(self, buf) -> int:
        if self.pos >= self.size or len(buf) == 0:
            return 0
        data = _fetch_range(self.url, self.pos, min(self.pos + len(buf), self.size) - 1, self.timeout)
        n = len(data)
        memoryview(buf).cast("B")[:n] = data
        self.pos += n
        return n


def _fetch_range(url: str, start: int, end: int, timeout: float = 300.0) -> bytes:
    if start < 0 or end < start:
        raise ValueError(f"invalid byte range {start}-{end}")
    req = urllib.request.Request(url, headers={**HEADERS, "Range": f"bytes={start}-{end}"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        if resp.status != 206:
            raise RuntimeError(f"expected 206 Partial Content, got {resp.status}")
        data = resp.read()
    if len(data) != end - start + 1:
        raise RuntimeError(f"short range read: {len(data)} of {end - start + 1} bytes")
    return data


def _extract_member(url: str, size: int, info: zipfile.ZipInfo, target: Path) -> None:
    """Fetch one member's local header + compressed bytes in a single range request and inflate."""
    # Local header is 30 bytes + name + extra; the extra field length can differ from the
    # central directory copy, so over-fetch 64 KiB and parse the real lengths.
    end = min(info.header_offset + 30 + 65536 + info.compress_size, size - 1)
    blob = _fetch_range(url, info.header_offset, end)
    if blob[:4] != b"PK\x03\x04":
        raise RuntimeError(f"bad local header for {info.filename}")
    name_len, extra_len = struct.unpack("<HH", blob[26:30])
    start = 30 + name_len + extra_len
    payload = blob[start : start + info.compress_size]
    if info.compress_type == zipfile.ZIP_DEFLATED:
        data = zlib.decompressobj(-zlib.MAX_WBITS).decompress(payload)
    elif info.compress_type == zipfile.ZIP_STORED:
        data = payload
    else:
        raise RuntimeError(f"unsupported compression {info.compress_type} for {info.filename}")
    if len(data) != info.file_size or zlib.crc32(data) != info.CRC:
        raise RuntimeError(f"size/CRC mismatch for {info.filename}")
    # Written in place (no rename: Unity Catalog volumes do not guarantee atomic rename).
    # A partial file from an interrupted run has the wrong size and is fetched again.
    target.write_bytes(data)


def download_parts(out_dir: str | Path, parts: int = 1, url: str = LCL_PARTITIONED_URL) -> list[Path]:
    """Extract the first ``parts`` CSV members of the partitioned LCL zip into ``out_dir``."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    # zipfile only reads the end-of-archive records and the central directory here.
    remote = HttpRangeFile(url)
    with zipfile.ZipFile(remote) as zf:
        members = sorted(
            (i for i in zf.infolist() if i.filename.lower().endswith(".csv")),
            key=lambda i: i.header_offset,
        )
    written: list[Path] = []
    for info in members[:parts]:
        target = out / Path(info.filename).name
        if not (target.exists() and target.stat().st_size == info.file_size):
            _extract_member(url, remote.size, info, target)
        written.append(target)
    return written


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--out", default="data/raw", help="output directory (local path or /Volumes/...)")
    p.add_argument("--parts", type=int, default=1, help="number of 1M-row CSV parts to extract")
    args = p.parse_args(argv)
    for path in download_parts(args.out, args.parts):
        print(f"{path}\t{path.stat().st_size} bytes")


if __name__ == "__main__":
    main()
