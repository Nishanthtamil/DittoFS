import os, pathlib, blake3

CHUNK = 64 * 1024
CHUNK_DIR = pathlib.Path.home() / ".dittofs" / "chunks"
CHUNK_DIR.mkdir(parents=True, exist_ok=True)

def split(path: pathlib.Path):
    with open(path, "rb") as f:
        chunk_index = 0
        while True:
            data = f.read(CHUNK)
            if not data:
                break
            h = blake3.blake3(data).hexdigest()
            (CHUNK_DIR / h).write_bytes(data)
            yield h
            chunk_index += 1

def join(hashes, out_path: pathlib.Path):
    with open(out_path, "wb") as out:
        for h in hashes:
            out.write((CHUNK_DIR / h).read_bytes())