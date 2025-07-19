import tempfile, pathlib, hashlib
from dittofs.chunker import split, join

def test_roundtrip():
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(b"A" * 1024 * 1024)        # 1 MB of 'A'
        src = pathlib.Path(f.name)

    # split
    hashes = list(split(src))
    assert len(hashes) == 16               # 1 MB / 64 KB = 16 chunks

    # re-assemble
    dst = src.with_suffix(".restored")
    join(hashes, dst)

    # check identical
    assert hashlib.sha256(src.read_bytes()).digest() == hashlib.sha256(dst.read_bytes()).digest()
    print("roundtrip ok")