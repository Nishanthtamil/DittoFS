import tempfile, pathlib, time
from dittofs.crdt_store import CRDTStore

def test_register_and_retrieve():
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(b"hello")
        f.flush()
        path = pathlib.Path(f.name)

    store = CRDTStore()
    store.add_file(path, ["fake_hash"])
    store.save()

    # reload fresh
    store2 = CRDTStore.load()
    entry = store2.get_file(path)
    assert entry is not None
    assert entry["hashes"] == ["fake_hash"]
    assert abs(entry["mtime"] - path.stat().st_mtime) < 1
    print("CRDT round-trip ok")