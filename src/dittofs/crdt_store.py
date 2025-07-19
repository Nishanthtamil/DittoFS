import pathlib, pycrdt as Y, json, uuid

DOC_PATH = pathlib.Path.home() / ".dittofs" / "doc.yrs"

class CRDTStore:
    def __init__(self):
        self.doc = Y.Doc()
        # For a new doc, CREATE the map using dict-style assignment
        self.map = self.doc['files'] = Y.Map()

    def add_file(self, path: pathlib.Path, hashes):
        key = str(path.resolve())
        self.map[key] = {"hashes": hashes, "mtime": path.stat().st_mtime}

    def get_file(self, path):
        return self.map.get(str(path.resolve()))

    def merge_remote(self, remote_doc_bytes):
        Y.apply_update(self.doc, remote_doc_bytes)

    def save(self):
        DOC_PATH.parent.mkdir(parents=True, exist_ok=True)
        DOC_PATH.write_bytes(self.doc.get_update())

    @classmethod
    def load(cls):
        store = cls()
        if DOC_PATH.exists():
            store.doc = Y.Doc()
            store.doc.apply_update(DOC_PATH.read_bytes())
            # For a loaded doc, GET the existing map
            store.map = store.doc.get("files",type=Y.Map)
        return store