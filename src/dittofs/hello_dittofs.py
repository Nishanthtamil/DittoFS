import asyncio, errno, stat
import pyfuse3, trio,sys

class HelloFS(pyfuse3.Operations):
    def __init__(self):
        super().__init__()
        self.files = {
            1: pyfuse3.EntryAttributes()  # root
        }
        self.files[1].st_mode = stat.S_IFDIR | 0o755
        self.files[1].st_size = 0
        self.files[1].st_ino = 1

        self.files[2] = pyfuse3.EntryAttributes()  # hello.txt
        self.files[2].st_mode = stat.S_IFREG | 0o644
        self.files[2].st_size = 12
        self.files[2].st_ino = 2

    async def getattr(self, inode, ctx):
        return self.files[inode]

    async def lookup(self, parent_inode, name, ctx):
        if parent_inode == 1 and name == b'hello.txt':
            return self.files[2]
        raise pyfuse3.FUSEError(errno.ENOENT)

    async def open(self, inode, flags, ctx):
        if inode != 2:
            raise pyfuse3.FUSEError(errno.ENOENT)
        return pyfuse3.FileInfo(fh=2)

    async def read(self, fh, off, size):
        return b'Hello Ditto\n'[off:off+size]

async def main():
    import sys
    if len(sys.argv) != 2:
        print("Usage: python hello_dittofs.py <mountpoint>")
        sys.exit(1)
    mountpoint = sys.argv[1]
    operations = HelloFS()
    pyfuse3.init(operations, mountpoint, [])
    try:
        await pyfuse3.main()
    finally:
        pyfuse3.close(unmount=True)

if __name__ == '__main__':
    trio.run(main)