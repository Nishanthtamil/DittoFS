import trio, sys
from .hello_dittofs import HelloFS 
import pyfuse3

async def main_async():
    if len(sys.argv) != 2:
        print("Usage: dittofs <mountpoint>")
        sys.exit(1)
    mountpoint = sys.argv[1]
    operations = HelloFS()
    pyfuse3.init(operations, mountpoint, [])
    try:
        await pyfuse3.main()
    finally:
        pyfuse3.close(unmount=True)
def main():
    trio.run(main_async)
if __name__ == "__main__":
    main()