import trio, sys, argparse, pathlib
from .hello_dittofs import HelloFS   
from .chunker import split, join  
import pyfuse3  

# FUSE mount
async def main_mount(path):
    operations = HelloFS()
    pyfuse3.init(operations, path, [])
    try:
        await pyfuse3.main()
    finally:
        pyfuse3.close(unmount=True)

# chunk commands
def cli_add(args):
    hashes = list(split(pathlib.Path(args.file)))
    print("chunks:", *hashes)

def cli_get(args):
    hashes = args.hashes.split(",")
    join(hashes, pathlib.Path(args.out))

# Unified CLI builder
def build_cli():
    p = argparse.ArgumentParser(prog="dittofs")
    sub = p.add_subparsers(dest="cmd", required=True)

    # mount
    mount_p = sub.add_parser("mount")
    mount_p.add_argument("mountpoint")
    mount_p.set_defaults(func=lambda a: trio.run(main_mount, a.mountpoint))

    # add
    add_p = sub.add_parser("add")
    add_p.add_argument("file")
    add_p.set_defaults(func=cli_add)

    # get
    get_p = sub.add_parser("get")
    get_p.add_argument("hashes")
    get_p.add_argument("out")
    get_p.set_defaults(func=cli_get)

    return p

def main():
    parser = build_cli()
    args = parser.parse_args()
    if hasattr(args, 'func'):
        args.func(args)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()