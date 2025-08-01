import trio, sys, argparse, pathlib , asyncio
from .hello_dittofs import HelloFS   
from .chunker import split, join  
import pyfuse3  
from .crdt_store import CRDTStore
from .gui import TrayApp
from .ble_peer import serve_forever, fetch_once
from .lan_fallback import LANTransport

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
    store = CRDTStore.load()
    store.add_file(pathlib.Path(args.file), hashes)
    store.save()
    print("registered:", args.file, "chunks:", *hashes)

def cli_get(args):
    hashes = args.hashes.split(",")
    join(hashes, pathlib.Path(args.out))

# for pairing
# from .lan_transport import LANTransport

def cli_pair_ble(args):
    if args.role == "serve":
        LANTransport().serve(b"hello")
    else:
        data = LANTransport().fetch()
        print("Fetched:", data)

# for GUI
def cli_tray(args):
    app = TrayApp()
    app.exec()

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

    # pair
    ble_cmd = sub.add_parser("ble")
    ble_cmd.add_argument("role", choices=["serve", "fetch"])
    ble_cmd.set_defaults(func=lambda a: asyncio.run(cli_pair_ble(a)))
    ble_cmd.set_defaults(func=cli_pair_ble)
    # GUI
    tray_cmd = sub.add_parser("tray")
    tray_cmd.set_defaults(func=cli_tray)
    
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