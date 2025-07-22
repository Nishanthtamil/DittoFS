# src/dittofs/ble_peer.py
import asyncio
from bless import BlessServer
from bleak import BleakClient, BleakScanner

SERVICE_UUID = "12345678-1234-1234-1234-123456789abc"
CHAR_UUID    = "87654321-4321-4321-4321-210987654321"

class BLETransport:
    async def serve(self, payload: bytes):
        server = BlessServer(name="DittoFS")
        await server.add_new_service(SERVICE_UUID)
        # flags are plain strings, no extra enums or permissions
        await server.add_new_characteristic(
            SERVICE_UUID,
            CHAR_UUID,
            ["read", "write"],
            payload
        )
        await server.start()
        print("Advertising on", SERVICE_UUID)
        await asyncio.sleep(120)

    async def discover_and_fetch(self, timeout=10):
        device = await BleakScanner.find_device_by_filter(
            lambda d, ad: SERVICE_UUID in ad.service_uuids, timeout
        )
        if not device:
            return None
        async with BleakClient(device) as client:
            return await client.read_gatt_char(CHAR_UUID)

async def serve_forever(payload):
    await BLETransport().serve(payload)

async def fetch_once():
    return await BLETransport().discover_and_fetch()