import asyncio
from .ble_real import serve_forever as ble_serve, fetch_once as ble_fetch
from .lan_fallback import lan_serve, lan_fetch

async def serve(payload: bytes):
    try:
        await ble_serve(payload)
    except RuntimeError:
        await lan_serve(payload)

async def fetch():
    try:
        return await ble_fetch()
    except RuntimeError:
        return await lan_fetch()