import socket, asyncio, json

PORT = 8765

async def advertise_and_send(data: bytes):
    print("[S4 placeholder] advertising via mDNS on port", PORT)
    print("data to send:", data.hex())
    await asyncio.sleep(30)

async def scan_and_receive():
    print("[S4 placeholder] scanning via mDNS on port", PORT)
    await asyncio.sleep(5)