"""Run a short RGBIC chase on the H605C and restore its camera settings."""

import asyncio
from contextlib import asynccontextmanager
from functools import reduce
from operator import xor

from bleak.exc import BleakError
from bleak_retry_connector import BleakClientWithServiceCache, establish_connection
from homeassistant.components import bluetooth
from homeassistant.core import SupportsResponse
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import config_validation as cv
import voluptuous as vol

DOMAIN = "govee_tv_chase"
CONFIG_SCHEMA = cv.empty_config_schema(DOMAIN)
SERVICE_SCHEMA = vol.Schema({
    vol.Required("team"): vol.In(("broncos", "nuggets")),
    vol.Required("seconds"): vol.All(vol.Coerce(float), vol.Range(min=2, max=30)),
})

WRITE = "00010203-0405-0607-0809-0a0b0c0d2b11"
NOTIFY = "00010203-0405-0607-0809-0a0b0c0d2b10"
SEGMENTS = 14
TAIL = 4
POWER = 0x01
MODE = 0x05
VIDEO = 0x00
SEGMENT_COLOR = 0x15
PALETTES = {
    "broncos": ((251, 79, 20), (0, 34, 68)),
    "nuggets": ((254, 197, 36), (14, 34, 64)),
}
CONNECTION_ERRORS = (BleakError, TimeoutError, OSError)


def frame(payload):
    """Encode the H605C's fixed-length packet and XOR checksum."""
    body = bytes(payload).ljust(19, b"\0")
    return body + bytes([reduce(xor, body, 0)])


def segment_frame(color, mask):
    """Set RGB on the selected logical segments without changing brightness."""
    return frame(bytes((0x33, MODE, SEGMENT_COLOR, 1, *color)) + bytes(5)
                 + mask.to_bytes(2, "little"))


def video_frame(status):
    """Reuse the reported region, style, saturation, sound, and edge levels."""
    return frame(bytes((0x33, MODE, VIDEO)) + status[3:8] + b"\0" + status[9:13])


class Backlight:
    """Send H605C commands and match status notifications to queries."""

    def __init__(self, client):
        self.client = client
        self.pending = None
        characteristic = client.services.get_characteristic(WRITE)
        if characteristic is None:
            raise BleakError("Govee write characteristic is missing")
        self.write_response = "write" in characteristic.properties

    def receive(self, _characteristic, data):
        if self.pending is None:
            return
        opcode, reply = self.pending
        if (not reply.done() and len(data) == 20 and data[:2] == bytes((0xAA, opcode))
                and not reduce(xor, data, 0)):
            reply.set_result(bytes(data))

    async def send(self, packet):
        async with asyncio.timeout(2):
            await self.client.write_gatt_char(WRITE, packet, response=self.write_response)

    async def read(self, opcode):
        reply = asyncio.get_running_loop().create_future()
        self.pending = (opcode, reply)
        try:
            async with asyncio.timeout(2):
                await self.send(frame((0xAA, opcode)))
                return await reply
        finally:
            self.pending = None
            reply.cancel()


@asynccontextmanager
async def connect(hass):
    devices = [info.device for info in bluetooth.async_discovered_service_info(hass, connectable=True)
               if info.name.startswith("Govee_H605C_")]
    if len(devices) != 1:
        raise HomeAssistantError("Expected exactly one reachable Govee H605C backlight")
    async with asyncio.timeout(12):
        client = await establish_connection(
            BleakClientWithServiceCache, devices[0], "Govee TV Backlight", max_attempts=2
        )
    try:
        backlight = Backlight(client)
        await client.start_notify(NOTIFY, backlight.receive)
        yield backlight
    finally:
        await client.disconnect()


async def restore(backlight, saved):
    """Verify that the camera mode and its original settings are back."""
    was_off = (await backlight.read(POWER))[2] == 0
    await backlight.send(video_frame(saved))
    if was_off:
        await backlight.send(frame((0x33, POWER, 0)))
    restored = await backlight.read(MODE)
    if restored[2] != VIDEO or video_frame(restored) != video_frame(saved):
        raise BleakError("Backlight did not report its original TV-sync settings")


async def chase(backlight, palette, seconds):
    """Move a four-segment highlight one segment at a time around the TV."""
    highlight, background = palette
    loop = asyncio.get_running_loop()
    end = loop.time() + seconds
    next_power_check = loop.time() + 2
    await backlight.send(segment_frame(background, (1 << SEGMENTS) - 1))
    await backlight.send(segment_frame(highlight, (1 << TAIL) - 1))
    if (await backlight.read(MODE))[2] != SEGMENT_COLOR:
        raise BleakError("Backlight did not enter segment color mode")

    steps = 0
    while loop.time() < end:
        await asyncio.sleep(min(0.16, end - loop.time()))
        if loop.time() >= end:
            break
        if loop.time() >= next_power_check:
            if (await backlight.read(POWER))[2] == 0:
                break
            next_power_check = loop.time() + 2
        await backlight.send(segment_frame(background, 1 << (steps % SEGMENTS)))
        await backlight.send(segment_frame(highlight, 1 << ((steps + TAIL) % SEGMENTS)))
        steps += 1
    return steps


async def async_setup(hass, _config):
    """Register the shared action and prevent overlapping celebrations."""
    lock = asyncio.Lock()

    async def run(call):
        if lock.locked():
            return {"result": "already_running"}
        async with lock:
            try:
                return await run_chase(hass, call.data["team"], call.data["seconds"])
            except CONNECTION_ERRORS as error:
                raise HomeAssistantError(f"Govee TV backlight: {error}") from error

    hass.services.async_register(
        DOMAIN, "celebrate", run, schema=SERVICE_SCHEMA, supports_response=SupportsResponse.OPTIONAL
    )
    return True


async def run_chase(hass, team, seconds):
    # Authorize the brief override only from verified camera sync mode.
    # Formats: github.com/teh-hippo/ha-govee-led-ble/tree/main/tools/ble/kaitai/speculative
    recovery = None
    try:
        async with connect(hass) as backlight:
            if (await backlight.read(POWER))[2] != 1:
                return {"result": "backlight_off"}
            saved = await backlight.read(MODE)
            if saved[2] != VIDEO:
                return {"result": "tv_sync_inactive", "mode": saved[2]}
            recovery = saved
            try:
                steps = await chase(backlight, PALETTES[team], seconds)
            finally:
                try:
                    await restore(backlight, saved)
                    recovery = None
                except CONNECTION_ERRORS:
                    pass  # Reconnect below if the original link cannot restore the mode.
    finally:
        if recovery is not None:
            for attempt in range(2):
                try:
                    async with connect(hass) as backlight:
                        await restore(backlight, recovery)
                    break
                except (*CONNECTION_ERRORS, HomeAssistantError) as error:
                    if attempt == 1:
                        raise HomeAssistantError("Could not restore the previous TV sync mode") from error
    return {"result": "tv_sync_restored", "team": team, "steps": steps}
