"""The Blue Current integration."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import datetime
import random
from typing import Any
import zoneinfo

from bluecurrent_api import Client
from bluecurrent_api.exceptions import (
    BlueCurrentException,
    InvalidApiToken,
    RequestLimitReached,
    WebsocketError,
)
import voluptuous as vol

from homeassistant.components.matter.helpers import get_matter
from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.const import CONF_API_TOKEN, CONF_DEVICE_ID, Platform
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import (
    ConfigEntryAuthFailed,
    ConfigEntryNotReady,
    ServiceValidationError,
)
from homeassistant.helpers import config_validation as cv, device_registry as dr
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.helpers.typing import ConfigType

from .const import (
    BCU_APP,
    CHARGEPOINT_SETTINGS,
    CHARGEPOINT_STATUS,
    CHARGING_CARD_ID,
    DOMAIN,
    EVSE_ID,
    LOGGER,
    PLUG_AND_CHARGE,
    SERVICE_START_CHARGE_SESSION,
    VALUE,
)

type BlueCurrentConfigEntry = ConfigEntry[Connector]

PLATFORMS = [Platform.BUTTON, Platform.SENSOR, Platform.SWITCH]
CHARGE_POINTS = "CHARGE_POINTS"
CHARGE_CARDS = "CHARGE_CARDS"
DATA = "data"
DELAY = 5

GRID = "GRID"
OBJECT = "object"
VALUE_TYPES = [CHARGEPOINT_STATUS, CHARGEPOINT_SETTINGS]

CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)

SERVICE_START_CHARGE_SESSION_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_DEVICE_ID): cv.string,
        # When no charging card is provided, use no charging card (BCU_APP = no charging card).
        vol.Optional(CHARGING_CARD_ID, default=BCU_APP): cv.string,
    }
)


async def async_setup_entry(
    hass: HomeAssistant, config_entry: BlueCurrentConfigEntry
) -> bool:
    """Set up Blue Current as a config entry."""
    client = Client()
    connector = Connector(hass, config_entry, client)

    if config_entry.data.get("ignore", False):
        LOGGER.warning("Ignoring real data, dummy data mode active")
        await connector.setup_matter_integration()
        config_entry.async_create_background_task(
            hass, connector.run_task(), "blue_current-websocket"
        )
        config_entry.runtime_data = connector
        await hass.config_entries.async_forward_entry_setups(config_entry, PLATFORMS)
        return True

    api_token = config_entry.data[CONF_API_TOKEN]

    try:
        await client.validate_api_token(api_token)
    except InvalidApiToken as err:
        raise ConfigEntryAuthFailed("Invalid API token.") from err
    except BlueCurrentException as err:
        raise ConfigEntryNotReady from err
    config_entry.async_create_background_task(
        hass, connector.run_task(), "blue_current-websocket"
    )

    await client.wait_for_charge_points()
    config_entry.runtime_data = connector
    await hass.config_entries.async_forward_entry_setups(config_entry, PLATFORMS)

    return True


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up Blue Current."""

    async def start_charge_session(service_call: ServiceCall) -> None:
        """Start a charge session with the provided device and charge card ID."""
        # When no charge card is provided, use the default charge card set in the config flow.
        charging_card_id = service_call.data[CHARGING_CARD_ID]
        device_id = service_call.data[CONF_DEVICE_ID]

        # Get the device based on the given device ID.
        device = dr.async_get(hass).devices.get(device_id)

        if device is None:
            raise ServiceValidationError(
                translation_domain=DOMAIN, translation_key="invalid_device_id"
            )

        config_entry = hass.config_entries.async_get_entry(
            list(device.config_entries)[0]
        )

        if config_entry is None or config_entry.state is not ConfigEntryState.LOADED:
            raise ServiceValidationError(
                translation_domain=DOMAIN, translation_key="config_entry_not_loaded"
            )

        connector = config_entry.runtime_data

        # Get the evse_id from the identifier of the device.
        evse_id = next(
            identifier[1]
            for identifier in device.identifiers
            if identifier[0] == DOMAIN
        )

        await connector.client.start_session(evse_id, charging_card_id)

    hass.services.async_register(
        DOMAIN,
        SERVICE_START_CHARGE_SESSION,
        start_charge_session,
        SERVICE_START_CHARGE_SESSION_SCHEMA,
    )

    return True


async def async_unload_entry(
    hass: HomeAssistant, config_entry: BlueCurrentConfigEntry
) -> bool:
    """Unload the Blue Current config entry."""

    return await hass.config_entries.async_unload_platforms(config_entry, PLATFORMS)


class Connector:
    """Define a class that connects to the Blue Current websocket API."""

    def __init__(
        self, hass: HomeAssistant, config: BlueCurrentConfigEntry, client: Client
    ) -> None:
        """Initialize."""
        self.config = config
        self.hass = hass
        self.client = client
        self.charge_points: dict[str, dict] = {}
        self.grid: dict[str, Any] = {}
        self.charge_cards: dict[str, dict[str, Any]] = {}
        self.dummy_data = config.data.get("ignore", False)

    async def on_data(self, message: dict) -> None:
        """Handle received data."""

        object_name: str = message[OBJECT]

        if self.dummy_data:
            LOGGER.info(message)

        # gets charge point ids
        if object_name == CHARGE_POINTS:
            charge_points_data: list = message[DATA]
            await self.handle_charge_point_data(charge_points_data)

        # gets charge point key / values
        elif object_name in VALUE_TYPES:
            value_data: dict = message[DATA]
            evse_id = value_data.pop(EVSE_ID)
            self.update_charge_point(evse_id, object_name, value_data)

        # gets grid key / values
        elif GRID in object_name:
            data: dict = message[DATA]
            self.grid = data
            self.dispatch_grid_update_signal()

    async def handle_charge_point_data(self, charge_points_data: list) -> None:
        """Handle incoming chargepoint data."""
        await asyncio.gather(
            *(
                self.handle_charge_point(entry[EVSE_ID], entry)
                for entry in charge_points_data
            ),
            self.client.get_grid_status(charge_points_data[0][EVSE_ID]),
        )

    async def handle_charge_point(
        self, evse_id: str, charge_point: dict[str, Any]
    ) -> None:
        """Add the chargepoint and request their data."""
        self.add_charge_point(evse_id, charge_point)
        await self.client.get_status(evse_id)

    def add_charge_point(self, evse_id: str, charge_point: dict[str, Any]) -> None:
        """Add a charge point to charge_points."""
        self.charge_points[evse_id] = charge_point

    def update_charge_point(self, evse_id: str, update_type: str, data: dict) -> None:
        """Update the charge point data."""
        charge_point = self.charge_points[evse_id]
        if update_type == CHARGEPOINT_SETTINGS:
            # Update the plug and charge object. The library parses this object to a bool instead of an object.
            plug_and_charge = charge_point.get(PLUG_AND_CHARGE)
            if plug_and_charge is not None:
                plug_and_charge[VALUE] = data[PLUG_AND_CHARGE]

            # Remove the plug and charge object from the data list before updating.
            del data[PLUG_AND_CHARGE]

        charge_point.update(data)

        self.dispatch_charge_point_update_signal(evse_id)

    def dispatch_charge_point_update_signal(self, evse_id: str) -> None:
        """Dispatch a charge point update signal."""
        async_dispatcher_send(self.hass, f"{DOMAIN}_charge_point_update_{evse_id}")

    def dispatch_grid_update_signal(self) -> None:
        """Dispatch a grid update signal."""
        async_dispatcher_send(self.hass, f"{DOMAIN}_grid_update")

    async def on_open(self) -> None:
        """Fetch data when connection is established."""
        await self.client.get_charge_points()

    async def generate_dummy_data(self) -> dict:
        """Generate dummy data for testing purposes."""
        evse_id = "EVSE1234"

        # First time: add charge point with initial data
        if evse_id not in self.charge_points:
            dummy_charge_point = {
                EVSE_ID: evse_id,
                "name": "Dummy Charge Point",
                "model_type": "BC-Wallbox-22",
                "activity": "available",
                "vehicle_status": "standby",
                "actual_v1": 230.0,
                "actual_v2": 230.0,
                "actual_v3": 230.0,
                "avg_voltage": 230.0,
                "actual_p1": 0.0,
                "actual_p2": 0.0,
                "actual_p3": 0.0,
                "avg_current": 0.0,
                "total_kw": 0.0,
                "actual_kwh": 0.0,
                "total_cost": 0.0,
                "max_usage": 32,
                "smartcharging_max_usage": 16,
                "max_offline": 6,
                "current_left": 32,
                PLUG_AND_CHARGE: {VALUE: True},
                "public_charging": {VALUE: False},
                "start_datetime": datetime.now(zoneinfo.ZoneInfo("Europe/Amsterdam")),
                "stop_datetime": None,
                "offline_since": None,
                "smart_charging": {VALUE: True},
                "delayed_charging": {VALUE: False},
                "price_based_charging": {VALUE: False},
                "grid_actual_p1": 0.0,
                "grid_actual_p2": 0.0,
                "grid_actual_p3": 0.0,
                "grid_avg_current": 0.0,
                "grid_max_current": 32.0,
            }
            self.add_charge_point(evse_id, dummy_charge_point)

        data = {}
        data[OBJECT] = random.choice([CHARGEPOINT_STATUS, CHARGEPOINT_SETTINGS])
        object_name: str = data[OBJECT]

        if object_name == CHARGEPOINT_STATUS:
            # Generate realistic status data
            is_charging = random.choice([True, False])
            current_per_phase = random.uniform(0, 16) if is_charging else 0
            voltage_per_phase = random.uniform(225, 235)

            data[DATA] = {
                EVSE_ID: evse_id,
                "activity": "charging" if is_charging else "available",
                "vehicle_status": "ready" if is_charging else "standby",
                "actual_v1": voltage_per_phase + random.uniform(-2, 2),
                "actual_v2": voltage_per_phase + random.uniform(-2, 2),
                "actual_v3": voltage_per_phase + random.uniform(-2, 2),
                "avg_voltage": voltage_per_phase,
                "actual_p1": current_per_phase + random.uniform(-1, 1),
                "actual_p2": current_per_phase + random.uniform(-1, 1),
                "actual_p3": current_per_phase + random.uniform(-1, 1),
                "avg_current": current_per_phase,
                "total_kw": voltage_per_phase * current_per_phase * 3 / 1000,
                "actual_kwh": self.charge_points[evse_id].get("actual_kwh", 0)
                + (0.1 if is_charging else 0),
                "total_cost": self.charge_points[evse_id].get("total_cost", 0)
                + (0.025 if is_charging else 0),
            }
        elif object_name == CHARGEPOINT_SETTINGS:
            data[DATA] = {
                EVSE_ID: evse_id,
                PLUG_AND_CHARGE: {VALUE: random.choice([True, False])},
                "max_usage": random.randint(16, 32),
                "smartcharging_max_usage": random.randint(6, 16),
                "max_offline": random.randint(6, 10),
                "current_left": random.randint(16, 32),
                "smart_charging": {VALUE: random.choice([True, False])},
                "delayed_charging": {VALUE: random.choice([True, False])},
                "price_based_charging": {VALUE: random.choice([True, False])},
            }

        return data

    async def run_task(self) -> None:
        """Start the receive loop."""
        try:
            while True:
                if self.dummy_data:
                    message = await self.generate_dummy_data()
                    LOGGER.info(message)
                    if message:
                        await self.on_data(message)
                    await asyncio.sleep(10)
                    continue
                try:
                    await self.client.connect(self.on_data, self.on_open)
                except RequestLimitReached:
                    LOGGER.warning(
                        "Request limit reached. reconnecting at 00:00 (Europe/Amsterdam)"
                    )
                    delay = self.client.get_next_reset_delta().seconds
                except WebsocketError:
                    LOGGER.debug("Disconnected, retrying in background")
                    delay = DELAY

                self._on_disconnect()
                await asyncio.sleep(delay)
        finally:
            await self._disconnect()

    def _on_disconnect(self) -> None:
        """Dispatch signals to update entity states."""
        for evse_id in self.charge_points:
            self.dispatch_charge_point_update_signal(evse_id)
        self.dispatch_grid_update_signal()

    async def _disconnect(self) -> None:
        """Disconnect from the websocket."""
        with suppress(WebsocketError):
            await self.client.disconnect()
            self._on_disconnect()

    @property
    def connected(self) -> bool:
        """Returns the connection status."""
        return self.client.is_connected() or self.dummy_data

    async def setup_matter_integration(self):
        """Link Blue Current devices with Matter devices."""
        matter = get_matter(self.hass)
        # Find Matter devices that correspond to Blue Current chargers
        entities = matter.discovered_entities
        for ent in entities:
            LOGGER.info("Discovered Matter entity: %s", ent)
