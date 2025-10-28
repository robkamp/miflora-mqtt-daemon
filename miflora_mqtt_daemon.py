import asyncio
import argparse
import struct
import logging
import json
import time
from datetime import datetime, timezone
from bleak import BleakClient, BleakError
import paho.mqtt.client as mqtt

# Mi Flora GATT Characteristic UUIDs
UUID_HISTORY_CONTROL = "00001a10-0000-1000-8000-00805f9b34fb"
UUID_HISTORY_DATA = "00001a11-0000-1000-8000-00805f9b34fb"
UUID_REALTIME_DATA_MODE = "00001a00-0000-1000-8000-00805f9b34fb"
UUID_SENSOR_DATA = "00001a01-0000-1000-8000-00805f9b34fb"
UUID_STATUS = "00001a02-0000-1000-8000-00805f9b34fb"

# Standard Bluetooth Service/Characteristic UUIDs
UUID_BATTERY_SERVICE = "0000180f-0000-1000-8000-00805f9b34fb"
UUID_BATTERY_LEVEL = "00002a19-0000-1000-8000-00805f9b34fb"

# Home Assistant MQTT Discovery Config
BASE_TOPIC = "homeassistant"
POLL_INTERVAL_SECONDS = 900 # 15 minutes

# --- Logger Setup ---
log = logging.getLogger('miflora_daemon')
log.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
log.addHandler(handler)

# --- Global Args ---
# We use a simple class to hold args so they are globally accessible
class GlobalArgs:
    MQTT_BROKER_IP = "127.0.0.1"
    MQTT_PORT = 1883
    MQTT_USER = None
    MQTT_PASS = None
    DEVICES = []
    FETCH_HISTORY = False
    mqtt_client = None # To hold the client instance

args = GlobalArgs()

class DeviceState:
    """Holds the runtime state for a single device."""
    def __init__(self, mac, plant_name):
        self.mac = mac
        self.plant_name = plant_name
        self.device_id = f"miflora_{mac.replace(':', '').lower()}"
        self.discovery_published = False
        self.last_history_index = 0 # Index of the last history entry we've seen

def setup_mqtt_client():
    """Configures and connects the MQTT client."""
    global args
    client = mqtt.Client(client_id="miflora_mqtt_daemon_service")
    if args.MQTT_USER and args.MQTT_PASS:
        client.username_pw_set(args.MQTT_USER, args.MQTT_PASS)
    try:
        client.connect(args.MQTT_BROKER_IP, args.MQTT_PORT, 60)
        client.loop_start() # Start background thread
        log.info(f"Connected to MQTT Broker at {args.MQTT_BROKER_IP}")
        args.mqtt_client = client # Store client in global args
        return client
    except Exception as e:
        log.error(f"Failed to connect to MQTT broker: {e}")
        return None

def publish_ha_discovery(mqtt_client, plant_name, device_id, state_topic):
    """Publishes the MQTT discovery messages for Home Assistant."""
    device_info = {
        "identifiers": [device_id],
        "name": plant_name,
        "model": "Mi Flora Plant Sensor",
        "manufacturer": "Xiaomi"
    }

    sensors = {
        "temperature": {"name": "Temperature", "unit": "°C", "class": "temperature", "icon": "mdi:thermometer"},
        "illuminance": {"name": "Illuminance", "unit": "lx", "class": "illuminance", "icon": "mdi:white-balance-sunny"},
        "moisture": {"name": "Moisture", "unit": "%", "class": "humidity", "icon": "mdi:water-percent"},
        "conductivity": {"name": "Fertility", "unit": "µS/cm", "class": "volatile_organic_compounds", "icon": "mdi:leaf"},
        "battery": {"name": "Battery", "unit": "%", "class": "battery", "icon": "mdi:battery"},
        "firmware": {"name": "Firmware", "class": "None", "icon": "mdi:chip"},
    }

    for key, config in sensors.items():
        discovery_topic = f"{BASE_TOPIC}/sensor/{device_id}/{key}/config"
        payload = {
            "name": f"{plant_name} {config['name']}",
            "unique_id": f"{device_id}_{key}",
            "state_topic": state_topic,
            "value_template": f"{{{{ value_json.{key} }}}}",
            "unit_of_measurement": config.get("unit"),
            "device_class": config.get("class"),
            "icon": config.get("icon"),
            "device": device_info,
            "availability_topic": f"{BASE_TOPIC}/sensor/{device_id}/status",
            "payload_available": "online",
            "payload_not_available": "offline"
        }
        # Remove keys with None values
        payload = {k: v for k, v in payload.items() if v is not None}
        mqtt_client.publish(discovery_topic, json.dumps(payload), retain=True)

    log.info(f"[{plant_name}] Discovery messages published.")

def parse_realtime_data(sensor_data, status_data, battery_data):
    """Parses the raw bytes from the sensor into a friendly dictionary."""
    
    # Parse main sensor data (UUID 1a01)
    temperature = struct.unpack('<h', sensor_data[0:2])[0] / 10.0
    illuminance = struct.unpack('<I', sensor_data[3:7])[0]
    moisture = sensor_data[7]
    fertility = struct.unpack('<H', sensor_data[8:10])[0]

    # Parse status data (UUID 1a02)
    firmware = status_data[2:].decode('ascii').rstrip('\x00') # Clean up trailing nulls

    # Determine battery
    battery = None
    if battery_data is not None:
        # New method: Read from standard Battery Service
        battery = battery_data[0]
    else:
        # Fallback method: Parse from status data
        battery_val = status_data[0]
        if battery_val <= 100:
            battery = battery_val
        else:
            log.warning(f"Got invalid fallback battery value {battery_val}. Reporting as None.")

    return {
        "temperature": temperature,
        "illuminance": illuminance,
        "moisture": moisture,
        "conductivity": fertility,
        "battery": battery,
        "firmware": firmware
    }

def parse_history_entry(data):
    """Parses a 16-byte historical data entry."""
    entry_time_device = struct.unpack('<I', data[0:4])[0] # Seconds since device boot
    temperature = struct.unpack('<h', data[4:6])[0] / 10.0
    illuminance = struct.unpack('<I', data[7:11])[0]
    moisture = data[11]
    fertility = struct.unpack('<H', data[12:14])[0]
    
    return {
        "device_time": entry_time_device,
        "temperature": temperature,
        "illuminance": illuminance,
        "moisture": moisture,
        "conductivity": fertility
    }

async def read_realtime_data(client, plant_name):
    """Reads and parses the real-time sensor data."""
    try:
        log.info(f"[{plant_name}] Enabling real-time data...")
        await client.write_gatt_char(UUID_REALTIME_DATA_MODE, b'\xa0\x1f', response=True)
        
        log.info(f"[{plant_name}] Waiting 1s for sensor to stabilize...")
        await asyncio.sleep(1.0)
        
        log.info(f"[{plant_name}] Reading sensor data...")
        sensor_data = await client.read_gatt_char(UUID_SENSOR_DATA)
        
        log.info(f"[{plant_name}] Reading device status (firmware)...")
        status_data = await client.read_gatt_char(UUID_STATUS)
        
        battery_data = None
        try:
            log.info(f"[{plant_name}] Reading battery from standard service...")
            battery_data = await client.read_gatt_char(UUID_BATTERY_LEVEL)
            log.info(f"[{plant_name}] Read battery from standard service.")
        except Exception:
            log.warning(f"[{plant_name}] Could not read from standard Battery Service. Will use fallback.")

        return parse_realtime_data(sensor_data, status_data, battery_data)

    except Exception as e:
        log.error(f"[{plant_name}] Error reading real-time data: {e}")
        return None

async def read_historical_data(client, plant_name, device_state):
    """Reads and parses new historical data entries from the sensor."""
    try:
        log.info(f"[{plant_name}] Reading history control data...")
        # Read 0x1A10 to get entry count and device time
        control_data = await client.read_gatt_char(UUID_HISTORY_CONTROL)

        # Check data length before unpacking
        # Some sensor versions return data < 6 bytes (e.g., just 2 bytes for total_entries)
        # and do not support the time sync.
        if len(control_data) < 6:
            log.warning(f"[{plant_name}] History control data is too short ({len(control_data)} bytes). "
                        "This device model may not support history time sync. Aborting history read.")
            return [] # Return empty list

        total_entries = struct.unpack('<H', control_data[0:2])[0]
        device_time_now = struct.unpack('<I', control_data[2:6])[0] # Seconds since boot
        server_time_now = int(time.time())

        # Calculate offset between server time and device time
        # We can use this to give a "real" timestamp to each history entry
        time_offset = server_time_now - device_time_now

        if total_entries == 0:
            log.info(f"[{plant_name}] No historical entries found on device.")
            return []
        
        if device_state.last_history_index >= total_entries:
            log.info(f"[{plant_name}] No new historical entries to fetch.")
            return []

        log.info(f"[{plant_name}] Device has {total_entries} total entries. Fetching from index {device_state.last_history_index}.")
        
        new_entries = []
        for i in range(device_state.last_history_index, total_entries):
            try:
                # Tell the device which entry we want
                await client.write_gatt_char(UUID_HISTORY_CONTROL, struct.pack('<H', i), response=True)
                
                # Read the 16-byte entry from the data characteristic
                entry_data = await client.read_gatt_char(UUID_HISTORY_DATA)
                
                if len(entry_data) != 16:
                    log.warning(f"[{plant_name}] Invalid history entry data length: {len(entry_data)}. Skipping.")
                    continue

                # Parse the entry
                parsed_entry = parse_history_entry(entry_data)
                
                # Add the "real" timestamp
                real_timestamp_utc = parsed_entry['device_time'] + time_offset
                parsed_entry['timestamp_utc'] = datetime.fromtimestamp(real_timestamp_utc, tz=timezone.utc).isoformat()
                
                new_entries.append(parsed_entry)
            except Exception as e:
                log.error(f"[{plant_name}] Failed to fetch history entry {i}: {e}")
                # Stop trying on an error to avoid spamming
                break
        
        # Update the state to remember the last index we fetched
        device_state.last_history_index = total_entries
        return new_entries

    except Exception as e:
        log.error(f"[{plant_name}] Error reading historical data: {e}")
        return []

def publish_historical_data(mqtt_client, device_id, history_data):
    """Publishes a list of historical entries to MQTT."""
    if not history_data:
        return
        
    history_topic = f"{BASE_TOPIC}/sensor/{device_id}/history"
    payload = json.dumps(history_data)
    mqtt_client.publish(history_topic, payload)
    log.info(f"[{device_id}] Published {len(history_data)} new historical entries to {history_topic}")

async def device_loop(mqtt_client, device_state, ble_lock):
    """The main monitoring loop for a single device."""
    mac = device_state.mac
    plant_name = device_state.plant_name
    device_id = device_state.device_id
    
    state_topic = f"{BASE_TOPIC}/sensor/{device_id}/state"
    availability_topic = f"{BASE_TOPIC}/sensor/{device_id}/status"

    while True:
        realtime_data = None
        history_data = []
        
        log.info(f"[{plant_name}] Waiting to acquire Bluetooth lock...")
        async with ble_lock:
            log.info(f"[{plant_name}] Lock acquired. Attempting to connect to {mac}...")
            try:
                # Add a 15-second timeout to the connection attempt
                async with BleakClient(mac, timeout=15.0) as client:
                    if not client.is_connected:
                        log.warning(f"[{plant_name}] Failed to connect to {mac}")
                        continue # Skip to next loop iteration
                    
                    log.info(f"[{plant_name}] Connected.")
                    
                    # 1. Read Real-Time Data
                    realtime_data = await read_realtime_data(client, plant_name)
                    
                    # 2. Read Historical Data (if enabled AND real-time read was successful)
                    if args.FETCH_HISTORY and realtime_data:
                        history_data = await read_historical_data(client, plant_name, device_state)

                    log.info(f"[{plant_name}] Disconnecting from device.")
                
                # Short delay after disconnect to help BLE stack
                await asyncio.sleep(2.0)

            except BleakError as e:
                log.error(f"[{plant_name}] BleakError: {e}")
            
            # Log the actual exception string
            except Exception as e:
                log.error(f"[{plant_name}] An unexpected error occurred: {e}")
            
        log.info(f"[{plant_name}] Lock released.")

        # --- MQTT Publishing (outside the BLE lock) ---
        if realtime_data:
            log.info(f"[{plant_name}] Successfully read data: {realtime_data}")
            
            # Publish discovery messages (once)
            if not device_state.discovery_published:
                publish_ha_discovery(mqtt_client, plant_name, device_id, state_topic)
                device_state.discovery_published = True

            # Publish availability and sensor data
            mqtt_client.publish(availability_topic, "online", retain=True)
            mqtt_client.publish(state_topic, json.dumps(realtime_data))
            log.info(f"[{plant_name}] Sensor data published to MQTT.")
            
            # Publish history (if any)
            if history_data:
                publish_historical_data(mqtt_client, device_id, history_data)
        
        else:
            log.warning(f"[{plant_name}] Failed to read sensor data. Will retry next cycle.")
            mqtt_client.publish(availability_topic, "offline", retain=True)

        log.info(f"[{plant_name}] Sleeping for {POLL_INTERVAL_SECONDS} seconds...")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)

async def main():
    """The main daemon entry point."""
    global args
    parser = argparse.ArgumentParser(description="Mi Flora MQTT Daemon for OpenHab/Home Assistant")
    
    # Use 'append' action to allow multiple devices
    parser.add_argument(
        '--device',
        action='append',
        nargs=2,
        metavar=('MAC', 'NAME'),
        help="MAC address and friendly name of a Mi Flora device. (Can be used multiple times)"
    )
    parser.add_argument('--broker', required=True, help="IP address of the MQTT broker")
    parser.add_argument('--port', type=int, default=1883, help="Port of the MQTT broker")
    parser.add_argument('--user', help="MQTT username")
    parser.add_argument('--password', help="MQTT password")
    parser.add_argument('--interval', type=int, default=3600, help="Polling interval in seconds (default: 3600)")
    parser.add_argument('--history', action='store_true', help="Enable fetching of historical data")

    # Parse args from command line into our global object
    parsed_args = parser.parse_args()
    
    if not parsed_args.device:
        log.error("Error: At least one --device argument is required.")
        parser.print_help()
        return

    args.MQTT_BROKER_IP = parsed_args.broker
    args.MQTT_PORT = parsed_args.port
    args.MQTT_USER = parsed_args.user
    args.MQTT_PASS = parsed_args.password
    args.DEVICES = parsed_args.device
    args.FETCH_HISTORY = parsed_args.history
    
    global POLL_INTERVAL_SECONDS
    POLL_INTERVAL_SECONDS = parsed_args.interval

    mqtt_client = setup_mqtt_client()
    if not mqtt_client:
        log.error("Exiting. Could not configure MQTT client.")
        return

    # Create a lock to ensure only one BLE operation happens at a time
    ble_lock = asyncio.Lock()

    # Create a list of tasks to run concurrently
    tasks = []
    for (mac, plant_name) in args.DEVICES:
        log.info(f"Setting up monitoring for '{plant_name}' at {mac}")
        state = DeviceState(mac, plant_name)
        tasks.append(device_loop(mqtt_client, state, ble_lock))
    
    # Run all device monitoring loops forever
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutting down daemon...")
    finally:
        if args.mqtt_client:
            log.info("Stopping MQTT client loop.")
            args.mqtt_client.loop_stop()
            args.mqtt_client.disconnect()
        log.info("Shutdown complete.")


