#!/usr/bin/env python3

import os
import sys
import signal
import time
import threading
import socket
import serial
import requests
import base64
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from typing import List, Optional, Dict, Any

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("python-dotenv not installed, using environment variables only")

# ################################
# VARIABLES
SERIAL_PORT                 = os.getenv('SERIAL_PORT') or os.getenv('UART_PORT') or '/dev/ttyAMA0'
BAUD_RATE                   = int(os.getenv('BAUD_RATE', '115200'))
TCP_PORT                    = int(os.getenv('TCP_PORT', '10110'))
TCP_HOST                    = os.getenv('TCP_HOST', 'localhost')
TCP_MAX_CLIENTS             = int(os.getenv('TCP_MAX_CLIENTS', '5'))

TCP_ALLOW                   = os.getenv('TCP_ALLOW', 'RMC,VTG,GGA')
TCP_ALLOW_LIST              = TCP_ALLOW.split(',')

TCP_ONLY_RTK_FIXED          = os.getenv('TCP_ONLY_RTK_FIXED', 'false').lower() == 'true'

NTRIP_HOST                  = os.getenv('NTRIP_HOST')
NTRIP_PORT                  = int(os.getenv('NTRIP_PORT', '2101'))
NTRIP_MOUNTPOINT            = os.getenv('NTRIP_MOUNTPOINT')
NTRIP_USERNAME              = os.getenv('NTRIP_USERNAME')
NTRIP_PASSWORD              = os.getenv('NTRIP_PASSWORD')
NTRIP_USE_HTTPS             = os.getenv('NTRIP_USE_HTTPS', 'false').lower() == 'true'
NTRIP_USER_AGENT            = os.getenv('NTRIP_USER_AGENT', 'PythonNTRIPClient')

PARQUET_LOG                 = os.getenv('PARQUET_LOG', 'rtk_log.parquet')
PARQUET_WRITE_PERIOD        = int(os.getenv('PARQUET_WRITE_PERIOD', '180'))

# ################################
# GLOBAL OBJECTS
clients: List[socket.socket]                = []
latest_gga: Optional[Dict[str, Any]]        = None
serial_port: Optional[serial.Serial]        = None
tcp_server: Optional[socket.socket]         = None
ntrip_session: Optional[requests.Session]   = None
shutdown_event                              = threading.Event()

# ################################
# PYARROW GPS LOGGING CLASS

class LightweightGPSLogger:
    def __init__(self, filename='rtk_log.parquet', write_interval=180):
        self.filename = filename
        self.write_interval = write_interval
        self.buffer = []
        self.buffer_lock = threading.Lock()
        
        # Define schema for consistent parquet structure
        self.schema = pa.schema([
            ('timestamp', pa.timestamp('us', tz='UTC')),
            ('latitude', pa.float64()),
            ('longitude', pa.float64()),
            ('fix_quality', pa.int8()),
            ('satellite_count', pa.uint8()),
            # Store geometry as WKT string instead of binary
            ('geometry_wkt', pa.string())
        ])
        
        # Start background writer thread
        self.writer_thread = threading.Thread(target=self._background_writer, daemon=True)
        self.writer_thread.start()
        
        print(f"GPS Logger started. Data -> {filename} every {write_interval}s")

    def append_gps_point(self, lat: float, lon: float, fix_quality: int, sat_count: int):
        """Buffer GPS data point"""
        timestamp = datetime.now(datetime.timezone.utc)
        
        # Create WKT POINT string instead of Shapely geometry
        geometry_wkt = f"POINT({lon} {lat})" if lat and lon else None
        
        with self.buffer_lock:
            self.buffer.append({
                'timestamp': timestamp,
                'latitude': lat,
                'longitude': lon,
                'fix_quality': int(fix_quality) if fix_quality else 0,
                'satellite_count': sat_count,
                'geometry_wkt': geometry_wkt
            })

    def _background_writer(self):
        """Background thread writes buffered data periodically"""
        while not shutdown_event.is_set():
            time.sleep(self.write_interval)
            self._write_buffer_to_file()

    def _write_buffer_to_file(self):
        """Write buffered data to Parquet file"""
        with self.buffer_lock:
            if not self.buffer:
                return
            
            data_to_write = self.buffer.copy()
            self.buffer.clear()

        try:
            # Convert to PyArrow table
            table = pa.table(data_to_write, schema=self.schema)
            
            # Append to existing file or create new one
            if os.path.exists(self.filename):
                # Read existing data and concatenate
                existing_table = pq.read_table(self.filename)
                combined_table = pa.concat_tables([existing_table, table])
                pq.write_table(combined_table, self.filename)
            else:
                pq.write_table(table, self.filename)
            
            print(f"{datetime.now()}: Wrote {len(data_to_write)} GPS points to {self.filename}")
            
        except Exception as e:
            print(f"Error writing to parquet: {e}")
            # Put data back in buffer on error
            with self.buffer_lock:
                self.buffer.extend(data_to_write)

    def force_write(self):
        """Manually trigger write (for shutdown)"""
        self._write_buffer_to_file()

    def read_data(self):
        """Read all data from parquet file"""
        if os.path.exists(self.filename):
            return pq.read_table(self.filename).to_pandas()
        return None

# ################################
# HELPER FUNCTIONS

def clean_address(addr: str) -> str:
    """Remove IPv6-mapped IPv4 prefix if present"""
    return addr.replace('::ffff:', '')

def parse_nmea_coord(coord: str, direction: str) -> Optional[float]:
    """Parse NMEA latitude/longitude coordinate"""
    if not coord or len(coord) < 4:
        return None
    
    try:
        if direction in ['N', 'S']:
            deg = int(coord[:2])
            min_val = float(coord[2:])
        else:  # E, W
            deg = int(coord[:3])
            min_val = float(coord[3:])
        
        val = deg + min_val / 60.0
        if direction in ['S', 'W']:
            val *= -1
        return val
    except (ValueError, IndexError):
        return None

def parse_gga(sentence: str) -> Optional[Dict[str, Any]]:
    """Parse GGA NMEA sentence"""
    parts = sentence.split(',')
    if len(parts) < 15:
        return None
    
    try:
        return {
            'lat': parse_nmea_coord(parts[2], parts[3]),
            'lon': parse_nmea_coord(parts[4], parts[5]),
            'fix': parts[6],
            'time': parts[1],
            'sats': int(parts[7]) if parts[7] else 0,
            'raw': sentence
        }
    except (ValueError, IndexError):
        return None

def get_fix_status(fix: str) -> str:
    """Convert fix status number to description"""
    status_map = {
        '0': 'No Fix',
        '1': 'GPS Fix',
        '2': 'DGPS Fix',
        '3': 'PPS Fix',
        '4': 'RTK Fix',
        '5': 'RTK Float',
        '6': 'Dead Reckoning',
        '7': 'Manual Input',
        '8': 'Simulation'
    }
    return status_map.get(fix, 'Unknown')

# ################################
# NTRIP CLIENT

def start_ntrip_stream():
    """Start NTRIP stream in a separate thread"""
    if not all([NTRIP_HOST, NTRIP_MOUNTPOINT, NTRIP_USERNAME, NTRIP_PASSWORD]):
        print("NTRIP configuration incomplete - skipping NTRIP client")
        return
    
    def ntrip_worker():
        global ntrip_session
        
        while not shutdown_event.is_set():
            try:
                ntrip_session = requests.Session()
                
                # Prepare authentication
                auth_string = f"{NTRIP_USERNAME}:{NTRIP_PASSWORD}"
                auth_encoded = base64.b64encode(auth_string.encode()).