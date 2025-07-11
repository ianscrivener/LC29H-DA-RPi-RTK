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
from datetime import datetime
from typing import List, Optional, Dict, Any

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import threading


# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()


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

PARQUET_LOG                 = os.getenv('PARQUET_LOG','rtk_log.parquet')
PARQUET_WRITE_PERIOD        = os.getenv('PARQUET_WRITE_PERIOD',180)


# ################################
# GLOBAL OBJECTS
clients: List[socket.socket]                = []
latest_gga: Optional[Dict[str, Any]]        = None
serial_port: Optional[serial.Serial]        = None
tcp_server: Optional[socket.socket]         = None
ntrip_session: Optional[requests.Session]   = None
shutdown_event                              = threading.Event()



# ################################
# GEOPARQUET LOGGINF FUNCTIONS
# logger.append_gps_point(lat, lon, fix_quality, sat_count)
class GPSLogger:
    def __init__(self):        
        self.filename = PARQUET_LOG
        self.write_interval = PARQUET_WRITE_PERIOD
        self.buffer = []
        self.buffer_lock = threading.Lock()
        self.last_write_time = time.time()
        
        # Start background writer thread
        self.writer_thread = threading.Thread(target=self._background_writer, daemon=True)
        self.writer_thread.start()

        print("GPS Logger started. Call logger.append_gps_point() from your RTK process.")
        print("Data will be written to file every 180 seconds.")


    def append_gps_point(self, lat, lon, fix_quality, sat_count):
        """Called by external process every second to buffer GPS data"""
        timestamp = datetime.now(datetime.timezone.utc)
        
        with self.buffer_lock:
            self.buffer.append({
                'timestamp': timestamp,
                'fix': fix_quality,
                'satellite_count': sat_count,
                'latitude': lat,
                'longitude': lon
            })
    
    def _background_writer(self):
        """Background thread that writes buffered data every 180 seconds"""
        while True:
            time.sleep(self.write_interval)
            self._write_buffer_to_file()
    
    def _write_buffer_to_file(self):
        """Write all buffered data to GeoParquet file"""
        with self.buffer_lock:
            if not self.buffer:
                return
            
            # Create points from buffered data
            data_to_write = []
            for record in self.buffer:
                point = Point(record['longitude'], record['latitude'])
                data_to_write.append({
                    'timestamp': record['timestamp'],
                    'fix': record['fix'],
                    'satellite_count': record['satellite_count'],
                    'geometry': point
                })
            
            # Clear buffer
            self.buffer.clear()
        
        # Create GeoDataFrame
        gdf = gpd.GeoDataFrame(data_to_write, crs='EPSG:4326')
        gdf['fix'] = gdf['fix'].astype('int8')
        gdf['satellite_count'] = gdf['satellite_count'].astype('uint8')
        
        # Append to file
        if os.path.exists(self.filename):
            existing_gdf = gpd.read_parquet(self.filename)
            combined_gdf = pd.concat([existing_gdf, gdf], ignore_index=True)
            combined_gdf.to_parquet(self.filename)
        else:
            gdf.to_parquet(self.filename)
        
        print(f"{datetime.now()}: Wrote {len(data_to_write)} RTK data to {self.filename}")
    
    def force_write(self):
        """Manually trigger writing buffered data (useful for shutdown)"""
        self._write_buffer_to_file()



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
                auth_encoded = base64.b64encode(auth_string.encode()).decode()
                
                headers = {
                    'Ntrip-Version': 'Ntrip/2.0',
                    'User-Agent': NTRIP_USER_AGENT,
                    'Authorization': f'Basic {auth_encoded}',
                    'Connection': 'close'
                }
                
                protocol = 'https' if NTRIP_USE_HTTPS else 'http'
                url = f"{protocol}://{NTRIP_HOST}:{NTRIP_PORT}/{NTRIP_MOUNTPOINT}"
                
                print("Connecting to NTRIP caster...")
                response = ntrip_session.get(url, headers=headers, stream=True, timeout=30)
                
                if response.status_code != 200:
                    print(f"NTRIP connection failed: {response.status_code} {response.reason}")
                    time.sleep(10)
                    continue
                
                print("NTRIP connection established, streaming RTCM corrections...")
                
                # Stream RTCM data
                for chunk in response.iter_content(chunk_size=1024):
                    if shutdown_event.is_set():
                        break
                    if chunk and serial_port and serial_port.is_open:
                        serial_port.write(chunk)
                
                print("NTRIP stream ended")
                
            except Exception as e:
                print(f"NTRIP error: {e}")
                time.sleep(10)
    
    # Start NTRIP worker thread
    ntrip_thread = threading.Thread(target=ntrip_worker, daemon=True)
    ntrip_thread.start()
    
    # GGA sender thread
    def gga_sender():
        while not shutdown_event.is_set():
            time.sleep(60)  # Send every 60 seconds
            if latest_gga and latest_gga.get('raw') and ntrip_session:
                try:
                    # Send GGA to NTRIP caster (this is simplified - proper implementation would need to post to the stream)
                    print(f"NTRIP GGA sent: {latest_gga['raw']}")
                except Exception as e:
                    print(f"Error sending GGA to NTRIP: {e}")
    
    gga_thread = threading.Thread(target=gga_sender, daemon=True)
    gga_thread.start()

# ################################
# TCP SERVER

def handle_client(client_socket: socket.socket, address):
    """Handle individual TCP client"""
    try:
        print(f"TCP NMEA client connected: {clean_address(address[0])}:{address[1]}")
        
        while not shutdown_event.is_set():
            time.sleep(0.1)  # Prevent busy waiting
            
    except Exception as e:
        print(f"Client handler error: {e}")
    finally:
        if client_socket in clients:
            clients.remove(client_socket)
        try:
            client_socket.close()
        except:
            pass
        print(f"TCP NMEA client disconnected: {clean_address(address[0])}:{address[1]}")

def start_tcp_server():
    """Start TCP server"""
    global tcp_server
    
    tcp_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        tcp_server.bind((TCP_HOST, TCP_PORT))
        tcp_server.listen(TCP_MAX_CLIENTS)
        tcp_server.settimeout(1.0)  # Allow periodic checking of shutdown_event
        
        print(f"TCP NMEA server listening on port {TCP_PORT}")
        
        while not shutdown_event.is_set():
            try:
                client_socket, address = tcp_server.accept()
                clients.append(client_socket)
                
                # Handle client in separate thread
                client_thread = threading.Thread(
                    target=handle_client, 
                    args=(client_socket, address),
                    daemon=True
                )
                client_thread.start()
                
            except socket.timeout:
                continue  # Check shutdown_event
            except Exception as e:
                if not shutdown_event.is_set():
                    print(f"TCP server error: {e}")
                break
                
    except Exception as e:
        print(f"Failed to start TCP server: {e}")
    finally:
        if tcp_server:
            tcp_server.close()

# ################################
# SERIAL HANDLING

def should_send_nmea(sentence: str) -> bool:
    """Check if NMEA sentence should be sent based on filters"""
    # Check if sentence type is in allow list
    if len(sentence) >= 6:
        nmea_suffix = sentence[3:6]
        if nmea_suffix not in TCP_ALLOW_LIST:
            return False
    
    # Check RTK fixed filter
    if TCP_ONLY_RTK_FIXED:
        if sentence.startswith('$GNGGA') or sentence.startswith('$GPGGA'):
            fields = sentence.split(',')
            if len(fields) > 6 and fields[6] != '4':  # Not RTK fixed
                return False
        elif sentence.startswith('$GNRMC') or sentence.startswith('$GPRMC'):
            fields = sentence.split(',')
            if len(fields) > 12 and fields[12] != 'R':  # Not RTK fixed
                return False
    
    return True

def broadcast_to_clients(data: str):
    """Send data to all connected TCP clients"""
    global clients
    disconnected_clients = []
    
    for client in clients[:]:  # Create a copy to iterate over
        try:
            if client.fileno() != -1:  # Check if socket is still valid
                client.send((data + '\n').encode())
            else:
                disconnected_clients.append(client)
        except Exception as e:
            disconnected_clients.append(client)
    
    # Remove disconnected clients
    for client in disconnected_clients:
        if client in clients:
            clients.remove(client)
        try:
            client.close()
        except:
            pass

def start_serial_reader():
    """Start serial port reader"""
    global serial_port, latest_gga
    
    try:
        serial_port = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=1)
        print(f"Serial port {SERIAL_PORT} opened at {BAUD_RATE} baud")
        
        while not shutdown_event.is_set():
            try:
                line = serial_port.readline().decode('ascii', errors='ignore').strip()
                
                if not line:
                    continue
                
                # Parse GGA for position tracking
                if line.startswith('$GPGGA') or line.startswith('$GNGGA'):
                    gga = parse_gga(line)
                    if gga:
                        latest_gga = gga
                        # Append to geoparquet logger
                        logger.append_gps_point(gga['lat'], gga['lon'], gga['fix'], gga['sats'])      
                
                # Check if we should send this sentence
                if should_send_nmea(line):
                    broadcast_to_clients(line)
                
            except Exception as e:
                print(f"Serial read error: {e}")
                time.sleep(1)
                
    except Exception as e:
        print(f"Failed to open serial port {SERIAL_PORT}: {e}")
        sys.exit(1)
    finally:
        if serial_port and serial_port.is_open:
            serial_port.close()
            print("Serial port closed")

# ################################
# CONSOLE LOGGING

def start_position_logger():
    """Log GNSS position every 15 seconds"""
    def logger():
        while not shutdown_event.is_set():
            time.sleep(15)
            
            if latest_gga:
                lat_str = f"{latest_gga['lat']:.7f}" if latest_gga['lat'] is not None else "---"
                lon_str = f"{latest_gga['lon']:.7f}" if latest_gga['lon'] is not None else "---"
                sats_str = str(latest_gga['sats']) if latest_gga['sats'] else "--"
                
                print(f" GNSS $GPGGA - Lat: {lat_str}, Lon: {lon_str}, "
                      f"Sats: {sats_str}, Fix: {get_fix_status(latest_gga['fix'])}")
            else:
                print("[GNSS] No GGA data yet...")
    
    log_thread = threading.Thread(target=logger, daemon=True)
    log_thread.start()

# ################################
# SIGNAL HANDLING

def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully"""
    print("\nCtrl+C received - gracefully shutting down...")
    shutdown_event.set()
    
    # Close all client connections
    for client in clients[:]:
        try:
            client.send(b"Server shutting down\n")
            client.close()
        except:
            pass
    clients.clear()
    
    # Close serial port
    if serial_port and serial_port.is_open:
        serial_port.close()
    
    # Close TCP server
    if tcp_server:
        tcp_server.close()
    
    # Close NTRIP session
    if ntrip_session:
        ntrip_session.close()
    
    print("App terminated. Goodbye!")
    sys.exit(0)

# ################################
# MAIN

def main():
    """Main function"""

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("Starting GNSS NMEA TCP Bridge...")
    
    # Start position logger
    start_position_logger()
    
    # Start NTRIP client
    start_ntrip_stream()
    
    # Start TCP server in separate thread
    tcp_thread = threading.Thread(target=start_tcp_server, daemon=True)
    tcp_thread.start()
    
    # Start serial reader (main thread)
    start_serial_reader()

if __name__ == "__main__":
    # initiate geoparquet logger process
    logger = GPSLogger()

    # Start the main application
    main()

    # try:
    #     while True:
    #         time.sleep(1)  # Keep main thread alive
    # except KeyboardInterrupt:
    #     print("\nShutting down...")
    #     logger.force_write()  # Write any remaining buffered data