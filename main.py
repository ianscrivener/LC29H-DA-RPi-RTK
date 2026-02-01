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
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("python-dotenv not installed, using environment variables only")

# ################################
# VARIABLES
SERIAL_PORT                 = os.getenv('SERIAL_PORT') or os.getenv('UART_PORT') or '/dev/ttyS0'
BAUD_RATE                   = int(os.getenv('BAUD_RATE', '115200'))
TCP_PORT                    = int(os.getenv('TCP_PORT', '10110'))
TCP_HOST                    = os.getenv('TCP_HOST', '0.0.0.0')  # Listen on all interfaces
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

TEXT_LOG                    = os.getenv('TEXT_LOG', 'rtk_log.txt')
LOG_WRITE_PERIOD            = int(os.getenv('LOG_WRITE_PERIOD', '180'))

# ################################
# GLOBAL OBJECTS
clients: List[socket.socket]                = []
clients_lock                               = threading.Lock()  # Thread safety for clients
latest_gga: Optional[Dict[str, Any]]        = None
latest_rmc: Optional[Dict[str, Any]]        = None
utc_date: str                               = datetime.now(timezone.utc).strftime('%Y-%m-%d')
serial_port: Optional[serial.Serial]        = None
tcp_server: Optional[socket.socket]         = None
ntrip_session: Optional[requests.Session]   = None
shutdown_event                              = threading.Event()

# ################################
# TEXT FILE GPS LOGGING CLASS

class LightweightGPSLogger:
    def __init__(self, filename='rtk_log.txt', write_interval=180):
        self.filename = filename
        self.write_interval = write_interval
        self.buffer = []
        self.buffer_lock = threading.Lock()
        
        # Create header if file doesn't exist
        if not os.path.exists(self.filename):
            with open(self.filename, 'w') as f:
                f.write("gps_datetime,latitude,longitude,fix_quality,satellite_count\n")
        
        # Start background writer thread
        self.writer_thread = threading.Thread(target=self._background_writer, daemon=True)
        self.writer_thread.start()
        
        print(f"GPS Logger started. Data -> {filename} every {write_interval}s")

    def append_gps_point(self, lat: float, lon: float, fix_quality: int, sat_count: int, gps_time: str = None, gps_datetime: str = None):
        """Buffer GPS data point"""
        global utc_date
        
        # Handle both parameter names for backwards compatibility
        time_param = gps_time or gps_datetime
        
        # Create full datetime from UTC date and GPS time
        if time_param and len(time_param) >= 6:
            try:
                hours = int(time_param[:2])
                minutes = int(time_param[2:4])
                seconds = float(time_param[4:])
                
                # Combine UTC date with GPS time
                dt = datetime.strptime(utc_date, '%Y-%m-%d').replace(
                    hour=hours,
                    minute=minutes,
                    second=int(seconds),
                    microsecond=int((seconds % 1) * 1000000),
                    tzinfo=timezone.utc
                )
                timestamp = dt.isoformat()
            except (ValueError, IndexError):
                # Fall back to system time if GPS time parsing fails
                timestamp = datetime.now(timezone.utc).isoformat()
        else:
            # Fall back to system time if no GPS time
            timestamp = datetime.now(timezone.utc).isoformat()
        
        
        with self.buffer_lock:
            self.buffer.append({
                'gps_datetime': timestamp,
                'latitude': lat if lat else "",
                'longitude': lon if lon else "",
                'fix_quality': int(fix_quality) if fix_quality else 0,
                'satellite_count': sat_count if sat_count else 0
            })

    def _background_writer(self):
        """Background thread writes buffered data periodically"""
        while not shutdown_event.is_set():
            time.sleep(self.write_interval)
            self._write_buffer_to_file()

    def _write_buffer_to_file(self):
        """Write buffered data to text file"""

        # If file doesn't exist, write header first
        if not os.path.exists(self.filename):
            with open(self.filename, 'w') as f:
                f.write("gps_datetime,latitude,longitude,fix_quality,satellite_count\n")

        with self.buffer_lock:
            if not self.buffer:
                return
            
            data_to_write = self.buffer.copy()
            self.buffer.clear()

        try:
            with open(self.filename, 'a') as f:
                for point in data_to_write:
                    line = f"{point['gps_datetime']},{point['latitude']},{point['longitude']},{point['fix_quality']},{point['satellite_count']}\n"
                    f.write(line)
            
            print(f"{datetime.now()}: Wrote {len(data_to_write)} GPS points to {self.filename}")
            
        except Exception as e:
            print(f"Error writing to text file: {e}")
            # Put data back in buffer on error
            with self.buffer_lock:
                self.buffer.extend(data_to_write)

    def force_write(self):
        """Manually trigger write (for shutdown)"""
        self._write_buffer_to_file()

    def read_data(self):
        """Read all data from text file"""
        try:
            with open(self.filename, 'r') as f:
                return f.read()
        except FileNotFoundError:
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

def parse_nmea_time_date(time_str: str, date_str: str) -> Optional[str]:
    """Parse NMEA time and date into ISO format"""
    if not time_str or not date_str:
        return None
    
    try:
        # Parse time (HHMMSS.sss)
        if len(time_str) >= 6:
            hours = int(time_str[:2])
            minutes = int(time_str[2:4])
            seconds = float(time_str[4:])
        else:
            return None
        
        # Parse date (DDMMYY)
        if len(date_str) == 6:
            day = int(date_str[:2])
            month = int(date_str[2:4])
            year = 2000 + int(date_str[4:])  # Assume 20xx
        else:
            return None
        
        # Create datetime object
        dt = datetime(year, month, day, hours, minutes, int(seconds), 
                     int((seconds % 1) * 1000000), timezone.utc)
        return dt.isoformat()
        
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

def parse_rmc(sentence: str) -> Optional[Dict[str, Any]]:
    """Parse RMC NMEA sentence for date/time"""
    global utc_date
    
    parts = sentence.split(',')
    if len(parts) < 10:
        return None
    
    try:
        # Update global UTC date if we have valid date
        if parts[9] and len(parts[9]) == 6:
            day = int(parts[9][:2])
            month = int(parts[9][2:4])
            year = 2000 + int(parts[9][4:])  # Assume 20xx
            new_date = f"{year:04d}-{month:02d}-{day:02d}"
            
            if new_date != utc_date:
                utc_date = new_date
                print(f"Updated UTC date from GPS: {utc_date}")
        
        return {
            'time': parts[1],
            'status': parts[2],
            'lat': parse_nmea_coord(parts[3], parts[4]),
            'lon': parse_nmea_coord(parts[5], parts[6]),
            'date': parts[9],
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

def get_gps_datetime() -> Optional[str]:
    """Get GPS datetime from latest RMC data"""
    global latest_rmc
    if latest_rmc and latest_rmc.get('time') and latest_rmc.get('date'):
        return parse_nmea_time_date(latest_rmc['time'], latest_rmc['date'])
    return None

# ################################
# NTRIP CLIENT

def start_ntrip_stream():
    """Start NTRIP stream in a separate thread"""
    if not all([NTRIP_HOST, NTRIP_MOUNTPOINT, NTRIP_USERNAME, NTRIP_PASSWORD]):
        print("NTRIP configuration incomplete - skipping NTRIP client")
        return
    
    def ntrip_worker():
        while not shutdown_event.is_set():
            ntrip_socket = None
            try:
                print(f"Connecting to NTRIP: {NTRIP_HOST}:{NTRIP_PORT}/{NTRIP_MOUNTPOINT}")
                
                # Create socket connection
                ntrip_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                ntrip_socket.settimeout(10)
                ntrip_socket.connect((NTRIP_HOST, NTRIP_PORT))
                
                # Prepare HTTP request
                auth_string = f"{NTRIP_USERNAME}:{NTRIP_PASSWORD}"
                auth_encoded = base64.b64encode(auth_string.encode()).decode()
                
                request = (
                    f"GET /{NTRIP_MOUNTPOINT} HTTP/1.0\r\n"
                    f"User-Agent: {NTRIP_USER_AGENT}\r\n"
                    f"Authorization: Basic {auth_encoded}\r\n"
                    f"Accept: */*\r\n"
                    f"Connection: close\r\n"
                    f"\r\n"
                )
                
                # Send request
                ntrip_socket.send(request.encode())
                
                # Read response header
                response = b""
                while b"\r\n\r\n" not in response:
                    data = ntrip_socket.recv(1)
                    if not data:
                        raise Exception("Connection closed while reading header")
                    response += data
                
                header = response.decode('utf-8', errors='ignore')
                
                # Check for successful response (accept both HTTP 200 and ICY 200)
                if "200 OK" not in header:
                    raise Exception(f"NTRIP server returned: {header.split()[0:3]}")
                
                print("NTRIP stream connected successfully")
                
                # Set socket to non-blocking for data streaming
                ntrip_socket.settimeout(1.0)
                
                # Stream correction data
                while not shutdown_event.is_set():
                    try:
                        data = ntrip_socket.recv(1024)
                        if not data:
                            print("NTRIP stream ended")
                            break
                        
                        # Send correction data to GPS receiver
                        if serial_port:
                            try:
                                serial_port.write(data)
                            except Exception as e:
                                print(f"Error writing NTRIP data to serial: {e}")
                                break
                                
                    except socket.timeout:
                        continue  # Normal timeout, keep trying
                    except Exception as e:
                        print(f"Error reading NTRIP data: {e}")
                        break
                        
            except Exception as e:
                print(f"NTRIP connection error: {e}")
                
            finally:
                if ntrip_socket:
                    try:
                        ntrip_socket.close()
                    except:
                        pass
                
                if not shutdown_event.is_set():
                    print("Retrying NTRIP connection in 10 seconds...")
                    time.sleep(10)
    
    threading.Thread(target=ntrip_worker, daemon=True).start()

# ################################
# TCP SERVER

def handle_client(client_socket: socket.socket, address: str):
    """Handle individual TCP client"""
    print(f"Client connected from {clean_address(address)}")
    
    try:
        while not shutdown_event.is_set():
            time.sleep(0.1)  # Prevent busy loop
    except Exception as e:
        print(f"Client {clean_address(address)} error: {e}")
    finally:
        try:
            client_socket.close()
        except:
            pass
        
        with clients_lock:
            if client_socket in clients:
                clients.remove(client_socket)
        
        print(f"Client {clean_address(address)} disconnected")

def start_tcp_server():
    """Start TCP server in a separate thread"""
    global tcp_server
    
    def tcp_worker():
        global tcp_server
        
        try:
            tcp_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            tcp_server.bind((TCP_HOST, TCP_PORT))
            tcp_server.listen(TCP_MAX_CLIENTS)
            tcp_server.settimeout(1.0)
            
            print(f"TCP server listening on {TCP_HOST}:{TCP_PORT}")
            
            while not shutdown_event.is_set():
                try:
                    client_socket, address = tcp_server.accept()
                    
                    with clients_lock:
                        if len(clients) >= TCP_MAX_CLIENTS:
                            print(f"Max clients reached, rejecting {clean_address(address[0])}")
                            client_socket.close()
                            continue
                        
                        clients.append(client_socket)
                    
                    threading.Thread(
                        target=handle_client, 
                        args=(client_socket, address[0]), 
                        daemon=True
                    ).start()
                    
                except socket.timeout:
                    continue
                except Exception as e:
                    if not shutdown_event.is_set():
                        print(f"TCP server error: {e}")
                    break
        
        except Exception as e:
            print(f"Failed to start TCP server: {e}")
        
        finally:
            if tcp_server:
                tcp_server.close()
    
    threading.Thread(target=tcp_worker, daemon=True).start()

def broadcast_to_clients(data: bytes):
    """Send data to all connected TCP clients"""
    with clients_lock:
        if not clients:
            return
        
        disconnected = []
        
        for client in clients:
            try:
                client.send(data)
            except Exception:
                disconnected.append(client)
        
        # Remove disconnected clients
        for client in disconnected:
            if client in clients:
                clients.remove(client)
            try:
                client.close()
            except:
                pass

# ################################
# SERIAL PROCESSING

def should_send_sentence(sentence: str) -> bool:
    """Check if sentence should be sent to TCP clients"""
    if not sentence.startswith('$'):
        return False
    
    # Extract sentence type (e.g., GNGGA, GNRMC, etc.)
    parts = sentence.split(',')
    if len(parts) < 1:
        return False
    
    sentence_type = parts[0][3:]  # Remove $GN prefix
    
    if sentence_type not in TCP_ALLOW_LIST:
        return False
    
    # If RTK-only mode is enabled, only send RTK fixed positions
    if TCP_ONLY_RTK_FIXED and sentence_type == 'GGA':
        gga_data = parse_gga(sentence)
        if not gga_data or gga_data.get('fix') != '4':
            return False
    
    return True

def process_serial_data():
    """Main serial data processing loop"""
    global latest_gga, latest_rmc, serial_port
    
    # Initialize GPS logger
    gps_logger = LightweightGPSLogger(TEXT_LOG, LOG_WRITE_PERIOD)
    
    buffer = ""
    
    print(f"Starting serial processing on {SERIAL_PORT} at {BAUD_RATE} baud")
    
    try:
        serial_port = serial.Serial(SERIAL_PORT, BAUD_RATE, timeout=1)
        print(f"Serial port {SERIAL_PORT} opened successfully")
        
        while not shutdown_event.is_set():
            try:
                data = serial_port.read(1024)
                if not data:
                    continue
                
                buffer += data.decode('utf-8', errors='ignore')
                
                # Process complete sentences
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    sentence = line.strip()
                    
                    if not sentence:
                        continue
                    
                    # Parse GGA sentences for position logging
                    if sentence.startswith('$') and 'GGA' in sentence:
                        gga_data = parse_gga(sentence)
                        if gga_data:
                            latest_gga = gga_data
                            
                            # Get GPS datetime from latest RMC
                            gps_datetime = get_gps_datetime()
                            
                            # Log position data
                            if gga_data.get('lat') and gga_data.get('lon'):
                                gps_logger.append_gps_point(
                                    lat=gga_data['lat'],
                                    lon=gga_data['lon'],
                                    fix_quality=int(gga_data.get('fix', 0)),
                                    sat_count=gga_data.get('sats', 0),
                                    gps_datetime=gps_datetime
                                )
                            
                            # Status output
                            fix_status = get_fix_status(gga_data.get('fix', '0'))
                            print(f"GPS: {gga_data.get('lat', 'N/A'):.6f}, "
                                  f"{gga_data.get('lon', 'N/A'):.6f} | "
                                  f"{fix_status} | {gga_data.get('sats', 0)} sats")
                    
                    # Parse RMC sentences for date/time
                    elif sentence.startswith('$') and 'RMC' in sentence:
                        rmc_data = parse_rmc(sentence)
                        if rmc_data:
                            latest_rmc = rmc_data
                    
                    # Broadcast allowed sentences to TCP clients
                    if should_send_sentence(sentence):
                        broadcast_to_clients((sentence + '\r\n').encode())
                        
            except Exception as e:
                print(f"Serial processing error: {e}")
                time.sleep(1)
                
    except Exception as e:
        print(f"Failed to open serial port {SERIAL_PORT}: {e}")
        return
    
    finally:
        if serial_port:
            serial_port.close()
        
        # Force write any remaining GPS data
        gps_logger.force_write()

# ################################
# SIGNAL HANDLERS

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    print(f"\nReceived signal {signum}, shutting down...")
    shutdown_event.set()

# ################################
# MAIN

def main():
    """Main function"""
    print("RTK GPS Bridge Starting...")
    print(f"Serial: {SERIAL_PORT} @ {BAUD_RATE} baud")
    print(f"TCP Server: {TCP_HOST}:{TCP_PORT}")
    print(f"GPS Log: {TEXT_LOG}")
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start services
    start_tcp_server()
    start_ntrip_stream()
    
    # Main processing loop
    try:
        process_serial_data()
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received")
    
    # Cleanup
    shutdown_event.set()
    
    # Close all client connections
    with clients_lock:
        for client in clients[:]:
            try:
                client.close()
            except:
                pass
    
    # Close TCP server
    if tcp_server:
        tcp_server.close()
    
    # Close NTRIP session
    if ntrip_session:
        ntrip_session.close()
    
    print("Shutdown complete")

if __name__ == "__main__":
    main()
