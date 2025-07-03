import socket
import pynmea2
import math

def haversine(lat1, lon1, lat2, lon2):
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371000  # Earth radius in meters
    return c * r

TCP_IP = 'localhost'
TCP_PORT = 10112  # Replace with your TCP port

origin_lat = origin_lon = None
prev_lat = prev_lon = None

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((TCP_IP, TCP_PORT))
    while True:
        data = s.recv(1024).decode('ascii', errors='ignore')
        for line in data.split('\n'):
            if line.startswith('$GPGGA') or line.startswith('$GNGGA'):
                try:
                    msg = pynmea2.parse(line)
                    lat = msg.latitude
                    lon = msg.longitude

                    if origin_lat is None and origin_lon is None:
                        origin_lat, origin_lon = lat, lon
                        prev_lat, prev_lon = lat, lon
                        print(f"Origin set at lat: {origin_lat}, lon: {origin_lon}")
                        continue

                    # Distance from previous point
                    dist_prev = haversine(prev_lat, prev_lon, lat, lon)
                    # Distance from origin point
                    dist_origin = haversine(origin_lat, origin_lon, lat, lon)

                    print(f"Moved {dist_prev:.2f} meters from previous point.")
                    print(f"Distance from origin: {dist_origin:.2f} meters.")

                    prev_lat, prev_lon = lat, lon

                except pynmea2.ParseError:
                    continue
