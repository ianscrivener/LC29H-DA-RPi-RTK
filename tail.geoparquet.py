import geopandas as gpd
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get parquet log file, strip quotes if present
PARQUET_LOG = os.getenv('PARQUET_LOG', 'rtk_log.parquet').strip("'\"")

def geom_to_coords(geometry):
    """
    Convert a geometry object to its coordinates.
    """
    if geometry is not None:
        return geometry.x, geometry.y
    return None, None

def fix_expand_enum(fix):
    """
    Convert the fix enum to a human-readable string.
    """
    fix_map = {
        0: "No Fix",
        1: "GPS Fix",
        2: "DGPS Fix",
        3: "PPS Fix",
        4: "RTK Fix",
        5: "RTK Float",
        6: "Dead Reckoning",
        7: "Manual Input",
        8: "Simulation"
    }
    return fix_map.get(fix, "Unknown Fix")

# Read the parquet file using geopandas for geometry support
try:
    df = gpd.read_parquet(PARQUET_LOG)
except Exception as e:
    print(f"Error reading parquet file: {e}")
    exit(1)

# Get last 20 lines
raw = df.tail(20)

# Parse and print each line
for _, row in raw.iterrows():
    timestamp = row['timestamp']
    fix = row['fix']
    satellite_count = row['satellite_count']
    geometry = row['geometry']

    lat, lon = geom_to_coords(geometry)
    print(f"Timestamp: {timestamp}, Fix: {fix_expand_enum(fix)}, Satellite Count: {satellite_count}, Lat: {lat}, Long: {lon}")