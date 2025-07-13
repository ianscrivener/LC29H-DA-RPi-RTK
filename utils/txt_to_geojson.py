#!/usr/bin/env python3
import csv
import json
import sys
import argparse

def gps_to_geojson(csv_file_path):
    """
    Convert GPS CSV file to GeoJSON FeatureCollection.
    
    Args:
        csv_file_path (str): Path to CSV file with GPS data
        
    Returns:
        dict: GeoJSON FeatureCollection
    """
    features = []
    
    with open(csv_file_path, 'r') as file:
        reader = csv.DictReader(file)
        
        for row in reader:
            # Skip empty rows
            if not any(row.values()):
                continue
                
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [
                        float(row['longitude']), 
                        float(row['latitude'])
                    ]
                },
                "properties": {}
            }
            
            # Add all other columns as properties
            for key, value in row.items():
                if key not in ['latitude', 'longitude']:
                    # Convert numeric values
                    if key in ['fix_quality', 'satellite_count']:
                        try:
                            feature['properties'][key] = int(value) if value else None
                        except ValueError:
                            feature['properties'][key] = value
                    else:
                        feature['properties'][key] = value
            
            features.append(feature)
    
    return {
        "type": "FeatureCollection",
        "features": features
    }

def main():
    parser = argparse.ArgumentParser(description='Convert GPS CSV to GeoJSON')
    parser.add_argument('input_file', help='Input CSV file path')
    parser.add_argument('-o', '--output', help='Output GeoJSON file path (optional)')
    
    args = parser.parse_args()
    
    try:
        geojson = gps_to_geojson(args.input_file)
        
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(geojson, f, indent=2)
            print(f"GeoJSON saved to {args.output}")
        else:
            print(json.dumps(geojson, indent=2))
            
    except FileNotFoundError:
        print(f"Error: File '{args.input_file}' not found", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()