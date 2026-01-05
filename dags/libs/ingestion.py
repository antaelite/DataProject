import requests
import csv
import os
import time


def get_velov_data(output_path, api_url=None, limit=100, desired_count=2000, max_loops=100, sleep_seconds=0.5):
    """
    Fetch Vélo'v bike station data from Grand Lyon API and save to CSV.

    Args:
        output_path: Full path to the output CSV file.
        api_url: API endpoint URL (uses default Grand Lyon API if None).
        limit: Number of items per API request.
        desired_count: Maximum number of stations to fetch.
        max_loops: Maximum pagination iterations.
        sleep_seconds: Delay between paginated requests.

    Returns:
        str: Path to the saved CSV file.
    """
    output_folder = os.path.dirname(output_path)
    if output_folder:
        os.makedirs(output_folder, exist_ok=True)

    if api_url is None:
        api_url = "https://data.grandlyon.com/geoserver/ogc/features/v1/collections/metropole-de-lyon:jcd_jcdecaux.jcdvelov/items"
    
    all_data = []
    start_index = 0
    loops = 0
    while len(all_data) < desired_count and loops < max_loops:
        params = {
            "limit": limit,
            "startIndex": start_index,
            "f": "json"
        }
        try:
            response = requests.get(api_url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            raise
        except ValueError as e:
            raise
        items = data.get('features', [])
        if not items:
            break

        all_data.extend(items)
        if len(items) < limit:
            break

        start_index += limit
        loops += 1
        if sleep_seconds:
            time.sleep(sleep_seconds)   
    final_data = all_data[:desired_count]
    
    fields_names = ["station_id", "name", "address", "district", "lat", "long", 
                    "bike_stands", "available_bikes", "status", "last_update"]

    with open(output_path, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields_names)
        writer.writeheader()

        for station in final_data:
            properties = station.get('properties', {})
            row = {
                "station_id": properties.get('number'),
                "name": properties.get('name'),
                "address": properties.get('address'),
                "district": properties.get('commune'),
                "lat": properties.get('lat'),
                "long": properties.get('lng'),
                "bike_stands": properties.get('bike_stands'),
                "available_bikes": properties.get('available_bikes'),
                "status": properties.get('status'),
                "last_update": properties.get('last_update')
            }
            writer.writerow(row)

    return output_path

