import requests
import csv

def get_velov_data(output_folder, limit, desired_count):
    """
    Fetch Vélo'v bike station data and save it to a CSV file.
    """
    fields_names = ["station_id", "name", "address", "district", "position_lat", "position_lng",
                    "bike_stands","available_bikes", "status", "last_update_date"]

    velov_data = fetch_velov_v_data(limit, desired_count)
    output_path = output_folder + f"/velov_data.csv"
    with open(output_path, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields_names)
        writer.writeheader()
        
        for station in velov_data:
            properties = station.get('properties', {})
            row = {
                "station_id": properties.get('number'),
                "name": properties.get('name'),
                "address": properties.get('address'),
                "district": properties.get('commune'),
                "position_lat": properties.get('lat'),
                "position_lng": properties.get('lng'),
                "bike_stands": properties.get('bike_stands'),
                "available_bikes": properties.get('available_bikes'),
                "status": properties.get('status'),
                "last_update_date": properties.get('last_update')
            }
            writer.writerow(row)
    return True

def fetch_velov_v_data(limit=100, desired_count=1000):
    """
    Fetch Vélo'v bike station data from the Grand Lyon API.
    The API uses pagination with the 'startIndex' parameter.
    """
    base_url = "https://data.grandlyon.com/geoserver/ogc/features/v1/collections/metropole-de-lyon:jcd_jcdecaux.jcdvelov/items"
    all_data = []
    start_index = 0
    nb_requests = 0
    
    while len(all_data) < desired_count:
        params = {
            "limit": limit,
            "startIndex": start_index,
            "f": "json"
        }
        response = requests.get(base_url, params=params)
        if response.status_code != 200:
            print(f"Error fetching data: {response.status_code}")
            break
        data = response.json()
        items = data.get('features', [])
        all_data.extend(items)
        nb_requests += 1
        if len(items) < limit:
            break

        start_index += limit

    return all_data[:desired_count]

def main():
    output_folder = "../data/landing"
    limit = 100
    desired_count = 2000

    success = get_velov_data(output_folder, limit, desired_count)
    if success:
        print("Vélo'v data ingestion completed successfully.")
    else:
        print("Vélo'v data ingestion failed.")

if __name__ == "__main__":
    main()