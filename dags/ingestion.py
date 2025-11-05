import requests
import csv


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
            "startIndex": start_index
            "format": "json"
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
        print(f"Request {nb_request} - Retrieved {len(features)} items (total {len(total_results)})")

        start_index += limit

    return all_data[:desired_count]