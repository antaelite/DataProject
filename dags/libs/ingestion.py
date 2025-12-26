import requests
import csv
import os
import time

def get_velov_data(output_path, api_url=None, limit=100, desired_count=2000, max_loops=100, sleep_seconds=0.5):
    """
    Fetch Vélo'v bike station data and save it to a specific CSV path.
    Args:
        output_path (str): Chemin complet du fichier de sortie (ex: /data/landing/file.csv)
        api_url (str): URL de l'API (optionnel, sinon utilise la valeur par défaut)
        limit (int): Pagination
        desired_count (int): Nombre max d'éléments à récupérer
        max_loops (int): Safeguard to avoid infinite pagination loops
        sleep_seconds (float): Wait between paged requests to avoid rate limits
    """
    output_folder = os.path.dirname(output_path)
    if output_folder: # Si le chemin n'est pas juste "velov.csv"
        os.makedirs(output_folder, exist_ok=True)

    #Configuration de l'API
    if api_url is None:
        api_url = "https://data.grandlyon.com/geoserver/ogc/features/v1/collections/metropole-de-lyon:jcd_jcdecaux.jcdvelov/items"
    
    all_data = []
    start_index = 0
    loops = 0

    print(f"Début de l'ingestion vers le fichier : {output_path}...")
    print(f"URL cible : {api_url}")

    # Boucle de pagination
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
            print(f"Erreur réseau/API : {e}")
            # Propagate so Airflow marks the task as failed and retries according to DAG config
            raise
        except ValueError as e:
            print(f"Réponse non JSON ou décodage échoué : {e}")
            raise

        items = data.get('features', [])

        if not items:
            print("Aucun item renvoyé par l'API, arrêt de la boucle.")
            break

        all_data.extend(items)
        print(f"Récupéré : {len(all_data)} stations...")
        
        if len(items) < limit:
            print("Fin des pages disponibles.")
            break

        start_index += limit
        loops += 1
        if sleep_seconds:
            time.sleep(sleep_seconds)

    if loops >= max_loops:
        print("Arrêt: nombre max d'itérations atteint (max_loops)")

    # Écriture du CSV     
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
    
    print(f"Ingestion terminée. Fichier sauvegardé dans : {output_path}")

    return output_path

