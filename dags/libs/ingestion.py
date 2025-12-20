import requests
import csv
import os

def get_velov_data(output_path, api_url=None, limit=100, desired_count=2000):
    """
    Fetch Vélo'v bike station data and save it to a specific CSV path.
    Args:
        output_path (str): Chemin complet du fichier de sortie (ex: /data/landing/file.csv)
        api_url (str): URL de l'API (optionnel, sinon utilise la valeur par défaut)
        limit (int): Pagination
        desired_count (int): Nombre max d'éléments à récupérer
    """

    output_folder = os.path.dirname(output_path)
    if output_folder: # Si le chemin n'est pas juste "velov.csv"
        os.makedirs(output_folder, exist_ok=True)

    #Configuration de l'API
    if api_url is None:
        api_url = "https://data.grandlyon.com/geoserver/ogc/features/v1/collections/metropole-de-lyon:jcd_jcdecaux.jcdvelov/items"
    
    all_data = []
    start_index = 0

    print(f"Début de l'ingestion vers le fichier : {output_path}...")
    print(f"URL cible : {api_url}")

    # Boucle de pagination
    while len(all_data) < desired_count:
        params = {
            "limit": limit,
            "startIndex": start_index,
            "f": "json"
        }
        try:
            response = requests.get(api_url, params=params, timeout=10)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Erreur API : {e}")
            break

        data = response.json()
        items = data.get('features', [])

        if not items:
            break

        all_data.extend(items)
        print(f"Récupéré : {len(all_data)} stations...")
        
        if len(items) < limit:
            break

        start_index += limit

    # Écriture du CSV     
    final_data = all_data[:desired_count]
    
    fields_names = ["station_id", "name", "address", "district", "lat", "lng", 
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
                "lng": properties.get('lng'),
                "bike_stands": properties.get('bike_stands'),
                "available_bikes": properties.get('available_bikes'),
                "status": properties.get('status'),
                "last_update": properties.get('last_update')
            }
            writer.writerow(row)
    
    print(f"Ingestion terminée. Fichier sauvegardé dans : {output_path}")

    return output_path

if __name__ == "__main__":
    fichier_test_local = "../../data/landing/velov_raw.csv"
    
    print("TEST manuel de la fonction d'ingestion Vélo'v...")
    try:
        chemin_fichier = get_velov_data(output_path=fichier_test_local, limit=10, desired_count=20)
        print(f"TEST RÉUSSI ! Fichier créé : {chemin_fichier}")
    except Exception as e:
        print(f"TEST ÉCHOUÉ.")
        print(f"Raison de l'erreur : {e}")

