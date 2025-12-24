import pandas as pd 
import os
import numpy as np
import math

from pymongo import MongoClient

def accidents_cleaning (input_path, output_path):
    """
    Nettoie les données d'accidents.

    NOTE: Ce traitement suppose que le fichier d'entrée existe et est valide.
    Les vérifications d'existence/colonnes doivent être effectuées par des tâches
    de vérification distinctes (ex: `check_task_status`).
    """
    output_dir = os.path.dirname(output_path)
    os.makedirs(output_dir, exist_ok=True)

    df = pd.read_csv(input_path)
    if "dep" in df.columns:
        df = df[df["dep"].astype(str) == "69"]

    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["long"] = pd.to_numeric(df["long"], errors="coerce")
    df = df.dropna(subset=["lat", "long"])
    df = df[(df["lat"] != 0) & (df["long"] != 0)]

    df = df.drop_duplicates()

    keep_columns = []
    cols_to_keep = ["Num_Acc", "grav", "date", "an", "mois", "jour", "hrmn", "com", "lat", "long"]
    keep_columns = [col for col in df.columns if col in cols_to_keep]

    df = df[keep_columns]

    output_path = os.path.join(output_dir, "accidents_clean_lyon.csv")
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Saved cleaned accidents: {output_path} ({len(df)} rows)")


def clean_velov_data(input_path, output_path):
    """
    Nettoie les données Vélo'v.
    """
    df = pd.read_csv(input_path, sep=",") # Vérifie si c'est , ou ;

    # 1. Types
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['long'] = pd.to_numeric(df['long'], errors='coerce')
    df['available_bikes'] = pd.to_numeric(df['available_bikes'], errors='coerce').fillna(0).astype(int)
    
    # 2. Filtres
    df = df.dropna(subset=['lat', 'long'])
    
    if 'status' in df.columns:
        df = df[df['status'] == 'OPEN']

    # 3. Dates
    if 'last_update' in df.columns:
        df['last_update'] = pd.to_datetime(df['last_update'], errors='coerce')

    # 4. Sauvegarde
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Saved velov clean: {output_path} ({len(df)} stations)")

def check_task_status(path, min_rows=1, required_cols=None):
    """Vérifie qu'un fichier CSV existe, contient au moins `min_rows` lignes et
    possède les colonnes requises si spécifiées. Lève une exception en cas d'échec
    pour que la tâche Airflow marque l'échec et soit retriée selon la config.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Fichier introuvable : {path}")

    df = pd.read_csv(path)
    row_count = len(df)
    if row_count < min_rows:
        raise ValueError(f"Trop peu de lignes dans {path}: {row_count} < {min_rows}")

    if required_cols:
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"Colonnes manquantes dans {path}: {missing}")

    print(f"OK: {path} ({row_count} rows)")
    return True


def assign_accidents_to_stations(stations_path, accidents_path, output_stations_path=None, output_accidents_path=None, radius_m=500, station_id_col="station_id"):
    """Assigne chaque accident à la station la plus proche.

    - Compte le nombre d'accidents situés à moins de `radius_m` mètres d'une station
      et ajoute la colonne `accident_count_nearby` au CSV des stations.
    - Pour les accidents qui ne sont dans le rayon d'aucune station, on les assigne
      quand même à la station la plus proche et on marque `within_radius=False`.

    Args:
        stations_path (str): Chemin CSV stations avec colonnes lat/lng.
        accidents_path (str): Chemin CSV accidents avec colonnes lat/long (ou lng).
        output_stations_path (str): Chemin de sortie pour le CSV stations enrichi.
        output_accidents_path (str): Chemin de sortie pour le CSV accidents assignés.
        radius_m (int): Rayon en mètres pour considérer un accident "proche".
        station_id_col (str): Nom de la colonne identifiant la station.

    Returns:
        tuple(output_stations_path, output_accidents_path)
    """
    stations = pd.read_csv(stations_path)
    accidents = pd.read_csv(accidents_path)
    stations = stations.dropna(subset=['lat', 'long']).reset_index(drop=True)
    accidents = accidents.dropna(subset=['lat', 'long']).reset_index(drop=True)

    stations_lat = np.radians(stations['lat'].values)
    stations_lng = np.radians(stations['long'].values)

    # Préparer des listes pour stocker les résultats d'assignation
    assigned_station_ids = []  # id de la station la plus proche pour chaque accident
    assigned_distances = []    # distance en mètres à cette station
    within_radius_flags = []   # bool si distance <= radius_m

    # Boucle sur chaque accident pour trouver la station la plus proche
    for idx, acc in accidents.iterrows():
        lat_rad = math.radians(acc['lat'])
        lng_rad = math.radians(acc['long'])
        
        dists = _haversine_distance_m(lat_rad, lng_rad, stations_lat, stations_lng)
        # index de la station la plus proche
        nearest_idx = int(np.argmin(dists))
        nearest_dist = float(dists[nearest_idx])
        assigned_station_id = stations.iloc[nearest_idx].get(station_id_col, nearest_idx)

        # stocker les résultats
        assigned_station_ids.append(assigned_station_id)
        assigned_distances.append(nearest_dist)
        within_radius_flags.append(nearest_dist <= radius_m)

    # Ajouter les colonnes d'assignation au DataFrame des accidents
    accidents['assigned_station_id'] = assigned_station_ids
    accidents['distance_m'] = assigned_distances
    accidents['within_radius'] = within_radius_flags
    # 'assigned_to_deconne' indique les accidents qui étaient hors de tout cercle
    accidents['assigned_to_deconne'] = ~accidents['within_radius']

    # --- Comptage des accidents par station (seulement ceux à l'intérieur du rayon) ---
    counts = accidents[accidents['within_radius']].groupby('assigned_station_id').size().to_dict()
    # Si la colonne station_id existe on l'utilise pour mapper, sinon on mappe sur l'index
    stations['accident_count_nearby'] = stations[station_id_col].map(lambda s: int(counts.get(s, 0))) if station_id_col in stations.columns else stations.index.map(lambda i: int(counts.get(i, 0)))

    # --- Préparer les chemins de sortie si non précisés ---
    if output_stations_path is None:
        output_stations_path = os.path.join(os.path.dirname(stations_path), 'stations_with_accident_counts.csv')
    if output_accidents_path is None:
        output_accidents_path = os.path.join(os.path.dirname(accidents_path), 'accidents_assigned.csv')

    # Créer les dossiers de sortie si nécessaire
    os.makedirs(os.path.dirname(output_stations_path), exist_ok=True)
    os.makedirs(os.path.dirname(output_accidents_path), exist_ok=True)

    # Sauvegarder les résultats
    stations.to_csv(output_stations_path, index=False)
    accidents.to_csv(output_accidents_path, index=False)

    # Vérification finale : combien d'accidents étaient hors rayon
    n_outside = int((~accidents['within_radius']).sum())

    print(f"Assigned accidents saved: stations={output_stations_path}, accidents={output_accidents_path}, outside={n_outside}")

    return output_stations_path, output_accidents_path


def _haversine_distance_m(lat1, lon1, lat2_arr, lon2_arr):
    dlat = lat2_arr - lat1
    dlon = lon2_arr - lon1
    a = np.sin(dlat/2.0)**2 + np.cos(lat1)*np.cos(lat2_arr)*np.sin(dlon/2.0)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
    R = 6371000.0  # rayon de la Terre en mètres
    return R * c


def check_mongo_stations(mongo_uri="mongodb://mongo:27017/", db_name="VelovDB", collection_name="velov_stations", min_count=1):
    """Vérifie qu'il y a au moins `min_count` documents dans la collection Mongo.
    Lève une exception si la condition n'est pas satisfaite pour que la task Airflow échoue.
    """
    client = MongoClient(mongo_uri, username="admin", password="admin")
    try:
        count = client[db_name][collection_name].count_documents({})
    finally:
        client.close()

    if count < min_count:
        raise ValueError(f"Mongo collection {db_name}.{collection_name} has {count} documents < {min_count}")

    print(f"OK: Mongo {db_name}.{collection_name} contains {count} documents")
    return True


def save_stations_data_to_mongodb(csv_path):
    client = MongoClient(
        "mongodb://mongo:27017/",
        username="admin",
        password="admin"
    )
    db_name = "VelovDB"
    collection_name = "velov_stations"
    db = client[db_name]
    collection = db[collection_name]
    data = pd.read_csv(csv_path)
    records = []
    for _, row in data.iterrows():
        doc = row.to_dict()
        doc["location"] = {
            "type": "Point",
            "coordinates": [float(row["long"]), float(row["lat"])]
        }
        doc.pop("lat", None)
        doc.pop("long", None)
        if "accident_count_nearby" in doc:
            doc["accident_count_nearby"] = int(doc["accident_count_nearby"])
        records.append(doc)
    if records:
        collection.delete_many({})  # optionnel : reset collection
        collection.insert_many(records)
        print(f"Inserted {len(records)} documents into {db_name}.{collection_name}")
        collection.create_index([("location", "2dsphere")])
        print("Created 2dsphere index on 'location'")
    else:
        print("No records to insert")

    client.close()
