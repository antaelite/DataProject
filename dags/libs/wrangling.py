import pandas as pd
import os
import numpy as np
import math

from pymongo import MongoClient


def accidents_cleaning(input_path, output_path):
    """
    Clean accident data: filter by department, remove invalid coordinates, deduplicate.

    Args:
        input_path: Path to raw accidents CSV.
        output_path: Path for cleaned output CSV.
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


def clean_velov_data(input_path, output_path):
    """
    Clean Vélo'v station data: convert types, filter invalid coordinates and closed stations.

    Args:
        input_path: Path to raw Vélo'v CSV.
        output_path: Path for cleaned output CSV.
    """
    df = pd.read_csv(input_path, sep=",") 

    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['long'] = pd.to_numeric(df['long'], errors='coerce')
    df['available_bikes'] = pd.to_numeric(df['available_bikes'], errors='coerce').fillna(0).astype(int)
    
    df = df.dropna(subset=['lat', 'long'])
    
    if 'status' in df.columns:
        df = df[df['status'] == 'OPEN']

    if 'last_update' in df.columns:
        df['last_update'] = pd.to_datetime(df['last_update'], errors='coerce')

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)


def check_task_status(path, min_rows=1, required_cols=None):
    """
    Validate that a CSV file exists, has minimum rows, and contains required columns.

    Args:
        path: Path to the CSV file.
        min_rows: Minimum required row count.
        required_cols: List of required column names.

    Returns:
        bool: True if validation passes.

    Raises:
        FileNotFoundError: If file does not exist.
        ValueError: If row count or columns are insufficient.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found : {path}")

    df = pd.read_csv(path)
    row_count = len(df)
    if row_count < min_rows:
        raise ValueError(f"Insufficient rows in {path}: {row_count} < {min_rows}")

    if required_cols:
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"missing columns in {path}: {missing}")

    return True


def assign_accidents_to_stations(stations_path, accidents_path, output_stations_path=None, output_accidents_path=None, radius_m=500, station_id_col="station_id"):
    """
    Assign each accident to its nearest station.

    Counts accidents within radius_m of each station and adds 'accident_count_nearby' column.
    Accidents outside any station's radius are still assigned to the nearest station.

    Args:
        stations_path: CSV path with station data (lat/long columns).
        accidents_path: CSV path with accident data (lat/long columns).
        output_stations_path: Output path for enriched stations CSV.
        output_accidents_path: Output path for assigned accidents CSV.
        radius_m: Radius in meters to consider an accident "nearby".
        station_id_col: Column name for station identifier.

    Returns:
        tuple: (output_stations_path, output_accidents_path)
    """
    stations = pd.read_csv(stations_path)
    accidents = pd.read_csv(accidents_path)
    stations = stations.dropna(subset=['lat', 'long']).reset_index(drop=True)
    accidents = accidents.dropna(subset=['lat', 'long']).reset_index(drop=True)

    stations_lat = np.radians(stations['lat'].values)
    stations_lng = np.radians(stations['long'].values)

    assigned_station_ids = []
    assigned_distances = []
    within_radius_flags = []

    for idx, acc in accidents.iterrows():
        lat_rad = math.radians(acc['lat'])
        lng_rad = math.radians(acc['long'])
        
        dists = _haversine_distance_m(lat_rad, lng_rad, stations_lat, stations_lng)
        # index de la station la plus proche
        nearest_idx = int(np.argmin(dists))
        nearest_dist = float(dists[nearest_idx])
        assigned_station_id = stations.iloc[nearest_idx].get(station_id_col, nearest_idx)

        assigned_station_ids.append(assigned_station_id)
        assigned_distances.append(nearest_dist)
        within_radius_flags.append(nearest_dist <= radius_m)

    accidents['assigned_station_id'] = assigned_station_ids
    accidents['distance_m'] = assigned_distances
    accidents['within_radius'] = within_radius_flags
    accidents['assigned_to_deconne'] = ~accidents['within_radius']

    counts = accidents[accidents['within_radius']].groupby('assigned_station_id').size().to_dict()
    stations['accident_count_nearby'] = stations[station_id_col].map(lambda s: int(counts.get(s, 0))) if station_id_col in stations.columns else stations.index.map(lambda i: int(counts.get(i, 0)))

    if output_stations_path is None:
        output_stations_path = os.path.join(os.path.dirname(stations_path), 'stations_with_accident_counts.csv')
    if output_accidents_path is None:
        output_accidents_path = os.path.join(os.path.dirname(accidents_path), 'accidents_assigned.csv')

    os.makedirs(os.path.dirname(output_stations_path), exist_ok=True)
    os.makedirs(os.path.dirname(output_accidents_path), exist_ok=True)
    stations.to_csv(output_stations_path, index=False)
    accidents.to_csv(output_accidents_path, index=False)

    return output_stations_path, output_accidents_path


def _haversine_distance_m(lat1, lon1, lat2_arr, lon2_arr):
    """Calculate haversine distance in meters between a point and arrays of points."""
    dlat = lat2_arr - lat1
    dlon = lon2_arr - lon1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2_arr) * np.sin(dlon / 2.0) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return 6371000.0 * c


def check_mongo_stations(mongo_uri="mongodb://mongo:27017/", db_name="VelovDB", collection_name="velov_stations", min_count=1):
    """
    Validate that MongoDB collection contains minimum document count.

    Raises:
        ValueError: If document count is below min_count.
    """
    client = MongoClient(mongo_uri, username="admin", password="admin")
    try:
        count = client[db_name][collection_name].count_documents({})
    finally:
        client.close()

    if count < min_count:
        raise ValueError(f"Mongo collection {db_name}.{collection_name} has {count} documents < {min_count}")

    return True


def save_stations_data_to_mongodb(csv_path, db_name, collection_name):
    """
    Load station data from CSV and insert into MongoDB with geospatial indexing.

    Args:
        csv_path: Path to the stations CSV file.
        db_name: Target MongoDB database name.
        collection_name: Target collection name.
    """
    client = MongoClient("mongodb://mongo:27017/", username="admin", password="admin")
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
        collection.delete_many({})
        collection.insert_many(records)
        collection.create_index([("location", "2dsphere")])

    client.close()
