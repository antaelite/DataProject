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

# ---------------------------------------------------------------------------
# Cross-collection helpers (for DBs where each snapshot is a collection)
# ---------------------------------------------------------------------------

def top_stations_across_collections(
    mongo_uri="mongodb://mongo:27017/",
    db_name="VelovRealtimeDB",
    collection_name_pattern=None,
    station_field="station_id",
    bikes_field="available_bikes",
    ts_field=None,
    top_n=10,
    username="admin",
    password="admin",
    collection_ts_extractor=None,
):
    """Parcourt toutes les collections d'une DB (optionnellement filtrées)
    et calcule, pour chaque station, la somme des variations absolues
    successives |a-b|+|b-c|... en utilisant les valeurs présentes dans chaque
    collection (chaque collection correspond à un snapshot de l'état).

    - collection_name_pattern : regex pour filtrer les noms de collections.
    - ts_field : si fourni, on cherchera ce champ dans les documents de chaque
      collection pour déterminer la date du snapshot (ex: 'last_update').
    - collection_ts_extractor : callable(name) -> timestamp (si nommage custom).

    Retourne un DataFrame trié desc. par `fluctuation_sum`.
    """
    import re

    client = MongoClient(mongo_uri, username=username, password=password)
    try:
        db = client[db_name]
        coll_names = db.list_collection_names()
        if collection_name_pattern:
            coll_names = [c for c in coll_names if re.search(collection_name_pattern, c)]

        records = []

        for idx, cname in enumerate(sorted(coll_names)):
            coll = db[cname]
            # projection: on récupère au moins station et bikes, et éventuellement ts_field
            proj = {station_field: 1, bikes_field: 1, "_id": 0}
            if ts_field:
                proj[ts_field] = 1

            docs = list(coll.find({}, proj))
            if not docs:
                # collection vide -> skip
                continue

            # Déterminer timestamp du snapshot (essayer dans les docs, sinon du nom, sinon fallback ordinal)
            collection_ts = None
            if ts_field:
                for d in docs:
                    if ts_field in d and d[ts_field] is not None:
                        try:
                            collection_ts = pd.to_datetime(d[ts_field], errors="coerce")
                        except Exception:
                            collection_ts = None
                        if pd.notnull(collection_ts):
                            break

            if collection_ts is None and collection_ts_extractor is not None:
                try:
                    collection_ts = pd.to_datetime(collection_ts_extractor(cname))
                except Exception:
                    collection_ts = None

            if collection_ts is None:
                # Essayer d'extraire un timestamp numérique dans le nom (ex: 20251226_1829 ou 20251226182900)
                m = re.search(r"(\d{8,14})", cname)
                if m:
                    s = m.group(1)
                    for fmt in ("%Y%m%d%H%M%S", "%Y%m%d%H%M", "%Y%m%d"):
                        try:
                            collection_ts = pd.to_datetime(s, format=fmt)
                            break
                        except Exception:
                            continue

            if collection_ts is None:
                # fallback: utiliser un timestamp artificiel basé sur l'index pour garantir l'ordre
                collection_ts = pd.Timestamp("1970-01-01") + pd.Timedelta(seconds=idx)

            # Ajouter les enregistrements (station, bikes, collection_ts)
            for d in docs:
                sid = d.get(station_field)
                bikes = d.get(bikes_field)
                records.append({station_field: sid, bikes_field: bikes, "collection_ts": collection_ts})

        if not records:
            return pd.DataFrame(columns=[station_field, "fluctuation_sum"])

        df = pd.DataFrame.from_records(records)
        df = df.dropna(subset=[station_field, bikes_field, "collection_ts"])  # enlever docs incomplets
        df = df.sort_values([station_field, "collection_ts"])  # tri par station puis par date

        def station_fluct(s):
            diffs = s.astype(float).diff().abs()
            return diffs.sum()

        grouped = df.groupby(station_field)[bikes_field].apply(station_fluct)
        result = grouped.reset_index().rename(columns={bikes_field: "fluctuation_sum"})
        result = result.sort_values("fluctuation_sum", ascending=False).head(top_n)
        return result
    finally:
        client.close()


def compute_and_store_fluctuations_across_collections(
    mongo_uri="mongodb://mongo:27017/",
    db_name="VelovRealtimeDB",
    input_collection_pattern=None,
    output_collection="velov_station_fluctuations",
    station_field="station_id",
    bikes_field="available_bikes",
    ts_field=None,
    top_n=100,
    username="admin",
    password="admin",
    replace=True,
    collection_ts_extractor=None,
    station_metadata_collection='velov_stations',
):
    """Wrapper: calcule les fluctuations en parcourant les collections et écrit
    le top-N dans `output_collection` de la même DB.
    """
    df_top = top_stations_across_collections(
        mongo_uri=mongo_uri,
        db_name=db_name,
        collection_name_pattern=input_collection_pattern,
        station_field=station_field,
        bikes_field=bikes_field,
        ts_field=ts_field,
        top_n=top_n,
        username=username,
        password=password,
        collection_ts_extractor=collection_ts_extractor,
    )

    docs = df_top.assign(computed_at=pd.Timestamp.utcnow()).to_dict(orient="records")
    client = MongoClient(mongo_uri, username=username, password=password)
    ts_now = pd.Timestamp.utcnow()
    try:
        db = client[db_name]
        out_col = db[output_collection]

        if replace:
            out_col.delete_many({})

        # Enrichir les documents du top avec des métadonnées (nom, address, district)
        enriched_docs = []
        # préparer accès à la collection metadata si fournie
        meta_col = None
        if station_metadata_collection:
            try:
                meta_col = db[station_metadata_collection]
            except Exception:
                meta_col = None

        # Préparer liste des collections à parcourir si besoin pour fallback metadata
        try:
            snapshot_collections = db.list_collection_names()
        except Exception:
            snapshot_collections = []

        for d in docs:
            sid = d.get(station_field)
            meta = None
            # 1) essayer la collection metadata dédiée
            if meta_col is not None and sid is not None:
                meta = meta_col.find_one({station_field: sid}, {"name": 1, "address": 1, "district": 1, "_id": 0})
            # 2) fallback : chercher dans les snapshots si pas trouvé
            if not meta and sid is not None:
                for cname in snapshot_collections:
                    if input_collection_pattern and not __import__('re').search(input_collection_pattern, cname):
                        continue
                    sample = db[cname].find_one({station_field: sid}, {"name": 1, "address": 1, "district": 1, "_id": 0})
                    if sample:
                        meta = sample
                        break

            # attacher les champs (mettre None si absent)
            d["name"] = meta.get("name") if meta else None
            d["address"] = meta.get("address") if meta else None
            d["district"] = meta.get("district") if meta else None
            enriched_docs.append(d)

        if enriched_docs:
            res = out_col.insert_many(enriched_docs)
            print(f"Inserted {len(res.inserted_ids)} documents into {db_name}.{output_collection}")
        else:
            # s'assurer que la collection existe et y mettre un document méta pour qu'elle apparaisse
            try:
                db.create_collection(output_collection)
                print(f"Created collection {db_name}.{output_collection}")
            except Exception:
                pass
            out_col.update_one({"_meta": "no_data"}, {"$set": {"computed_at": ts_now}}, upsert=True)
            print(f"No results; added meta document into {db_name}.{output_collection}")

        # créer un index utile
        try:
            out_col.create_index([("fluctuation_sum", -1)])
            print(f"Created index on 'fluctuation_sum' in {db_name}.{output_collection}")
        except Exception as e:
            print(f"Could not create index on {db_name}.{output_collection}: {e}")
    finally:
        client.close()

    return df_top


def save_top10_to_realtime(
    mongo_uri="mongodb://mongo:27017/",
    db_name="VelovRealtimeDB",
    output_collection="velov_top10_fluctuations",
    station_field="station_id",
    bikes_field="available_bikes",
    ts_field=None,
    station_metadata_collection='velov_stations',
    username="admin",
    password="admin",
):
    """Convenience wrapper: calcule le TOP-10 et l'enregistre dans la DB
    `VelovDBRealtime` sous la collection `velov_top10_fluctuations`.
    """
    return compute_and_store_fluctuations_across_collections(
        mongo_uri=mongo_uri,
        db_name=db_name,
        input_collection_pattern=None,
        output_collection=output_collection,
        station_field=station_field,
        bikes_field=bikes_field,
        ts_field=ts_field,
        top_n=10,
        username=username,
        password=password,
        replace=True,
        station_metadata_collection=station_metadata_collection,
    )


def check_top10_collection_exists(
    mongo_uri="mongodb://mongo:27017/",
    db_name="VelovRealtimeDB",
    collection_name="velov_top10_fluctuations",
    min_docs=1,
    username="admin",
    password="admin",
):
    """Vérifie que la collection existe et contient au moins `min_docs` vrais
    documents (exclut le document méta `_meta`). Lève une exception en cas
    d'échec pour que la tâche Airflow signale un échec.
    """
    client = MongoClient(mongo_uri, username=username, password=password)
    try:
        db = client[db_name]
        coll_names = db.list_collection_names()
        if collection_name not in coll_names:
            raise ValueError(f"Collection '{collection_name}' not found in DB '{db_name}'")

        col = db[collection_name]
        # compter les documents réels (excluant ceux qui ont _meta)
        count_real = col.count_documents({"_meta": {"$exists": False}})
        if count_real < min_docs:
            raise ValueError(f"Collection '{db_name}.{collection_name}' has {count_real} real docs < {min_docs}")

        print(f"OK: {db_name}.{collection_name} contains {count_real} real documents")
        return True
    finally:
        client.close()


