import numpy as np
import pandas as pd
import math
import logging

from neo4j import GraphDatabase
from pymongo import MongoClient

import libs.wrangling as wranglingLib


def mongo_to_neo4j_graph(k=5):
    """
    Build Neo4j graph from MongoDB station data with KNN edges.

    Args:
        k: Number of nearest neighbors for edge creation.

    Returns:
        tuple: (station_count, edge_count)
    """
    stations = load_stations_from_mongo()        
    edges = build_station_edges(stations, k=k)    

    driver = connect_neo4j()
    try:
        created_nodes = create_station_nodes(driver, stations)
        if created_nodes == 0:
            raise ValueError("No Station nodes were created in Neo4j; aborting.")

        created_edges = create_routes(driver, edges)
        if created_edges == 0:
            logging.getLogger(__name__).warning("No ROUTE relationships were created in Neo4j.")
    finally:
        close_neo4j(driver)

    return len(stations), len(edges)


def load_stations_from_mongo(
    mongo_uri="mongodb://mongo:27017/",
    username="admin",
    password="admin",
    db_name="VelovDB",
    collection_name="velov_stations"
) -> pd.DataFrame:
    """Load station data from MongoDB into a DataFrame."""
    client = MongoClient(mongo_uri, username=username, password=password, authSource="admin")
    col = client[db_name][collection_name]

    docs = list(col.find({}, {"_id": 0}))
    client.close()

    if not docs:
        raise ValueError(f"No documents found in {db_name}.{collection_name}")

    df = pd.DataFrame(docs)

    if "location" in df.columns:
        df["long"] = df["location"].apply(lambda x: float(x["coordinates"][0]) if x else None)
        df["lat"]  = df["location"].apply(lambda x: float(x["coordinates"][1]) if x else None)

    if "accident_count_nearby" not in df.columns:
        df["accident_count_nearby"] = 0

    # Vérifier présence de station_id
    if "station_id" not in df.columns:
        raise ValueError("Les documents Mongo doivent contenir la clé 'station_id'.")

    df = df.dropna(subset=["lat", "long"]).reset_index(drop=True)

    # Debug: logger info
    logging.getLogger(__name__).info(f"Loaded {len(df)} stations from {db_name}.{collection_name}")
    return df


def connect_neo4j(uri="bolt://neo4j:7687", user="neo4j", pwd="adminPass"):
    """Create and return a Neo4j driver connection."""
    return GraphDatabase.driver(uri, auth=(user, pwd))


def close_neo4j(driver):
    """Close Neo4j driver connection."""
    driver.close()


def run_query(driver, query, parameters=None):
    """Execute a Cypher query and return results as list of dicts."""
    with driver.session() as session:
        result = session.run(query, parameters)
        return result.data()


def create_station_nodes(driver, stations_df):
    """Create or update Station nodes in Neo4j from DataFrame."""
    rows = [{
        "station_id": r["station_id"],
        "lat": float(r["lat"]),
        "lng": float(r["long"]),
        "accidents": int(r.get("accident_count_nearby", 0)),
    } for _, r in stations_df.iterrows()]

    with driver.session() as s:
        s.run("""
        CREATE CONSTRAINT station_id_unique IF NOT EXISTS
        FOR (s:Station) REQUIRE s.station_id IS UNIQUE
        """)
        s.run("""
        UNWIND $rows AS row
        MERGE (s:Station {station_id: row.station_id})
        SET s.lat=row.lat, s.lng=row.lng, s.accidents=row.accidents
        """, rows=rows)


def create_routes(driver, edges):
    """Create ROUTE relationships between Station nodes in Neo4j."""
    with driver.session() as s:
        s.run("""
        UNWIND $edges AS e
        MATCH (a:Station {station_id: e.source})
        MATCH (b:Station {station_id: e.target})
        MERGE (a)-[r:ROUTE]->(b)
        SET r.distance_m=e.distance_m,
            r.risk=e.risk,
            r.risk_per_km=e.risk_per_km
        """, edges=edges)
   
def build_station_edges(stations_df, k=5, directed=True):
    """
    Build KNN edges between stations using haversine distance.

    Args:
        stations_df: DataFrame with station data including lat/long.
        k: Number of nearest neighbors per station.
        directed: If True, create bidirectional edges.

    Returns:
        list: Edge dictionaries with source, target, distance_m, risk, risk_per_km.
    """
    use_ckdtree = False
    try:
        from scipy.spatial import cKDTree
        use_ckdtree = True
    except ModuleNotFoundError:
        logging.getLogger(__name__).warning("scipy not available; using numpy fallback KNN.")

    coords = np.column_stack([stations_df["lat"].to_numpy(), stations_df["long"].to_numpy()])
    edges = []
    n = len(stations_df)
    if n == 0:
        return edges

    lat_rads = np.radians(stations_df["lat"].to_numpy())
    lon_rads = np.radians(stations_df["long"].to_numpy())

    if use_ckdtree:
        tree = cKDTree(coords)
        for i, a in stations_df.iterrows():
            _, idxs = tree.query(coords[i], k=min(k + 1, n))  
            neighbor_idxs = idxs[1:] if len(idxs) > 1 else []

            for j in neighbor_idxs:
                b = stations_df.iloc[int(j)]

                lat1 = math.radians(a["lat"])
                lon1 = math.radians(a["long"])
                lat2 = math.radians(b["lat"])
                lon2 = math.radians(b["long"])
                dlat = lat2 - lat1
                dlon = lon2 - lon1
                a_val = math.sin(dlat / 2.0) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2.0) ** 2
                c = 2 * math.atan2(math.sqrt(a_val), math.sqrt(1 - a_val))
                distance_m = 6371000.0 * c
                risk = (float(a.get("accident_count_nearby", 0)) + float(b.get("accident_count_nearby", 0))) / 2.0
                risk_per_km = risk / max(distance_m / 1000.0, 1e-6)

                edges.append({
                    "source": a["station_id"],
                    "target": b["station_id"],
                    "distance_m": float(distance_m),
                    "risk": float(risk),
                    "risk_per_km": float(risk_per_km),
                })

                if directed:
                    edges.append({
                        "source": b["station_id"],
                        "target": a["station_id"],
                        "distance_m": float(distance_m),
                        "risk": float(risk),
                        "risk_per_km": float(risk_per_km),
                    })

    else:
        for i, a in stations_df.iterrows():
            lat1 = math.radians(a["lat"])
            lon1 = math.radians(a["long"])
            dists = wranglingLib._haversine_distance_m(lat1, lon1, lat_rads, lon_rads)
            idxs = np.argsort(dists)
            neighbor_idxs = idxs[1: min(k + 1, n)]

            for j in neighbor_idxs:
                b = stations_df.iloc[int(j)]
                distance_m = float(dists[int(j)])
                risk = (float(a.get("accident_count_nearby", 0)) + float(b.get("accident_count_nearby", 0))) / 2.0
                risk_per_km = risk / max(distance_m / 1000.0, 1e-6)

                edges.append({
                    "source": a["station_id"],
                    "target": b["station_id"],
                    "distance_m": float(distance_m),
                    "risk": float(risk),
                    "risk_per_km": float(risk_per_km),
                })

                if directed:
                    edges.append({
                        "source": b["station_id"],
                        "target": a["station_id"],
                        "distance_m": float(distance_m),
                        "risk": float(risk),
                        "risk_per_km": float(risk_per_km),
                    })

    return edges


def check_neo4j_graph(driver_uri="bolt://neo4j:7687", user="neo4j", pwd="adminPass", min_nodes=1, min_edges=1):
    """
    Validate Neo4j graph has minimum Station nodes and ROUTE relationships.

    Raises:
        ValueError: If node or edge count is below minimum.
    """
    driver = connect_neo4j(uri=driver_uri, user=user, pwd=pwd)
    try:
        nodes_res = run_query(driver, "MATCH (s:Station) RETURN count(s) AS cnt")
        edges_res = run_query(driver, "MATCH ()-[r:ROUTE]->() RETURN count(r) AS cnt")
    finally:
        close_neo4j(driver)

    nodes = int(nodes_res[0].get("cnt", 0)) if nodes_res else 0
    edges = int(edges_res[0].get("cnt", 0)) if edges_res else 0

    if nodes < min_nodes:
        raise ValueError(f"Neo4j contains {nodes} Station nodes < {min_nodes}")
    if edges < min_edges:
        raise ValueError(f"Neo4j contains {edges} ROUTE relationships < {min_edges}")

    return {"nodes": nodes, "edges": edges}


def clear_database(driver):
    """Delete all nodes and relationships from Neo4j database."""
    with driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n")