# Velov Data Engineering Project

### Project [DATA Engineering](https://www.riccardotommasini.com/courses/dataeng-insa-ot/) provided by [INSA Lyon](https://www.insa-lyon.fr/).
### Students: Amadou SOW, Anta GUEYE, Menphis NONO

## Abstract

This project analyzes the correlation between bike station activity (Vélo'v) and historical bike accidents in the Lyon metropolitan area. The goal is to recommend safer routes by weighting distances with an accident risk score.

The project addresses the following questions:

1. Which areas have high bike traffic but also a high accident rate?
2. What is the safest route between two stations (minimizing accident risk)?
3. How does station availability fluctuate during accident-prone hours?

A detailed report and presentation poster are available in the `docs` folder.

## Datasets

1. **JCDecaux Vélo'v API**: Real-time data on bike availability and station status in Lyon.
   - Source: [Grand Lyon Open Data](https://data.grandlyon.com/geoserver/ogc/features/v1/collections/metropole-de-lyon:jcd_jcdecaux.jcdvelov/items).

2. **French National Accident Database**: Historical data on road accidents involving bicycles (department 69).
   - Source: [Data.gouv.fr / Koumoul](https://opendata.koumoul.com/data-fair/api/v1/datasets/accidents-velos/raw).

## Architecture

The project uses a modern data engineering stack:

- **Apache Airflow**: Orchestrates the ETL pipeline.
- **MongoDB**: Stores cleaned station data with accident counts.
- **Neo4j**: Graph database for stations and ROUTE relationships, using Graph Data Science (GDS) for shortest paths.
- **Docker Compose**: Manages all services (Airflow, MongoDB, Neo4j, etc.).
- **Jupyter Notebook**: Interactive analysis and graph visualization.

## Project Pipeline

### Ingestion
- Downloads historical accident CSV from open data platform.
- Fetches real-time Vélo'v station data from JCDecaux API with pagination.
- Persists raw data in CSV format in `data/landing` folder.

### Wrangling
- Cleans invalid coordinates and filters for Lyon area (Zipcode 69).
- Standardizes timestamps and formats coordinates as GeoJSON.
- Stores cleaned data in MongoDB for flexible querying.

### Production
- Builds a graph in Neo4j with stations as nodes.
- Creates ROUTE relationships using KNN (k=5) for nearest neighbors.
- Weights routes by distance and accident risk score.
- Computes top-10 stations with highest fluctuations and saves to MongoDB.

## Requirements

- Docker installed on your computer.
- API Access: JCDecaux API is open, with rate limiting implemented.
- Environment: Libraries and configs managed via `docker-compose.yml` and `requirements.txt`.

## How to Run

1. In the project root (where `docker-compose.yml` is located), run:
   ```
   docker compose up -d --build
   ```
   - `-d`: Run in background.
   - `--build`: Build images (needed for first run or changes).

2. Access services:
   - **Airflow (Orchestrator)**: http://localhost:8080/ (username: `airflow`, password: `airflow`)
     - Trigger the `velov_fluctuations_dag` to start the pipeline.
   - **Neo4j Browser (Graph DB)**: http://localhost:7474/browser/ (username: `neo4j`, password: `adminPass`)
   - **Mongo Express** (Optional): http://localhost:8081/ (if enabled)

## Analytics

### Airflow DAG: velov_fluctuations_dag
The main DAG (`dags/velov_fluctuations_dag.py`) orchestrates:
- Data ingestion from APIs and CSVs.
- Data cleaning and enrichment.
- Saving processed data to MongoDB.
- Building the station graph in Neo4j with risk-weighted routes.
- Computing and storing top-10 fluctuating stations.

### Jupyter Notebook: analytics.ipynb
Located in `notebooks/analytics.ipynb`, provides:
- Neo4j connection and graph queries.
- Visualization of station network using PyVis.
- Shortest path calculations (both distance and risk-weighted).
- Dijkstra algorithm via Neo4j GDS.
- Display of top-10 stations with highest fluctuations from MongoDB.

Run the notebook after the DAG completes to explore the data and visualize results.

## Key Features

- **Graph-Based Routing**: Safest paths minimizing accident risk.
- **Real-Time Insights**: Fluctuation analysis during peak hours.
- **Scalable Architecture**: Containerized with Docker for easy deployment.
- **Interactive Analysis**: Jupyter notebook for data exploration.
