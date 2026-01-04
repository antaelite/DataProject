# DataEng 2025
### Project [DATA Engineering](https://www.riccardotommasini.com/courses/dataeng-insa-ot/) is provided by [INSA Lyon](https://www.insa-lyon.fr/).
### Students : Amadou Alassane SOW, Anta GUEYE, Menphis NONO
# Abstract

Our project aims to analyze the correlation between bike station activity (Vélo'v) and historical bike accidents in the Lyon metropolitan area. The goal is to recommend safer routes by weighting distances with an accident risk score.
Here are the different questions we would like our database to answer:

1) Which areas have high bike traffic but also a high accident rate?
2) What is the safest route between two stations (minimizing accident risk)?
3) How does station availability fluctuate during accident-prone hours?

Detailed report and the presentation poster are available in the docs folder.
# Datasets
1) JCDecaux Vélo'v API: Real-time data on bike availability and station status in Lyon.

    * Source: [Grand Lyon Open Data](https://data.grandlyon.com/geoserver/ogc/features/v1/collections/metropole-de-lyon:jcd_jcdecaux.jcdvelov/items).

2) French National Accident Database: Historical data on road accidents involving bicycles (department 69).

    * Source: [Data.gouv.fr / Koumoul](https://opendata.koumoul.com/data-fair/api/v1/datasets/accidents-velos/raw).

# Project steps
## Ingestion
We implemented a dynamic ingestion pipeline.

For Accidents, we download the historical CSV file directly from the open data platform.

For Vélo'v Stations, we fetch real-time data from the JCDecaux API using a paginated script to ensure we capture all stations. The ingestion part persists the raw data in CSV format in the data/landing folder.

## Wrangling
The raw data required significant cleaning and enrichment:

### Cleaning:
We removed invalid coordinates (lat/long), filtered data strictly for the Lyon area (Zipcode 69), and standardized timestamps.

### Enrichment: 
We formatted the station coordinates into GeoJSON objects to facilitate spatial queries.

### Staging: 
Instead of a traditional SQL database, we chose MongoDB to store the cleaned station data. This allows for flexible schema handling and efficient storage of JSON-like documents coming from the API.

## Production
To answer our routing and safety questions, we constructed a Graph Database using Neo4j.

* Nodes: Represent Vélo'v stations.

* Relationships (:ROUTE): Represent the path between stations. We used a K-Nearest Neighbors (KNN) algorithm (k=5) to connect each station to its closest neighbors.

* Risk Calculation: Each route is weighted not just by distance, but by a "risk score" calculated from the historical accident density in that sector.

## Requirements
You need Docker installed on your computer.

**API Access:** The JCDecaux API used is open, but we have implemented safe-guards (rate limiting) in our ingestion script to respect their infrastructure.

**Environment:** Required libraries and configurations are managed via the docker-compose.yml and requirements.txt.
## How to run
In the root folder of the project where the docker-compose.yml file is located, run: "docker compose up -d --build".

* -d is to run in background and free the terminal.

* --build is to build the solution, useful for the first run or when you make changes in config.
### Accessing the Services:

**Airflow (Orchestrator):** http://localhost:8080/

***Credentials:*** username="airflow", password="airflow"

Trigger the realtime_velov_dag to start the pipeline.

**Neo4j Browser (Graph DB):** http://localhost:7474/browser/

***Credentials:*** username="neo4j", password="adminPass"

**Mongo Express** (Optional): If enabled, available at http://localhost:8081/

## Analytics
We provided a set of Cypher queries to demonstrate the capabilities of our graph database. You can run them directly in the Neo4j Browser to visualize:

* The network of stations.

* The "safest" paths vs. the "shortest" paths.

* Clusters of high-risk stations
