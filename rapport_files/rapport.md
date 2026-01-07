# Velov Data Engineering Project: Comprehensive Report

## Introduction

The Velov Data Engineering Project is an end-to-end data pipeline and analytics solution designed to analyze the relationship between bike station activity (Vélo'v) and historical bike accidents in the Lyon metropolitan area. The primary objective is to provide actionable insights for safer urban cycling, including identifying high-risk areas, recommending the safest routes, and understanding station availability. This project leverages a modern data engineering stack, including Apache Airflow, MongoDB, Neo4j, Docker, and Jupyter Notebooks, to orchestrate, process, and analyze large-scale real-time and historical datasets.

## Project Motivation and Objectives

Urban cycling is increasingly popular, but safety remains a major concern. By integrating real-time bike station data with historical accident records, this project aims to:
- Identify areas with both high bike traffic and high accident rates, helping city planners and cyclists make informed decisions.
- Recommend the safest routes between stations by weighting paths with accident risk scores, not just distance.
- Analyze how station availability fluctuates during hours when accidents are most likely, providing insights for both users and operators.

## Data Sources

1. **JCDecaux Vélo'v API**: Provides real-time data on bike availability and station status across Lyon. This data is ingested regularly to capture the dynamic nature of bike usage.
2. **French National Accident Database**: Offers detailed historical records of road accidents involving bicycles in the Lyon area (department 69). This dataset is crucial for risk assessment and spatial analysis.

## Architecture Overview

The project is architected for scalability, reproducibility, and interactive analysis:
- **Apache Airflow** orchestrates the entire ETL pipeline, ensuring reliable and scheduled data processing.
- **MongoDB** stores cleaned and enriched station data, including accident counts and fluctuation metrics, enabling flexible and fast queries.
- **Neo4j** serves as a graph database, modeling stations and their connections (ROUTE relationships). It supports advanced graph algorithms, such as Dijkstra's shortest path, weighted by accident risk.
- **Docker Compose** manages all services, ensuring consistent environments and easy deployment.
- **Jupyter Notebook** provides an interactive platform for data exploration, visualization, and answering key analytical questions.

## Data Pipeline Details

### Ingestion
- The pipeline downloads historical accident data from open data platforms and fetches real-time Vélo'v station data from the JCDecaux API, handling pagination and rate limits.
- Raw data is stored in CSV format in the `data/landing` directory for traceability and reproducibility.

### Wrangling
- Accident data is cleaned by filtering for the Lyon area, removing invalid coordinates, and deduplicating records. Only relevant columns are retained, and the output is saved as `accidents_clean_lyon.csv`.
- Vélo'v station data is cleaned by converting types, filtering out closed stations and invalid coordinates, and standardizing timestamps. Cleaned data is stored in MongoDB for further analysis.

### Production
- A graph is constructed in Neo4j, where each station is a node and ROUTE relationships are created using a k-nearest neighbors (KNN) approach (k=5). Each route is weighted by both distance and accident risk, enabling risk-aware routing.
- The pipeline computes the top-10 stations with the highest fluctuations in bike availability, storing these results in MongoDB for real-time insights.

## Orchestration with Airflow

Two main DAGs orchestrate the pipeline:
- **global_DAG.py**: Handles the end-to-end ETL process, from ingestion and cleaning to graph construction and risk computation. It ensures robust error handling and logging.
- **velov_fluctuations_dag.py**: Focuses on computing and verifying the top-10 fluctuating stations, using custom Python operators that interact with the wrangling library and MongoDB.

## Data Wrangling and Enrichment

The wrangling module is central to data quality and enrichment. It:
- Cleans accident and station data, ensuring only valid, relevant records are processed.
- Aggregates accident counts per station, enabling risk scoring.
- Computes fluctuation metrics by summing absolute differences in available bikes over time, highlighting stations with the most dynamic usage patterns.

## Key Analytical Questions and Insights

### 1. Which areas have high bike traffic but also a high accident rate?
By joining real-time station usage data with historical accident records, the project identifies stations and areas where high traffic coincides with elevated accident risk. These hotspots are visualized using interactive network graphs, helping both cyclists and city planners target interventions and improve safety.

### 2. What is the safest route between two stations (minimizing accident risk)?
Using Neo4j's Graph Data Science library, the project computes the safest paths between any two stations. Instead of simply minimizing distance, the Dijkstra algorithm is applied with accident risk as the edge weight. This approach provides cyclists with routes that balance efficiency and safety, and the results are visualized interactively in the notebook.

### 3. How does station availability fluctuate during accident-prone hours?
The pipeline analyzes fluctuations in bike availability, especially during periods with historically high accident rates. By identifying the top-10 most dynamic stations, the project offers insights into demand patterns and potential stress points in the network. These findings can inform operational decisions and user guidance.

## Interactive Analytics and Visualization

The Jupyter Notebook (`analytics.ipynb`) serves as the main interface for exploring the data and results. It includes:
- Connection to Neo4j for running graph queries and visualizing the station network.
- Calculation and visualization of shortest and safest paths between stations.
- Display of top-10 fluctuating stations and accident risk distributions using MongoDB and advanced plotting libraries.
- Enhanced visualizations (e.g., PyVis, Seaborn) for intuitive understanding of complex relationships.

## Deployment and Usage

The entire project is containerized for ease of deployment. After building and starting the services with Docker Compose, users can trigger the Airflow DAGs, explore the Neo4j graph, and analyze results in the Jupyter Notebook. All configurations and dependencies are managed via `docker-compose.yml` and `requirements.txt`.

## Conclusion

The Velov Data Engineering Project demonstrates the power of integrating real-time and historical data with modern data engineering tools to address urban mobility and safety challenges. By providing actionable insights and interactive analytics, it supports safer cycling and smarter city planning in Lyon. The modular, scalable architecture ensures that the solution can be extended to other cities or datasets in the future.
