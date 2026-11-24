# Airline Reporting Carrier On-Time Performance - Apache Spark

An interactive Big Data analytics platform for exploring US domestic air traffic, analyzing flight delays, evaluating airline performance, and investigating airport network structures.

Developed using Apache Spark and PySpark, the project processes more than **6.3 million flight records** from the 2013 US Airline On-Time Performance dataset, integrating distributed data processing, machine learning, graph analytics, and geospatial visualization into a single interactive application.

Developed as part of the **Modelli e tecniche per big data** course at the University of Calabria.

## Project Overview

Air transportation generates large volumes of operational data, providing valuable insights into airline performance, airport congestion, flight delays, and network connectivity.

FlightScope processes the complete 2013 US domestic flight dataset using Apache Spark, without sampling or reducing the original dataset for the main analysis.

The platform combines four analytical components:

- **Big Data Analytics:** distributed data processing and large-scale aggregation using PySpark.
- **Machine Learning:** flight cancellation classification, departure delay regression, and KMeans clustering.
- **Graph Analytics:** airport network analysis using GraphFrames.
- **Interactive Visualization:** geospatial maps, analytical dashboards, and predictive interfaces built with Streamlit and PyDeck.

## Key Features

- Processing of over **6.3 million flight records** using Apache Spark.
- Comprehensive analysis of airlines, airports, cities, and flight routes.
- Interactive geospatial visualization of airport traffic and flight connections.
- Machine learning models for cancellation risk classification and delay estimation.
- Graph-based analysis of airport connectivity and network structure.
- Interactive airport removal simulation to investigate network resilience.

## Dataset

**Source:** [US Bureau of Transportation Statistics (BTS)](https://www.transtats.bts.gov/)

**Dataset:** Airline Reporting Carrier On-Time Performance — 2013.

The dataset consists of 12 monthly CSV files containing more than 6.3 million domestic flight records.

Each record describes an individual flight and includes:

| Category | Data |
|---|---|
| Flight information | Scheduled and actual departure and arrival times |
| Airports | Origin and destination airport codes |
| Airlines | Reporting carrier information |
| Delays | Departure and arrival delays |
| Delay causes | Weather, NAS, security, carrier, and late aircraft |
| Flight status | Cancellations and diversions |

The dataset is used to investigate operational patterns and relationships across airlines, airports, routes, and time periods.

## Data Preprocessing

Before analytical processing, the original CSV files undergo a dedicated preprocessing stage.

Two scripts are responsible for data inspection and cleaning:

**1. `AnalisiDatasets.py`**

- Loads and inspects the 12 monthly CSV files.
- Analyzes column structures and data types.
- Generates reports on missing values and data anomalies.
- Identifies malformed columns, including the anomalous `_c109` field.

**2. `Preprocessing.py`**

- Removes entirely null columns.
- Eliminates empty rows.
- Removes malformed columns identified during inspection.
- Produces 12 cleaned CSV files ready for distributed processing.

During the initial application setup, the cleaned data is converted into Parquet format and persisted using Spark's `MEMORY_ONLY` storage level to accelerate subsequent analytical operations.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Main programming language |
| Apache Spark | Distributed data processing |
| PySpark | Data transformations and analytical workflows |
| Spark SQL | Distributed SQL queries and aggregations |
| Spark MLlib | Machine learning models |
| GraphFrames | Distributed graph processing and network analysis |
| Streamlit | Interactive dashboard and application interface |
| PyDeck | Geospatial and network visualization |
| Pandas | Conversion and preparation of analytical results |

## Application Architecture

The application follows a modular architecture that separates data preparation, analytical processing, and user interaction.

```text
             2013 Flight Dataset
              12 Monthly CSVs
                     |
                     v
              Data Inspection
              AnalisiDatasets.py
                     |
                     v
              Data Cleaning
              Preprocessing.py
                     |
                     v
              Cleaned CSV Files
                     |
                     v
              Apache Spark
              PySpark / Spark SQL
                     |
          +----------+----------+
          |          |          |
          v          v          v
       Analytics   Machine    Graph
       & EDA       Learning   Analytics
          |          |          |
          +----------+----------+
                     |
                     v
             Streamlit Dashboard
                     |
                     v
          Interactive Visualization
              and Predictions
```

## Interactive Dashboard

The Streamlit application provides several analytical sections, each focusing on a different aspect of US domestic air traffic.

### 1. Home — Global Overview

Provides a general overview of the dataset through key performance indicators and interactive visualizations.

Main features:

- Total number of flights.
- Average flight delay.
- Flight cancellation percentage.
- Top airlines by traffic volume, punctuality, and cancellations.
- Airports with the highest average delays.
- Most frequently used flight routes.
- Interactive airport map, with marker size representing flight volume and color representing average delay.

### 2. Airport Analytics

Explores the operational performance of individual airports.

Includes:

- Flight traffic statistics.
- Delay distributions.
- Comparison with national averages.
- Airport-level operational indicators.

### 3. Airline Performance

Compares airline operational performance and reliability.

Includes:

- On-time performance.
- Cancellation rates.
- Flight traffic volume.
- Temporal performance trends.

### 4. City Analytics

Provides aggregated flight statistics by origin and destination city, allowing comparisons between urban areas and their associated airport traffic.

### 5. Delays and Cancellations

Investigates flight delays and their underlying causes.

Includes:

- Weather-related delays.
- National Aviation System (NAS) delays.
- Security-related delays.
- Carrier-related delays.
- Late aircraft delays.
- Monthly and weekly trends.
- Delay distributions by departure time.

### 6. Route Analytics

Explores the structure and operational characteristics of flight routes.

The section includes:

- Most frequently traveled routes.
- Routes with the highest delay levels.
- Interactive route maps, with edge thickness proportional to flight volume.
- Detailed route statistics, including operating airlines, peak months, and bidirectional traffic.

Selecting an individual route provides a more detailed view of its operational characteristics.

### 7. Predictive Analytics — Machine Learning

Three machine learning models are implemented using Apache Spark MLlib.

**Random Forest — Flight Cancellation Classification**

Predicts the probability of flight cancellation based on flight characteristics.

- Classification task.
- AUC: **0.961**.
- Class imbalance addressed through inverse class weighting.
- The dataset contains approximately 96,000 cancelled flights out of more than 6.3 million records.

**Linear Regression — Departure Delay Estimation**

Estimates departure delay in minutes using a regression model.

- Regression task.
- Feature normalization using `StandardScaler`.
- Predicts the expected departure delay based on flight characteristics.

**KMeans — Flight Clustering**

Groups flights into four clusters based on their operational characteristics.

The resulting clusters represent different flight patterns:

- Short-distance, predominantly punctual flights.
- Medium-distance flights with moderate delays.
- Long-distance flights with more significant delays.
- High-variability and anomalous flight patterns.

**Interactive Prediction Interface**

The dashboard allows users to enter flight parameters, including distance, scheduled departure time, month, and day.

The application returns:

- Estimated cancellation probability.
- Predicted departure delay.
- Combined operational risk level.

### 8. Airport Network — Graph Analytics

The airport system is modeled as a directed graph using GraphFrames:

- **Nodes:** airports.
- **Edges:** directed flight routes.

Several graph algorithms and structural metrics are implemented.

| Algorithm / Metric | Purpose |
|---|---|
| PageRank | Identifies the most influential airports in the network |
| Connected Components | Identifies connected groups of airports |
| Strongly Connected Components | Analyzes mutual reachability within the directed network |
| Label Propagation | Identifies communities and regional airport clusters |
| In-Degree / Out-Degree | Measures incoming and outgoing flight connectivity |
| Triangle Count | Evaluates local connectivity and network structure |

The analysis identifies major airport hubs, including Atlanta (ATL), Chicago O'Hare (ORD), and Dallas/Fort Worth (DFW).

**Network Resilience Simulation**

An additional feature allows users to simulate the removal of an airport and observe the resulting changes in network connectivity.

This makes it possible to investigate how the closure of an airport may affect the structure of the flight network and its regional connectivity.

## Performance and Execution

The application uses Apache Spark in local execution mode.

The Spark session is configured with:

- 18 GB driver memory.
- 8 shuffle partitions.
- Automatic broadcast joins for tables up to 100 MB.
- Parquet conversion and in-memory persistence using `MEMORY_ONLY`.

These settings are part of the project's execution configuration and can be adapted to the available hardware.

## Technologies and Concepts

`Python` `Apache Spark` `PySpark` `Spark SQL` `MLlib` `GraphFrames` `Streamlit` `PyDeck` `Pandas` `Big Data Analytics` `Machine Learning` `Graph Analytics` `Data Visualization`

## Academic Context

Developed as part of the **Modelli e tecniche per big data** course at the University of Calabria.

The project explores the integration of distributed data processing, machine learning, graph algorithms, and interactive visualization within a unified analytical application.
