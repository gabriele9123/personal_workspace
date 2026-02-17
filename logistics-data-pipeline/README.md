# Logistics Data Pipeline

A production-ready ETL data pipeline built with Apache Airflow that extracts real-time transport and logistics data, transforms it, and loads it into a database for analysis.

## 🎯 Project Overview

This project demonstrates:
- **Data Pipeline Architecture**: End-to-end ETL workflow
- **Apache Airflow**: DAG orchestration and scheduling
- **API Integration**: Multiple REST API data sources
- **Data Transformation**: Cleaning, validation, and enrichment
- **Database Operations**: SQLite data storage
- **Error Handling**: Robust retry logic and monitoring

## 🏗️ Architecture

```
┌──────────────┐     ┌──────────────┐
│  CityBikes   │     │  OpenSky     │
│     API      │     │  Network API │
└──────┬───────┘     └──────┬───────┘
       │                    │
       └────────┬───────────┘
                │ Extract (Parallel)
                ▼
       ┌─────────────────┐
       │   Transform     │
       │  - Clean data   │
       │  - Validate     │
       └────────┬────────┘
                │ Load
                ▼
       ┌─────────────────┐
       │    Database     │
       │    (SQLite)     │
       └─────────────────┘
```

## 📊 Data Sources

1. **CityBikes API** - Real-time bike-sharing station availability across European cities
2. **OpenSky Network** - Live flight tracking data for major airports

## 🛠️ Tech Stack

- Python 3.9+
- Apache Airflow 2.8+
- SQLite
- Pandas (Data transformation)
- SQLAlchemy (Database ORM)
- Requests (API calls)

## 📁 Project Structure

```
logistics-data-pipeline/
├── dags/
│   └── logistics_pipeline_dag.py    # Airflow DAG definition
├── scripts/
│   ├── extractors/
│   │   ├── base_extractor.py        # Base API extractor
│   │   ├── citybikes_extractor.py   # Bike data extraction
│   │   └── flights_extractor.py     # Flight data extraction
│   ├── transformers/
│   │   ├── bikes_transformer.py     # Bike data transformation
│   │   └── flights_transformer.py   # Flight data transformation
│   └── loaders/
│       ├── database_schema.py       # Database models
│       └── data_loader.py           # Data loading logic
├── config/
│   └── config.yaml                  # Configuration settings
├── data/
│   └── logistics.db                 # SQLite database (created at runtime)
├── tests/
│   └── test_pipeline.py             # Unit tests
├── requirements.txt                 # Python dependencies
└── README.md                        # This file
```

## 🚀 Getting Started

### Prerequisites

- Python 3.9+
- pip
- virtualenv (recommended)

### Installation

1. **Navigate to project directory**
   ```bash
   cd logistics-data-pipeline
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

### Running the Pipeline

#### Option 1: Run with Airflow

```bash
# Initialize Airflow database
export AIRFLOW_HOME=$(pwd)
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --password admin \
    --firstname Gabriele \
    --lastname Pascaretta \
    --role Admin \
    --email gabriele.pascaretta@gmail.com

# Start webserver (Terminal 1)
airflow webserver --port 8080

# Start scheduler (Terminal 2)
airflow scheduler
```

Access Airflow UI at `http://localhost:8080`

#### Option 2: Test Individual Components

```bash
# Test bike extraction
python -c "from scripts.extractors.citybikes_extractor import CityBikesExtractor; e = CityBikesExtractor(); print(e.extract_network('bikemi'))"

# Run unit tests
pytest tests/ -v
```

## 📈 Usage

1. Access Airflow UI at `http://localhost:8080`
2. Find the `logistics_data_pipeline` DAG
3. Toggle it ON to enable scheduling (runs every 30 minutes)
4. Click "Trigger DAG" to run immediately
5. Monitor execution in the Graph or Tree view

## 🔍 Data Schema

### Bike Stations Table

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key |
| network_id | TEXT | Bike network identifier |
| network_name | TEXT | Network name |
| city | TEXT | City name |
| country | TEXT | Country code |
| station_id | TEXT | Station identifier |
| station_name | TEXT | Station name |
| latitude | REAL | Latitude coordinate |
| longitude | REAL | Longitude coordinate |
| free_bikes | INTEGER | Available bikes |
| empty_slots | INTEGER | Empty docking slots |
| total_slots | INTEGER | Total capacity |
| timestamp | DATETIME | Data timestamp |
| extracted_at | DATETIME | Extraction timestamp |

### Flights Table

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key |
| airport_code | TEXT | Airport ICAO code |
| icao24 | TEXT | Aircraft identifier |
| callsign | TEXT | Flight callsign |
| origin_country | TEXT | Origin country |
| latitude | REAL | Current latitude |
| longitude | REAL | Current longitude |
| altitude | REAL | Altitude (meters) |
| on_ground | BOOLEAN | On ground status |
| velocity | REAL | Speed (m/s) |
| heading | REAL | Direction (degrees) |
| timestamp | DATETIME | Data timestamp |
| extracted_at | DATETIME | Extraction timestamp |

## 📊 Example Queries

```sql
-- Latest bike availability by city
SELECT city, SUM(free_bikes) as total_bikes, SUM(empty_slots) as total_slots
FROM bike_stations
WHERE timestamp = (SELECT MAX(timestamp) FROM bike_stations)
GROUP BY city;

-- Active flights by airport
SELECT airport_code, COUNT(*) as flight_count, AVG(altitude) as avg_altitude
FROM flights
WHERE on_ground = 0 AND timestamp >= datetime('now', '-1 hour')
GROUP BY airport_code;

-- Bike station utilization rate
SELECT 
    station_name,
    city,
    ROUND(CAST(free_bikes AS FLOAT) / total_slots * 100, 2) as utilization_pct
FROM bike_stations
WHERE timestamp = (SELECT MAX(timestamp) FROM bike_stations)
ORDER BY utilization_pct DESC
LIMIT 10;
```

## 🧪 Testing

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest --cov=scripts tests/
```

## 📝 Key Skills Demonstrated

- **ETL Pipeline Design**: Scalable data pipeline architecture
- **Workflow Orchestration**: Airflow DAGs, task dependencies
- **API Integration**: REST API consumption with retry logic
- **Data Quality**: Validation and cleaning
- **Database Design**: Relational schema design
- **Best Practices**: Logging, error handling, testing

## 🚧 Future Enhancements

- Add data visualization dashboard
- Implement data retention policies
- Add email/Slack notifications
- Integrate with cloud storage (AWS S3)
- Add data quality metrics
- Deploy to production (Docker/Kubernetes)

## 👤 Author

**Gabriele Pascaretta**
- LinkedIn: [gabriele-pascaretta](https://www.linkedin.com/in/gabriele-pascaretta/)
- GitHub: [@gabriele9123](https://github.com/gabriele9123)
- Email: gabriele.pascaretta@gmail.com

---

*Built as part of a portfolio demonstrating data engineering capabilities relevant to supply chain and logistics operations.*
