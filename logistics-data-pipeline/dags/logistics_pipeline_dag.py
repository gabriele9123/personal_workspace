"""
Logistics Data Pipeline DAG
Orchestrates ETL for transport and logistics data
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import yaml
import logging

# Add scripts directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from extractors.citybikes_extractor import CityBikesExtractor
from extractors.flights_extractor import FlightsExtractor
from transformers.bikes_transformer import BikesTransformer
from transformers.flights_transformer import FlightsTransformer
from loaders.data_loader import DataLoader

# Load configuration
config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'gabriele_pascaretta',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': config['pipeline']['max_retries'],
    'retry_delay': timedelta(seconds=config['pipeline']['retry_delay']),
}

# Create DAG
dag = DAG(
    'logistics_data_pipeline',
    default_args=default_args,
    description='ETL pipeline for transport and logistics data',
    schedule_interval=config['pipeline']['schedule_interval'],
    catchup=config['pipeline']['catchup'],
    tags=['logistics', 'transport', 'etl'],
)


def extract_bikes(**context):
    """Extract bike-sharing data"""
    logger.info("Starting bike data extraction")
    
    extractor = CityBikesExtractor()
    network_ids = config['data_sources']['citybikes']['cities']
    
    data = extractor.extract_all_networks(network_ids)
    
    context['ti'].xcom_push(key='bikes_raw_data', value=data)
    logger.info(f"Extracted data for {len(data)} bike networks")


def extract_flights(**context):
    """Extract flight tracking data"""
    logger.info("Starting flight data extraction")
    
    extractor = FlightsExtractor()
    airports = config['data_sources']['opensky']['airports']
    
    data = extractor.extract_flights_for_airports(airports)
    
    context['ti'].xcom_push(key='flights_raw_data', value=data)
    
    total_flights = sum(len(flights) for flights in data.values())
    logger.info(f"Extracted {total_flights} flights")


def transform_bikes(**context):
    """Transform bike-sharing data"""
    logger.info("Starting bike data transformation")
    
    raw_data = context['ti'].xcom_pull(key='bikes_raw_data', task_ids='extract_bikes')
    
    if not raw_data:
        logger.warning("No bike data to transform")
        return
    
    transformer = BikesTransformer()
    df = transformer.transform(raw_data)
    
    context['ti'].xcom_push(key='bikes_transformed_data', value=df.to_dict('records'))
    logger.info(f"Transformed {len(df)} bike station records")


def transform_flights(**context):
    """Transform flight tracking data"""
    logger.info("Starting flight data transformation")
    
    raw_data = context['ti'].xcom_pull(key='flights_raw_data', task_ids='extract_flights')
    
    if not raw_data:
        logger.warning("No flight data to transform")
        return
    
    transformer = FlightsTransformer()
    df = transformer.transform(raw_data)
    
    context['ti'].xcom_push(key='flights_transformed_data', value=df.to_dict('records'))
    logger.info(f"Transformed {len(df)} flight records")


def load_bikes(**context):
    """Load bike data to database"""
    logger.info("Starting bike data loading")
    
    import pandas as pd
    
    data = context['ti'].xcom_pull(key='bikes_transformed_data', task_ids='transform_bikes')
    
    if not data:
        logger.warning("No bike data to load")
        return
    
    df = pd.DataFrame(data)
    
    db_path = config['database']['path']
    loader = DataLoader(db_path)
    
    count = loader.load_bikes(df)
    logger.info(f"Loaded {count} bike records to database")


def load_flights(**context):
    """Load flight data to database"""
    logger.info("Starting flight data loading")
    
    import pandas as pd
    
    data = context['ti'].xcom_pull(key='flights_transformed_data', task_ids='transform_flights')
    
    if not data:
        logger.warning("No flight data to load")
        return
    
    df = pd.DataFrame(data)
    
    db_path = config['database']['path']
    loader = DataLoader(db_path)
    
    count = loader.load_flights(df)
    logger.info(f"Loaded {count} flight records to database")


# Define tasks
extract_bikes_task = PythonOperator(
    task_id='extract_bikes',
    python_callable=extract_bikes,
    dag=dag,
)

extract_flights_task = PythonOperator(
    task_id='extract_flights',
    python_callable=extract_flights,
    dag=dag,
)

transform_bikes_task = PythonOperator(
    task_id='transform_bikes',
    python_callable=transform_bikes,
    dag=dag,
)

transform_flights_task = PythonOperator(
    task_id='transform_flights',
    python_callable=transform_flights,
    dag=dag,
)

load_bikes_task = PythonOperator(
    task_id='load_bikes',
    python_callable=load_bikes,
    dag=dag,
)

load_flights_task = PythonOperator(
    task_id='load_flights',
    python_callable=load_flights,
    dag=dag,
)

# Set task dependencies - parallel extraction and processing
extract_bikes_task >> transform_bikes_task >> load_bikes_task
extract_flights_task >> transform_flights_task >> load_flights_task
