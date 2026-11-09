from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
from sqlalchemy import create_engine

# --- CONFIGURATION ---
MYSQL_CONN_STRING = "mysql+pymysql://root:@host.docker.internal:3306/cryptodb"
SQLITE_PATH = "/opt/airflow/crypto_data.db"
SQLITE_CONN_STRING = f"sqlite:///{SQLITE_PATH}"

# --- FONCTIONS ETL ---

def extract_crypto_data():
    """Récupère le TOP 50 des cryptomonnaies depuis l'API CoinGecko."""
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': 50,
        'page': 1,
        'sparkline': False
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    print(f"Extraction réussie : {len(data)} cryptomonnaies récupérées")
    return data

def transform_data(**context):
    """Nettoyage et Transformation des données."""
    raw_data = context['ti'].xcom_pull(task_ids='extract_task')
    if not raw_data:
        raise ValueError("Erreur : Aucune donnée reçue")
        
    df = pd.DataFrame(raw_data)
    
    columns_to_keep = ['id', 'symbol', 'current_price', 'market_cap', 'market_cap_rank', 'last_updated']
    df = df[columns_to_keep]
    
    df['processed_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"Transformation effectuée à {df['processed_at'].iloc[0]}")
    return df.to_json(orient='records')

def load_data(**context):
    """Chargement dans MySQL et SQLite."""
    transformed_json = context['ti'].xcom_pull(task_ids='transform_task')
    if not transformed_json:
        raise ValueError("Erreur : Aucune donnée reçue")
    
    df = pd.read_json(transformed_json)
    timestamp_log = datetime.now().strftime("%H:%M:%S")
    
    try:
        mysql_engine = create_engine(MYSQL_CONN_STRING)
        df.to_sql('crypto_prices_recent', mysql_engine, if_exists='replace', index=False)
        print(f"[{timestamp_log}]  MySQL mis à jour")
    except Exception as e:
        print(f"[{timestamp_log}]  ERREUR MySQL : {e}")

    try:
        sqlite_engine = create_engine(SQLITE_CONN_STRING)
        df.to_sql('crypto_prices_recent', sqlite_engine, if_exists='replace', index=False)
        print(f"[{timestamp_log}]  SQLite mis à jour")
    except Exception as e:
        print(f"[{timestamp_log}]  ERREUR SQLite : {e}")

# --- DÉFINITION DU DAG ---
default_args = {
    'owner': 'Daouda',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    dag_id='etl_crypto_api_multi_db',
    default_args=default_args,
    description='ETL Crypto toutes les 2 minutes',
    start_date=datetime(2024, 1, 1),
    schedule='*/2 * * * *',      # Toutes les 2 minutes
    catchup=False,               # Ne pas rattraper les exécutions manquées
    max_active_runs=1,           # Une seule exécution à la fois
    is_paused_upon_creation=False,  # ← IMPORTANT: Démarre automatiquement!
    tags=['crypto', 'etl', 'mysql', 'sqlite']
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_crypto_data
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_data
    )

    extract_task >> transform_task >> load_task