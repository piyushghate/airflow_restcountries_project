from datetime import datetime, timedelta
import requests
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator


### Python functions

def get_countries():
    url = "https://restcountries.com/v3.1/all"
    # Make the GET request
    response = requests.get(url)
    if response.status_code == 200:
        # Parse the JSON data in the response
        country_data = response.json()

        # Print the country data (or you can perform other operations here)
        print(f"Number of countries in response:: {len(country_data)}")
        print(f"Type of the response:: {type(country_data)}")
        df = convert_to_df(country_data)
        print(f"Schema of DF:: {df.dtypes}")
    else:
        # Print an error message if the request was not successful
        print(f"Error: {response.status_code}")


def convert_to_df(list_of_countries) -> pd.DataFrame:
    df = pd.DataFrame(list_of_countries)
    return df


default_args = {
    'owners': 'Piyush',
    'retries': 5,
    'retry_delays': timedelta(minutes=2)
}

with DAG(
        dag_id='fetch_data_v02',
        default_args=default_args,
        description='To fetch the country data from RestCountries api',
        start_date=datetime.now(),
        schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='fetch_all_country',
        python_callable=get_countries
    )

    task1
