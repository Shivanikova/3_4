"""
### DAG documntation
This is a simple ETL data pipeline example which extract rates data from API
 and load it into postgresql.
"""

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.hooks.base import BaseHook


import decimal
from time import localtime, strftime
from datetime import datetime
import requests
import psycopg2

default_args = {
    "owner": "airflow",
    'start_date': days_ago(1)
}


variables = Variable.set(key="currency_load_variables",
                         value={"table_name": "rates_BTC_new",
                                "RUB": "RUB",
                                "USD": "USD",                              
                                "BTC": "BTC", 
                                "EUR": "EUR",
                                "connection_name":"my_db_conn",
                                "url_base":"https://api.exchangerate.host/"},
                         serialize_json=True)
dag_variables = Variable.get("currency_load_variables", deserialize_json=True)

def get_conn_credentials(conn_id) -> BaseHook.get_connection:
    """
    Function returns dictionary with connection credentials

    :param conn_id: str with airflow connection id
    :return: Connection
    """
    conn = BaseHook.get_connection(conn_id)
    return conn

"""
Run uploading code from exchangerate.host API
"""
def import_codes(**kwargs):
# Parameters
    hist_date = "latest"
    url = dag_variables.get('url_base') + hist_date
    ingest_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
    
    try:
        response = requests.get(url,
   
             params={'base': dag_variables.get('BTC')})
    except Exception as err:
        print(f'Error occured: {err}')
        return
    data = response.json()
    rate_date = data['date']

    value_BTC_to_USD = str(decimal.Decimal(data['rates']['USD']))[:20]
    value_BTC_to_EUR = str(decimal.Decimal(data['rates']['EUR']))[:20]
    value_BTC_to_RUB = str(decimal.Decimal(data['rates']['RUB']))[:20]
    
    ti = kwargs['task_instance']

    ti.xcom_push(key='results', value={"rate_date":rate_date, "value_BTC_to_USD":value_BTC_to_USD, "value_BTC_to_EUR":value_BTC_to_EUR, "value_BTC_to_RUB":value_BTC_to_RUB, "ingest_datetime":ingest_datetime })

def insert_data(**kwargs):
    task_instance = kwargs['ti']
    results = task_instance.xcom_pull(key='results', task_ids='import_rates')
    
    print("rate_date: ", results["rate_date"])
    print("value_BTC_to_USD: ", results["value_BTC_to_USD"])
    print("value_BTC_to_EUR: ", results["value_BTC_to_EUR"])
    print("value_BTC_to_RUB: ", results["value_BTC_to_RUB"])
    
    ingest_datetime = strftime("%Y-%m-%d %H:%M:%S", localtime())
    
    pg_conn = get_conn_credentials(dag_variables.get('connection_name'))
    pg_hostname, pg_port, pg_username, pg_pass, pg_db = pg_conn.host, pg_conn.port, pg_conn.login, pg_conn.password, pg_conn.schema
    conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)
    
    cursor = conn.cursor()

#Для сохрения курсов валют в postgresql создается таблица rates_BTC_new в Postgres, если ранее не создана
    cursor.execute(f"CREATE TABLE if not exists rates_BTC_new (ingest_datetime timestamp, rate_date date, rate_base varchar(20),rate_target varchar(20), value_ numeric (23,5));")
    conn.commit()

    cursor.execute(f"INSERT INTO {dag_variables.get('table_name')} (ingest_datetime, rate_date, rate_base, rate_target, value_) valueS('{ingest_datetime}','{results['rate_date']}', '{dag_variables.get('BTC')}', '{dag_variables.get('USD')}','{results['value_BTC_to_USD']}');")
    cursor.execute(f"INSERT INTO {dag_variables.get('table_name')} (ingest_datetime, rate_date, rate_base, rate_target, value_) valueS('{ingest_datetime}','{results['rate_date']}', '{dag_variables.get('BTC')}', '{dag_variables.get('EUR')}','{results['value_BTC_to_EUR']}');")
    cursor.execute(f"INSERT INTO {dag_variables.get('table_name')} (ingest_datetime, rate_date, rate_base, rate_target, value_) valueS('{ingest_datetime}','{results['rate_date']}', '{dag_variables.get('BTC')}', '{dag_variables.get('RUB')}','{results['value_BTC_to_RUB']}');")

#    cursor.execute(f"INSERT INTO {dag_variables.get('table_name')} (ingest_datetime, rate_date, RUB, USD, EUR, BTC, value_USD_to_RUB, value_EUR_to_RUB, value_BTC_to_RUB) valueS('{ingest_datetime}','{results['rate_date']}', #'{dag_variables.get('RUB')}', '{dag_variables.get('USD')}','{dag_variables.get('EUR')}', '{dag_variables.get('BTC')}', '{results['value_BTC_to_USD']}', #'{results['value_BCT_to_EUR']}', #'{results['value_BTC_to_RUB']}');")


    conn.commit()

    cursor.close()
    conn.close()

with DAG(dag_id = "calc-rates2", schedule_interval = "*/10 * * * *",
    default_args = default_args, tags=["1T", "test"], catchup = False) as dag:
        
    dag.doc_md = __doc__

    bash_operator_task = BashOperator(task_id = 'good_morning',
                    bash_command = "echo 'Good morning, my diggers!'")
    
    import_rates_from_api = PythonOperator(task_id = "import_rates",
                                                python_callable = import_codes)
    
    insert_rates_to_pg = PythonOperator(task_id="insert_data",
                                                python_callable = insert_data) 

bash_operator_task >> import_rates_from_api >> insert_rates_to_pg 