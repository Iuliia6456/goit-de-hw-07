from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta
import random
import time


def create_table():
    sql = """
    CREATE TABLE IF NOT EXISTS medal_counts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        medal_type VARCHAR(255),
        count INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    mysql_hook = MySqlHook(mysql_conn_id='mysql_iuliia')
    mysql_hook.run(sql)

def choose_medal_type(**kwargs):
    medal = random.choice(['Bronze', 'Silver', 'Gold'])
    if medal == 'Bronze':
        return 'count_bronze'  
    elif medal == 'Silver':
        return 'count_silver'  
    else:
        return 'count_gold'  


def count_and_log_medals(medal_type):
    mysql_hook = MySqlHook(mysql_conn_id='mysql_iuliia')
    medal_query = f"SELECT COUNT(*) FROM athlete_event_results WHERE medal = '{medal_type}'"
    result = mysql_hook.get_first(medal_query)
    count = result[0] if result else 0
    insert_query = f"INSERT INTO medal_counts (medal_type, count, created_at) VALUES ('{medal_type}', {count}, CURRENT_TIMESTAMP)"
    mysql_hook.run(insert_query)

def wait_for_data():
    time.sleep(30)

def check_new_record():
    mysql_hook = MySqlHook(mysql_conn_id='mysql_iuliia')
    query = "SELECT created_at FROM medal_counts ORDER BY created_at DESC LIMIT 1"
    result = mysql_hook.get_first(query)
    if result:
        last_created_at = result[0]
        if last_created_at and (datetime.now() - last_created_at).total_seconds() < 30:
            return True
    return False

def log_medal_counts():
    mysql_hook = MySqlHook(mysql_conn_id='mysql_iuliia')
    results = mysql_hook.get_records('SELECT * FROM medal_counts')
    for row in results:
        print(row)

with DAG(
    'olympic_medal_dag',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=None,
    start_date=datetime(2024, 12, 8),
    catchup=False,
) as dag:

    task_create_table = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
    )

    task_choose_medal = BranchPythonOperator(
        task_id='choose_medal_type',
        python_callable=choose_medal_type,
        provide_context=True,
    )

    task_bronze = PythonOperator(
        task_id='count_bronze',
        python_callable=count_and_log_medals,
        op_args=['Bronze'],
    )

    task_silver = PythonOperator(
        task_id='count_silver',
        python_callable=count_and_log_medals,
        op_args=['Silver'],
    )

    task_gold = PythonOperator(
        task_id='count_gold',
        python_callable=count_and_log_medals,
        op_args=['Gold'],
    )

    # check_records = PythonOperator(
    #     task_id='check_records',
    #     python_callable=log_medal_counts
    # )

    task_delay = PythonOperator(
        task_id='wait_for_data',
        python_callable=wait_for_data,
        trigger_rule='one_success'
    )

    task_check_new_record = PythonSensor(
        task_id='check_new_record',
        python_callable=check_new_record,
        poke_interval=5,
        timeout=40,
        mode='poke',
        trigger_rule='one_success'
    )

    task_create_table >> task_choose_medal
    task_choose_medal >> [task_bronze, task_silver, task_gold]
    [task_bronze, task_silver, task_gold] >> task_delay >> task_check_new_record
