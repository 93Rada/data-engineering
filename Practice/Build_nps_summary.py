from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
from datetime import timedelta

from airflow import AirflowException

import requests
import logging
import psycopg2

from airflow.exceptions import AirflowException

def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id = 'redshift_dev_db')
    return hook.get_conn().cursor()


def execSQL(**context):

    schema = context['params']['schema'] 
    table = context['params']['table']
    select_sql = context['params']['sql']

    logging.info(schema)
    logging.info(table)
    logging.info(select_sql)

    cur = get_Redshift_connection()

    sql = """DROP TABLE IF EXISTS {schema}.temp_{table};CREATE TABLE {schema}.temp_{table} AS """.format(
	schema=schema,
	table=table)
    sql += select_sql
    cur.execute(sql)

    cur.execute("SELECT COUNT(1) FROM {schema}.temp_{table}""".format(schema=schema, table=table))
    count = cur.fetchone()[0]
    if count == 0:
        raise ValueError("{schema}.{table} didn't have any record".format(schema=schema, table=table))

    try:
        sql = """DROP TABLE IF EXISTS {schema}.{table};ALTER TABLE {schema}.temp_{table} RENAME to {table};""".format(
            schema=schema,
	    table=table)
        sql += "COMMIT;"
        logging.info(sql)
        cur.execute(sql)
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error('Failed to sql. Completed ROLLBACK!')
        raise AirflowException("")


dag = DAG(
    dag_id = "Build_Summary",
    start_date = datetime(2021,12,10),
    schedule_interval = '@once',
    catchup = False
)

execsql = PythonOperator(
    task_id = 'execsql',
    python_callable = execSQL,
    params = {
        'schema' : 'k456123456',
        'table': 'nps_summary',
        'sql' : """
        SELECT date, cast((promoter - detractor) as float)/cast(total as float) as nps
        FROM
        (
          SELECT to_date(created_at, 'yyyy/mm/dd') as date
          , COUNT(DISTINCT CASE WHEN score >=9 THEN id END) as promoter
          , COUNT(DISTINCT CASE WHEN score >=0 and score <=6 THEN id END) as detractor
          , COUNT(DISTINCT id) as total
          FROM raw_data.nps
          GROUP BY 1
        );"""
    },
    provide_context = True,
    dag = dag
)
