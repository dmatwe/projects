"""
Забираем данные из CBR и складываем в Greenplum.
"""
from airflow import DAG
from airflow.utils.dates import days_ago
import logging
import xml.etree.ElementTree as ET
import csv

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'start_date': days_ago(2),
    'owner': 'd-matveev',
    'poke_interval': 600
}

with DAG("ds_load_cbr_2024_d_matveev",
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=['2024_d_matveev']
) as dag:

          
    export_cbr_xml = BashOperator(
        task_id='export_cbr_xml',
        bash_command='curl {url} | iconv -f Windows-1251 -t UTF-8 > /tmp/dina_cbr.xml'.format(
            url='https://www.cbr.ru/scripts/XML_daily.asp?date_req=04/12/2021'
        ),
        dag=dag
    )

    def xml_to_csv_func():
        parser = ET.XMLParser(encoding="UTF-8")
        tree = ET.parse('/tmp/dina_cbr.xml', parser=parser)
        root = tree.getroot()

        with open('/tmp/dina_cbr.csv', 'w') as csv_file:
            writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for Valute in root.findall('Valute'):
                NumCode = Valute.find('NumCode').text
                CharCode = Valute.find('CharCode').text
                Nominal = Valute.find('Nominal').text
                Name = Valute.find('Name').text
                Value = Valute.find('Value').text
                writer.writerow([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                                [Name] + [Value.replace(',', '.')])
                logging.info([root.attrib['Date']] + [Valute.attrib['ID']] + [NumCode] + [CharCode] + [Nominal] +
                             [Name] + [Value.replace(',', '.')])


    xml_to_csv = PythonOperator(
        task_id='xml_to_csv',
        python_callable=xml_to_csv_func
    )
    
    def drop_table():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.run("DROP TABLE IF EXISTS dina_cbr_dmatveev_2024")

    def create_table():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        create_query = """
        CREATE TABLE IF NOT EXISTS dina_cbr_dmatveev_2024
        (
            dt text,
            id text,
            num_code text,
            char_code text,
            nominal text,
            nm text,
            value text
        )
        """
        pg_hook.run(create_query)
    
    def load_csv_to_greenplum():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        pg_hook.copy_expert("COPY dina_cbr_dmatveev_2024  FROM STDIN DELIMITER ','", '/tmp/dina_cbr.csv')
        
       
    drop_table = PythonOperator(
        task_id = 'drop_table',
        python_callable=drop_table
    )
    
    create_table = PythonOperator(
        task_id = 'create_table',
        python_callable=create_table
    )
    
    load_csv_to_greenplum = PythonOperator(
        task_id = 'load_csv_to_greenplum',
        python_callable=load_csv_to_greenplum
    )
    
export_cbr_xml >> xml_to_csv >> drop_table >> create_table >> load_csv_to_greenplum