from datetime import datetime, timedelta
from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
import gspread

# Inputs
credentials_file = '/opt/airflow/config/credentials/cred.json'
sheet_url = 'https://docs.google.com/spreadsheets/d/1I7EsowY_uqWblkmDfPWQ9b8n4WPfKtVWsVzeYKN_wQk/edit#gid=121545731'
headers_w_datatype_dict = {
    "No.": "INTEGER",
    "Color": "VARCHAR(256)",
    "Picture": "VARCHAR(256)",
    "CLV Start": "DATE",
    "Looker Start": "DATE",
    "DoB": "DATE",
    "Current Laptop Spec": "VARCHAR(256)",
    "Passport": "BOOL",
    "Số mũi Vaccin": "INTEGER"
    }
table_sql = "members.info"
file_path = 'sql_transform_postgres.txt'

def download_data(credentials_file, sheet_url):
    # Authenticate with the Google Sheet API
    client = gspread.service_account(credentials_file)

    # Open the Google Sheet by URL
    sheet = client.open_by_url(sheet_url)

    # Select the desired worksheet
    worksheet = sheet.get_worksheet(0)  # The first worksheet

    # Get all the values from the worksheet (as list)
    csv_data = worksheet.get_all_values()
    return csv_data

def get_list_keys(headers_w_datatype_dict):
    # Get list of all keys from headers_w_datatype dictionary (created by user, containing all columns except the most common datatype: VARCHAR(50))
    return list(headers_w_datatype_dict.keys())

def get_create_sql_code(headers_csv, list_keys, headers_w_datatype_dict, table_sql):
    # Create string variable with no value
    headers_w_datatype = ""

    # For each header in headers list:
    # - if header in list all keys from headers_w_datatype dictionary: add header + value of the key (=header)
    # - else: add header + VARCHAR(50) -> this is the most common
    for h in headers_csv:
        if h in list_keys:
            headers_w_datatype += f"\"{h}\" {headers_w_datatype_dict[h]},\n"
        else:
            headers_w_datatype += f"\"{h}\" VARCHAR(50),\n"
    
    # Remove the 2 final characters "," and "\n"
    headers_w_datatype = headers_w_datatype[:-2]

    # Return SQL code to create table in Postgres
    return f"""CREATE TABLE IF NOT EXISTS {table_sql} (
            {headers_w_datatype}
            );
            """

def get_insert_sql_code(csv_data, table_sql):
    # Create string variable with no value
    inserted_value = ""
    
    # For each index from 1 -> end (excuding 0), add each data row (as tuple)
    for i in range(1, len(csv_data)):
        inserted_value += f"{tuple(csv_data[i])},\n"
    
    # Remove the 2 final characters "," and "\n"
    inserted_value = inserted_value[:-2]
    
    # SQL code in Postgres to insert value into the table created above
    sql_insert_table_code = f"""INSERT INTO {table_sql}
                            VALUES {inserted_value};
                            """
    
    # Replace '' by NULL then return SQL code
    new_sql_insert_table_code = sql_insert_table_code.replace("''", "NULL")
    return new_sql_insert_table_code

def get_sql_code(credentials_file, sheet_url, headers_w_datatype_dict, table_sql):
    csv_data = download_data(credentials_file, sheet_url)
    headers_csv = csv_data[0]
    list_keys = get_list_keys(headers_w_datatype_dict)
    sql_create_table_code = get_create_sql_code(headers_csv, list_keys, headers_w_datatype_dict, table_sql)
    new_sql_insert_table_code = get_insert_sql_code(csv_data, table_sql)
    return f""" DROP TABLE IF EXISTS {table_sql};
                {sql_create_table_code}
                {new_sql_insert_table_code}
            """
def transform_data(file_path):
    sql_transform = open(file_path,'r')
    return sql_transform.read()

with DAG(
    dag_id='dag_ggsheet_to_postgres_phoebe',
    default_args={
        'owner': 'phoebe',
        'retries': 2,
        'retry_delay': timedelta(minutes=1)
        },
    start_date=datetime(2023, 7, 15),
    schedule_interval='0 0 * * *'
) as dag:
    task1 = PostgresOperator(
        task_id = 'ggsheet_to_postgresql',
        postgres_conn_id = 'railway_postgres',
        sql = get_sql_code(credentials_file, sheet_url, headers_w_datatype_dict, table_sql)
        )
    task2 = PostgresOperator(
        task_id = 'transform_data',
        postgres_conn_id = 'railway_postgres',
        sql = transform_data(file_path)
    )

    task1 >> task2