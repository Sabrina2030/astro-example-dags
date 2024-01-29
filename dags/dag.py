from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from time import time_ns
from datetime import datetime , timedelta
from airflow.utils.dates import days_ago
import os
from pymongo import MongoClient
from pandas import DataFrame
from google.cloud import bigquery
import pandas as pd
import numpy as np


default_args = {
    'owner': 'Datapath',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def get_connect_mongo():
    CONNECTION_STRING ="mongodb+srv://atlas:T6.HYX68T8Wr6nT@cluster0.enioytp.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(CONNECTION_STRING)
    return client
    
def start_process():
    print(" INICIO EL PROCESO!")

def end_process():
    print(" FIN DEL PROCESO!")

def load_products():
    print(f" INICIO LOAD PRODUCTS")
    dbconnect = get_connect_mongo()
    dbname=dbconnect["retail_db"]
    collection_name = dbname["products"] 
    products = collection_name.find({})
    products_df = DataFrame(products)
    dbconnect.close()
    products_df['_id'] = products_df['_id'].astype(str)
    products_df['product_description'] = products_df['product_description'].astype(str)
    products_rows=len(products_df)
    print(f" Se obtuvo  {products_rows}  Filas")
    products_rows=len(products_df)
    if products_rows>0 :
        client = bigquery.Client(project='atomic-lens-395620')
        table_id =  "atomic-lens-395620.dep_raw.products"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("product_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("product_category_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("product_name", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("product_description", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("product_price", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("product_image", bigquery.enums.SqlTypeNames.STRING),
            ],
            write_disposition="WRITE_TRUNCATE",
        )


        job = client.load_table_from_dataframe(
            products_df, table_id, job_config=job_config
        )  
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else : 
        print('alerta no hay registros en la tabla productos')


def load_order_items():
    print(f"INICIO LOAD ORDER_ITEMS")
    dbconnect = get_connect_mongo()
    dbname = dbconnect["retail_db"]
    collection_name = dbname["order_items"]
    order_items = collection_name.find({})
    order_items_df = DataFrame(order_items)
    dbconnect.close()

    order_items_df['_id'] = order_items_df['_id'].astype(str)
    
    def transform_date(text):
        text = str(text)
        d = text[0:10]
        return d
    
    order_items_df['order_date'] = order_items_df['order_date'].map(transform_date)
    order_items_df['order_date'] = pd.to_datetime(order_items_df['order_date'], format='%Y-%m-%d').dt.date
    
    order_items_rows = len(order_items_df)
    print(f"Se obtuvo {order_items_rows} Filas")

    if order_items_rows > 0:
        client = bigquery.Client(project='atomic-lens-395620')
        table_id = "atomic-lens-395620.dep_raw.order_items"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("order_date", bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField("order_item_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_product_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_order_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_quantity", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_subtotal", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("order_item_product_price", bigquery.enums.SqlTypeNames.FLOAT),
            ],
            write_disposition="WRITE_TRUNCATE",
        )

        job = client.load_table_from_dataframe(
            order_items_df, table_id, job_config=job_config
        )
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else:
        print('Alerta: no hay registros en la tabla order_items')

def load_orders():
    print(f"INICIO LOAD ORDERS")
    dbconnect = get_connect_mongo()
    dbname = dbconnect["retail_db"]
    collection_name = dbname["orders"]
    orders = collection_name.find({})
    orders_df = DataFrame(orders)
    dbconnect.close()

    orders_df['_id'] = orders_df['_id'].astype(str)
    
    def transform_date(text):
        text = str(text)
        d = text[0:10]
        return d
    
    orders_df['order_date'] = orders_df['order_date'].map(transform_date)
    orders_df['order_date'] = pd.to_datetime(orders_df['order_date'], format='%Y-%m-%d').dt.date
    
    orders_rows = len(orders_df)
    print(f"Se obtuvo {orders_rows} Filas")

    if orders_rows > 0:
        client = bigquery.Client(project='atomic-lens-395620')
        table_id = "atomic-lens-395620.dep_raw.orders"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("order_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_date", bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField("order_customer_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_status", bigquery.enums.SqlTypeNames.STRING),
            ],
            write_disposition="WRITE_TRUNCATE",
        )

        job = client.load_table_from_dataframe(
            orders_df, table_id, job_config=job_config
        )
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else:
        print('Alerta: no hay registros en la tabla orders')


def load_customers():
    print(f"INICIO LOAD CUSTOMERS")
    dbconnect = get_connect_mongo()
    dbname = dbconnect["retail_db"]
    collection_name = dbname["customers"]
    customers = collection_name.find({})
    customers_df = DataFrame(customers)
    dbconnect.close()

    customers_df['_id'] = customers_df['_id'].astype(str)
    
    customers_rows = len(customers_df)
    print(f"Se obtuvo {customers_rows} Filas")

    if customers_rows > 0:
        client = bigquery.Client(project='atomic-lens-395620')
        table_id = "atomic-lens-395620.dep_raw.customers"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("customer_fname", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_lname", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_email", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_password", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_street", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_city", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_state", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("customer_zipcode", bigquery.enums.SqlTypeNames.INTEGER),
            ],
            write_disposition="WRITE_TRUNCATE",
        )

        job = client.load_table_from_dataframe(
            customers_df, table_id, job_config=job_config
        )
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else:
        print('Alerta: no hay registros en la tabla customers')


def load_categories():
    print("INICIO LOAD CATEGORIES")
    
    dbconnect = get_connect_mongo()
    dbname = dbconnect["retail_db"]
    collection_name = dbname["categories"]
    
    # Convertir ObjectId a cadena al obtener los datos de MongoDB
    categories = collection_name.find({})
    categories_list = [record for record in categories]
    for record in categories_list:
        record['_id'] = str(record['_id'])
    
    categories_df = DataFrame(categories_list)
    dbconnect.close()

    categories_rows = len(categories_df)
    print(f"Se obtuvo {categories_rows} Filas")

    if categories_rows > 0:
        client = bigquery.Client(project='atomic-lens-395620')
        table_id = "atomic-lens-395620.dep_raw.categories"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("category_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("category_department_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("category_name", bigquery.enums.SqlTypeNames.STRING),
            ],
            write_disposition="WRITE_TRUNCATE",
        )

        job = client.load_table_from_dataframe(
            categories_df, table_id, job_config=job_config
        )
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else:
        print('Alerta: no hay registros en la tabla categories')


def load_departments():
    print("INICIO LOAD DEPARTMENTS")
    client = get_connect_mongo()
    dbname = client["retail_db"]
    collection_name = dbname["departments"]
    departments = collection_name.find({})
    departments_df = pd.DataFrame(departments)
    client.close()
    
    departments_df['department_id'] = departments_df['department_id'].astype(str).apply(lambda x: np.int64(x))

    departments_rows = len(departments_df)
    print(f"Se obtuvo {departments_rows} Filas")

    print(departments_df.dtypes)

    if departments_rows > 0:
        client = bigquery.Client(project='atomic-lens-395620')
        table_id = "atomic-lens-395620.dep_raw.departments"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("department_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("department_name", bigquery.enums.SqlTypeNames.STRING),
            ],
            write_disposition="WRITE_TRUNCATE",
        )

        job = client.load_table_from_dataframe(
            departments_df, table_id, job_config=job_config
        )
        job.result()  # Esperar a que se complete el trabajo.

        table = client.get_table(table_id)  # Hacer una solicitud a la API.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else:
        print('Alerta: no hay registros en la tabla departments')


def load_master_order():
    client = bigquery.Client()
    sql = """
        SELECT *
        FROM `atomic-lens-395620.dep_raw.order_items`
    """
    m_order_items_df = client.query(sql).to_dataframe()

    client = bigquery.Client()
    sql_2 = """
        SELECT *
        FROM `atomic-lens-395620.dep_raw.orders`
    """
    m_orders_df = client.query(sql_2).to_dataframe()

    df_join = m_orders_df.merge(m_order_items_df, left_on='order_id', right_on='order_item_order_id', how='inner')

    df_master = df_join[['order_id', 'order_date_x', 'order_customer_id',
                        'order_status', 'order_item_id',
                        'order_item_order_id', 'order_item_product_id', 'order_item_quantity',
                        'order_item_subtotal', 'order_item_product_price']]

    df_master = df_master.rename(columns={"order_date_x": "order_date"})

    def get_group_status(text):
        text = str(text)
        if text == 'CLOSED':
            d = 'END'
        elif text == 'COMPLETE':
            d = 'END'
        else:
            d = 'TRANSIT'
        return d

    df_master['order_status_group'] = df_master['order_status'].map(get_group_status)

    df_master['order_date'] = df_master['order_date'].astype(str)
    df_master['order_date'] = pd.to_datetime(df_master['order_date'], format='%Y-%m-%d').dt.date

    df_master_rows = len(df_master)
    if df_master_rows > 0:
        client = bigquery.Client()

        table_id = "atomic-lens-395620.dep_raw.master_order"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("order_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_date", bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField("order_customer_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_status", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("order_item_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_order_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_product_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_quantity", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_subtotal", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("order_item_product_price", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("order_status_group", bigquery.enums.SqlTypeNames.STRING),
            ],
            write_disposition="WRITE_TRUNCATE",
        )

        job = client.load_table_from_dataframe(
            df_master, table_id, job_config=job_config
        )
        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else:
        print('Alerta: no hay registros en la tabla order_items')

    exchange_rate = 3.736
    df_master['order_item_subtotal_mn'] = df_master['order_item_subtotal'] * exchange_rate

    if df_master_rows > 0:
        # Configure BigQuery job
        table_id = "atomic-lens-395620.dep_raw.master_order"
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("order_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_date", bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField("order_customer_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_status", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("order_item_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_order_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_product_id", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_quantity", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField("order_item_subtotal", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("order_item_product_price", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("order_status_group", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("order_item_subtotal_mn", bigquery.enums.SqlTypeNames.FLOAT),
            ],
            write_disposition="WRITE_TRUNCATE",
        )

        # Load data into BigQuery
        job = client.load_table_from_dataframe(df_master, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete.

        # Print job result
        table = client.get_table(table_id)
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
    else:
        print('Alert: No hay registros en la tabla order_items')

    # Load BI Table
    query_string = """
    create or replace table `atomic-lens-395620.dep_raw.bi_orders` as
    SELECT 
     order_date,c.category_name ,d.department_name 
     , sum (a.order_item_subtotal) order_item_subtotal
     , sum (a.order_item_quantity) order_item_quantity
    FROM `premium-guide-410714.dep_raw.master_order` a
    inner join  `atomic-lens-395620.dep_raw.products` b on
    a.order_item_product_id=b.product_id
    inner join `atomic-lens-395620.dep_raw.categories` c on
    b.product_category_id=c.category_id
    inner join `atomic-lens-395620.dep_raw.departments` d on
    c.category_department_id=d.department_id
    group by order_date,c.category_name ,d.department_name
    """
    query_job = client.query(query_string)
    rows = list(query_job.result())
    print(rows)


with DAG(
    dag_id="load_project",
    schedule_interval="20 04 * * *", 
    start_date=days_ago(2), 
    default_args=default_args
) as dag:
    step_start = PythonOperator(
        task_id='step_start_id',
        python_callable=start_process,
        dag=dag
    )

    step_load_products = PythonOperator(
        task_id='load_products_id',
        python_callable=load_products,
        dag=dag
    )

    step_load_order_items = PythonOperator(
        task_id='load_order_items_id',
        python_callable=load_order_items,
        dag=dag
    )

    step_load_orders = PythonOperator(
        task_id='load_orders_id',
        python_callable=load_orders,
        dag=dag
    )

    step_load_customers = PythonOperator(
        task_id='load_customers_id',
        python_callable=load_customers,
        dag=dag
    )

    step_load_categories = PythonOperator(
        task_id='load_categories_id',
        python_callable=load_categories,
        dag=dag
    )

    step_load_departments = PythonOperator(
        task_id='load_departments_id',
        python_callable=load_departments,
        dag=dag
    )

    step_load_master_order = PythonOperator(
        task_id='load_master_order_id',
        python_callable=load_master_order,
        dag=dag
    )

    step_end = PythonOperator(
        task_id='step_end_id',
        python_callable=end_process,
        dag=dag
    )

    # Set task dependencies
    step_start >> [step_load_products, step_load_order_items, step_load_orders, step_load_customers, step_load_categories, step_load_departments] >> step_load_master_order >> step_end
