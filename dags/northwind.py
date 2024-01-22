from datetime import timedelta, datetime
from airflow import DAG 
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID="datawarehouse-411117"
GS_PATH = "data/"
BUCKET_NAME = 'us-central1-datawarehouse-ad43d7dc-bucket'
STAGING_DATASET = "OLTP"
DATASET = "OLAP"
LOCATION = "us-central1"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('northwind', schedule_interval=timedelta(days=1), default_args=default_args) as dag:
    start_pipeline = DummyOperator(
        task_id = 'start_pipeline',
        dag = dag
        )


    load_staging_dataset = DummyOperator(
        task_id = 'load_staging_dataset',
        dag = dag
        )
    create_table_task = BigQueryOperator(
        task_id='create_table',
        sql="""
        CREATE OR REPLACE TABLE `datawarehouse-411117.OLTP.customers` (
            customerID STRING,
            companyName STRING,
            contactName STRING,
            contactTitle STRING,
            address STRING,
            city STRING,
            region STRING,
            postalCode STRING,
            country STRING,
            phone STRING,
            fax STRING
        )
        """,
    use_legacy_sql=False,
    location='us-central1',
    dag=dag,
)
    load_dataset_customer = GCSToBigQueryOperator(
        task_id = 'load_dataset_customer',
        bucket = BUCKET_NAME,
        source_objects = ['./data/customers.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.customers',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        field_delimiter=',',
    )

    load_dataset_employee = GCSToBigQueryOperator(
        task_id = 'load_dataset_employee',
        bucket = BUCKET_NAME,
        source_objects = ['./data/employees.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.employees',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        field_delimiter=',',
    )

    load_dataset_product = GCSToBigQueryOperator(
        task_id = 'load_dataset_product',
        bucket = BUCKET_NAME,
        source_objects = ['./data/products.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.products',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        field_delimiter=',',
    )
    
    load_dataset_supplier = GCSToBigQueryOperator(
        task_id = 'load_dataset_supplier',
        bucket = BUCKET_NAME,
        source_objects = ['./data/suppliers.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.suppliers',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        field_delimiter=',',
    )
    load_dataset_order = GCSToBigQueryOperator(
        task_id = 'load_dataset_order',
        bucket = BUCKET_NAME,
        source_objects = ['./data/orders.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.orders',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        field_delimiter=',',
    )
    load_dataset_order_details = GCSToBigQueryOperator(
        task_id = 'load_dataset_order_details',
        bucket = BUCKET_NAME,
        source_objects = ['./data/order-details.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.order-details',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        field_delimiter=',',
    )
    load_dataset_categories = GCSToBigQueryOperator(
        task_id = 'load_dataset_categories',
        bucket = BUCKET_NAME,
        source_objects = ['./data/categories.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.categories',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        field_delimiter=',',
    )
    load_dataset_territories = GCSToBigQueryOperator(
        task_id = 'load_dataset_territories',
        bucket = BUCKET_NAME,
        source_objects = ['./data/territories.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.territories',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        field_delimiter=',',
    )
    load_dataset_employee_territories = GCSToBigQueryOperator(
        task_id = 'load_dataset_employee-territories',
        bucket = BUCKET_NAME,
        source_objects = ['./data/employee-territories.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.employee-territories',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        field_delimiter=',',
    )
    load_dataset_region = GCSToBigQueryOperator(
        task_id = 'load_dataset_region',
        bucket = BUCKET_NAME,
        source_objects = ['./data/regions.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.regions',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        field_delimiter=',',
    )
    load_dataset_shipper = GCSToBigQueryOperator(
        task_id = 'load_dataset_shipper',
        bucket = BUCKET_NAME,
        source_objects = ['./data/shippers.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.shippers',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        field_delimiter=',',
    )
    check_dataset_customer = BigQueryCheckOperator(
        task_id = 'check_dataset_customer',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.customers`'
        )

    check_dataset_employee = BigQueryCheckOperator(
        task_id = 'check_dataset_employee',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.employees`'
        )

    check_dataset_product = BigQueryCheckOperator(
        task_id = 'check_dataset_product',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.products`'
        ) 
    check_dataset_supplier = BigQueryCheckOperator(
        task_id = 'check_dataset_supplier',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.suppliers`'
        ) 
    check_dataset_order = BigQueryCheckOperator(
        task_id = 'check_dataset_order',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.orders`'
        ) 
    check_dataset_order_detail = BigQueryCheckOperator(
        task_id = 'check_dataset_order_detail',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.order-details`'
        ) 
    check_dataset_categories = BigQueryCheckOperator(
        task_id = 'check_dataset_categories',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.categories`'
        ) 
    check_dataset_employee_territories = BigQueryCheckOperator(
        task_id = 'check_dataset_employee-territories',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.employee-territories`'
        ) 
    check_dataset_territories = BigQueryCheckOperator(
        task_id = 'check_dataset_territories',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.territories`'
        ) 
    check_dataset_region = BigQueryCheckOperator(
        task_id = 'check_dataset_region',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.regions`'
        ) 
    check_dataset_shipper = BigQueryCheckOperator(
        task_id = 'check_dataset_shipper',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.shippers`'
        ) 
    create_D_Table = DummyOperator(
        task_id = 'Create_D_Table',
        dag = dag
        )

    create_D_dataset_customer = BigQueryOperator(
        task_id = 'create_D_dataset_customer',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/dim_customers.sql'
        )

    create_D_dataset_employee = BigQueryOperator(
        task_id = 'create_D_dataset_employee',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/dim_employees.sql'
        )   

    create_D_dataset_product = BigQueryOperator(
        task_id = 'create_D_dataset_product',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/dim_products.sql'
        )
    create_D_dataset_category = BigQueryOperator(
        task_id = 'create_D_dataset_category',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/dim_category.sql'
        )
    create_D_dataset_date = BigQueryOperator(
        task_id = 'create_D_dataset_date',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/dim_date.sql'
        )
    
    create_F_dataset_fact_sales = BigQueryOperator(
        task_id = 'create_F_dataset_fact_sales',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/fact_sales.sql'
        )
    check_F_dataset_fact_sales = BigQueryCheckOperator(
        task_id = 'check_F_dataset_fact_sales',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.Fact_sales`'
        ) 

    finish_pipeline = DummyOperator(
        task_id = 'finish_pipeline',
        dag = dag
        ) 
start_pipeline >> load_staging_dataset

load_staging_dataset >> [load_dataset_customer, load_dataset_employee, load_dataset_product, load_dataset_supplier, load_dataset_order, load_dataset_order_details, load_dataset_categories,load_dataset_employee_territories, load_dataset_territories, load_dataset_region, load_dataset_shipper]

load_dataset_customer >> check_dataset_customer
load_dataset_employee >> check_dataset_employee
load_dataset_product >> check_dataset_product
load_dataset_supplier >> check_dataset_supplier
load_dataset_order >> check_dataset_order
load_dataset_order_details >> check_dataset_order_detail
load_dataset_categories >> check_dataset_categories
load_dataset_employee_territories >> check_dataset_employee_territories
load_dataset_territories >>check_dataset_territories
load_dataset_region >>check_dataset_region
load_dataset_shipper >> check_dataset_shipper

[check_dataset_customer, check_dataset_employee, check_dataset_product, check_dataset_supplier, check_dataset_order, check_dataset_order_detail,check_dataset_categories, check_dataset_employee_territories, check_dataset_territories,check_dataset_region,check_dataset_shipper ] >> create_D_Table

create_D_Table >> [create_D_dataset_customer, create_D_dataset_employee, create_D_dataset_product,create_D_dataset_category, create_D_dataset_date]

[create_D_dataset_customer, create_D_dataset_employee, create_D_dataset_product,create_D_dataset_category, create_D_dataset_date] >> create_F_dataset_fact_sales

create_F_dataset_fact_sales >> check_F_dataset_fact_sales >> finish_pipeline
