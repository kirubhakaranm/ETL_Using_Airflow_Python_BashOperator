from datetime import datetime,timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'me',
    'start_date': datetime.now() - timedelta(days=1),
    'email': ['me@email.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='ELT_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule=timedelta(days=1),
)

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command="""
    echo "Unzipping data"
    cd /home/km1079/airflow/dags/finalassignment
    tar -xzf tolldata.tgz
    echo "Listing files after extraction:"
    ls -l
    """,
    dag=dag,
)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command="""
    echo "Extracting data from vehicle-data.csv"
    cut -d"," -f1-4 /home/km1079/airflow/dags/finalassignment/vehicle-data.csv \
    > /home/km1079/airflow/dags/finalassignment/csv_data.csv
    """,
    dag=dag,
)

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="""
    echo "Extracting data from tollplaza-data.tsv";
    cut -f5-7 /home/km1079/airflow/dags/finalassignment/tollplaza-data.tsv | tr '\t' ',' \
    > /home/km1079/airflow/dags/finalassignment/tsv_data.csv
    """,
    dag=dag,
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="""
    echo "Extracting data from payment-data.txt";
    INPUT_FILE=/home/km1079/airflow/dags/finalassignment/payment-data.txt
    OUTPUT_FILE=/home/km1079/airflow/dags/finalassignment/fixed_width_data.csv
    cut -c59-61 "$INPUT_FILE" > /tmp/col1.csv
    cut -c63-67 "$INPUT_FILE" > /tmp/col2.csv
    paste -d',' /tmp/col1.csv /tmp/col2.csv > "$OUTPUT_FILE"
    rm /tmp/col1.csv /tmp/col2.csv
    """,
    dag=dag,
)

# extract_data_from_fixed_width = BashOperator(
#     task_id='extract_data_from_fixed_width',
#     bash_command="""
#     echo "Extracting data from payment-data.txt";
#     INPUT_FILE=/home/km1079/airflow/dags/finalassignment/payment-data.txt
#     OUTPUT_FILE=/home/km1079/airflow/dags/finalassignment/fixed_width_data.csv
#     awk '{print substr($0,59,3) "," substr($0,63,5)}' "$INPUT_FILE" > "$OUTPUT_FILE"
#     """,
#     dag=dag,
# )


consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command="""
    echo "Consolidating data"
    paste -d"," \
    /home/km1079/airflow/dags/finalassignment/csv_data.csv \
    /home/km1079/airflow/dags/finalassignment/tsv_data.csv \
    /home/km1079/airflow/dags/finalassignment/fixed_width_data.csv \
    > /home/km1079/airflow/dags/finalassignment/extracted_data.csv
    head -n3 /home/km1079/airflow/dags/finalassignment/extracted_data.csv
    """,
    dag=dag,
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command="""
    echo "Transforming vehicle type field to uppercase"
    FILE=/home/km1079/airflow/dags/finalassignment/extracted_data.csv
    OUTPUT=/home/km1079/airflow/dags/finalassignment/transformed_data.csv

    cut -d, -f1-3 "$FILE" > /tmp/col1_3.csv
    cut -d, -f4 "$FILE" | tr '[:lower:]' '[:upper:]' > /tmp/col4.csv
    cut -d, -f5-9 "$FILE" > /tmp/col5_9.csv

    paste -d, /tmp/col1_3.csv /tmp/col4.csv /tmp/col5_9.csv > "$OUTPUT"
    head -n3 /home/km1079/airflow/dags/finalassignment/transformed_data.csv
    """,
    dag=dag,
)


unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data

