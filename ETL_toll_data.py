# Import necessary modules
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator

# Default arguments for the DAG
default_args = {
    'owner': 'me',
    'start_date': datetime.now() - timedelta(days=1),  # DAG start date is set to 1 day ago
    'email': ['me@email.com'],                        # Email for notifications
    'email_on_failure': True,                         # Notify on failure
    'email_on_retry': True,                           # Notify on retry
    'retries': 1,                                     # Number of retries allowed
    'retry_delay': timedelta(minutes=5),              # Wait time between retries
}

# Define the DAG
dag = DAG(
    dag_id='ELT_toll_data',                           # Unique identifier for the DAG
    default_args=default_args,
    description='Apache Airflow Final Assignment',    # Description for DAG
    schedule=timedelta(days=1),                       # Run daily
)

# Task 1: Unzip the toll data archive
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

# Task 2: Extract specific columns from vehicle-data.csv
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command="""
    echo "Extracting data from vehicle-data.csv"
    cut -d"," -f1-4 /home/km1079/airflow/dags/finalassignment/vehicle-data.csv \
    > /home/km1079/airflow/dags/finalassignment/csv_data.csv
    """,
    dag=dag,
)

# Task 3: Extract and convert columns from tollplaza-data.tsv
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="""
    echo "Extracting data from tollplaza-data.tsv";
    cut -f5-7 /home/km1079/airflow/dags/finalassignment/tollplaza-data.tsv | tr '\t' ',' \
    > /home/km1079/airflow/dags/finalassignment/tsv_data.csv
    """,
    dag=dag,
)

# Task 4: Extract specific character positions from fixed-width payment-data.txt
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="""
    echo "Extracting data from payment-data.txt";
    INPUT_FILE=/home/km1079/airflow/dags/finalassignment/payment-data.txt
    OUTPUT_FILE=/home/km1079/airflow/dags/finalassignment/fixed_width_data.csv
    cut -c59-61 "$INPUT_FILE" > /tmp/col1.csv
    cut -c63-67 "$INPUT_FILE" > /tmp/col2.csv
    paste -d',' /tmp/col1.csv /tmp/col2.csv > "$OUTPUT_FILE"
    rm /tmp/col1.csv /tmp/col2.csv  # Cleanup temporary files
    """,
    dag=dag,
)

# Task 5: Combine all extracted data into one consolidated file
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command="""
    echo "Consolidating data"
    paste -d"," \
    /home/km1079/airflow/dags/finalassignment/csv_data.csv \
    /home/km1079/airflow/dags/finalassignment/tsv_data.csv \
    /home/km1079/airflow/dags/finalassignment/fixed_width_data.csv \
    > /home/km1079/airflow/dags/finalassignment/extracted_data.csv
    head -n3 /home/km1079/airflow/dags/finalassignment/extracted_data.csv  # Preview output
    """,
    dag=dag,
)

# Task 6: Transform the vehicle type field to uppercase
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
    head -n3 "$OUTPUT"  # Preview output
    """,
    dag=dag,
)

# Define the task execution order of the task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
