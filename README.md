# 🚗 ETL Toll Data Pipeline using Apache Airflow

## Overview

This project implements an ETL (Extract, Transform, Load) data pipeline for toll booth data using **Apache Airflow**. It demonstrates how to automate data processing workflows using shell commands and BashOperators within Airflow, working across various file formats.

---

## 💡 Features

- Automated **daily ETL** workflow using Apache Airflow
- Handles multiple file formats: **CSV**, **TSV**, and **fixed-width text files**
- Extracts and consolidates specific fields from each dataset
- Transforms vehicle type data for standardization
- Includes retry mechanism and email alerts on failure

---

## 🧱 Pipeline Structure

The ETL pipeline consists of six main stages:

### 1. **Unzip Data**
Extracts the compressed archive `tolldata.tgz` to access raw data files.

### 2. **Extract Data from CSV (`vehicle-data.csv`)**
Extracts the following fields:
- `Rowid`
- `Timestamp`
- `Anonymized Vehicle number`
- `Vehicle type`

### 3. **Extract Data from TSV (`tollplaza-data.tsv`)**
Extracts the following fields and converts tab-separated values to CSV:
- `Tollplaza id`
- `Tollplaza code`
- `Number of axles`

### 4. **Extract Data from Fixed Width File (`payment-data.txt`)**
Extracts character positions corresponding to:
- `Type of Payment code`
- `Vehicle Code`

### 5. **Consolidate Data**
Combines all extracted fields from the three sources into a single structured CSV file.

### 6. **Transform Data**
Transforms the `Vehicle type` field to **uppercase** for consistency.

---

## 📂 File Structure

```
📁 finalassignment/
├── 📦 tolldata.tgz              # Compressed archive containing raw data files
├── 📄 vehicle-data.csv          # CSV file with vehicle information
├── 📄 tollplaza-data.tsv        # TSV file with toll plaza data
├── 📄 payment-data.txt          # Fixed-width file with payment information
├── 📄 csv_data.csv              # Extracted data from vehicle-data.csv
├── 📄 tsv_data.csv              # Extracted and converted data from tollplaza-data.tsv
├── 📄 fixed_width_data.csv      # Extracted data from payment-data.txt
├── 📄 extracted_data.csv        # Consolidated data from the above three sources
└── 📄 transformed_data.csv      # Final output after data transformation
```

---

## 🔧 Requirements

- **Python 3.11+**
- **Apache Airflow 3.0.1**
- **Linux or Unix-based environment** with access to shell commands:
  - `cut`, `paste`, `tr`, `awk`, `tar`

---

## ⚙️ Installation (Using requirements.txt)

1. Use the file named `requirements.txt` with the following content:

```txt
apache-airflow==3.0.1
```

2. Install the dependencies:

```bash
pip install -r requirements.txt
```

*Note: Adjust the Airflow version in requirements.txt as needed to match your environment.*

---

## 🛠️ Setup Instructions

1. **Clone this repository** and place your input files in the `finalassignment/` directory.

2. **Copy the DAG Python script** into your Airflow `dags/` folder.

3. **Start Airflow** in standalone mode (scheduler and webserver will start automatically):

```bash
airflow standalone
```

4. **Open the Airflow UI** (usually at http://localhost:8080) and enable the DAG titled `ETL_toll_data`.

---

## 📊 Input Data Schema

### 📁 `vehicle-data.csv` (CSV Format)
**Contains:**
- Rowid
- Timestamp
- Anonymized Vehicle number
- Vehicle type
- Number of axles
- Vehicle code

### 📁 `tollplaza-data.tsv` (TSV Format)
**Contains:**
- Rowid
- Timestamp
- Anonymized Vehicle number
- Vehicle type
- Number of axles
- Tollplaza id
- Tollplaza code

### 📁 `payment-data.txt` (Fixed-width Format)
**Contains:**
- Rowid
- Timestamp
- Anonymized Vehicle number
- Tollplaza id
- Tollplaza code
- Type of Payment code
- Vehicle code

---

## 🚀 Usage

1. Ensure all input data files are placed in the `finalassignment/` directory
2. Start the Airflow webserver and scheduler
3. Navigate to the Airflow UI
4. Enable and trigger the `ETL_toll_data` DAG
5. Monitor the pipeline execution through the Airflow interface
6. Check the `transformed_data.csv` file for the final processed output

---

## 🔍 Pipeline Monitoring

The Airflow DAG includes:
- **Retry mechanisms** for failed tasks
- **Email alerts** on pipeline failure
- **Task dependencies** to ensure proper execution order
- **Logging** for debugging and monitoring

---

## 🎓 Course Attribution

This project is part of the **IBM Data Engineering Professional Certificate** on Coursera.  
It is developed for the course **"ETL and Data Pipelines with Shell, Airflow and Kafka"**, which covers essential data engineering workflows using open-source tools.

---

## 👤 Author

**Kirubhakaran M**  
📧 Email: km1079@g.rit.edu  
💼 LinkedIn: https://www.linkedin.com/in/kirubhakaranm/

---

## 🤝 Contributing

This is an educational project. If you have suggestions for improvements or find any issues, feel free to create an issue or submit a pull request.

---

## 📚 Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [IBM Data Engineering Professional Certificate](https://www.coursera.org/professional-certificates/ibm-data-engineer)
- [ETL Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)