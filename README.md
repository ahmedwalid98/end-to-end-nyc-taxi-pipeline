# 🚖 End-to-End NYC Taxi Data Engineering Pipeline

> A production-inspired end-to-end Data Engineering project built with **Apache Airflow, PySpark, AWS S3, Glue, Athena, dbt, Great Expectations, and Terraform** following the **Medallion Architecture**.

---

## 📖 Overview

This project demonstrates how to build a modern cloud-based data platform capable of ingesting, validating, transforming, cataloging, and modeling large datasets.

The pipeline downloads monthly NYC Yellow and Green Taxi trip data, processes it using PySpark, validates data quality with Great Expectations, stores data using a Medallion Architecture in Amazon S3, catalogs datasets using AWS Glue, and builds analytical models with dbt.

The project focuses on applying data engineering best practices rather than simply moving data from one place to another.

---

# Architecture

```text
                        Monthly Schedule
                               │
                               ▼
                     Apache Airflow DAG
                               │
        ┌──────────────────────┼──────────────────────┐
        │                      │                      │
        ▼                      ▼                      ▼
 Download Green Trips   Download Yellow Trips     Logging
        │                      │
        └──────────────┬───────┘
                       ▼
                 Bronze Layer (S3)
                       │
                       ▼
              PySpark Transformation
      • Standardize Schemas
      • Merge Yellow & Green Trips
      • Add Partition Columns
      • Business Rule Validation
                       │
                       ▼
           Split Valid / Invalid Records
                 │                  │
                 ▼                  ▼
          Quarantine Layer     Great Expectations
                                      │
                                      ▼
                             Validation Successful?
                                  │          │
                                  ▼          ✗
                           Silver Layer    Fail DAG
                                  │
                                  ▼
                          AWS Glue Crawler
                                  │
                                  ▼
                         AWS Glue Catalog
                                  │
                                  ▼
                               Athena
                                  │
                                  ▼
                                 dbt
                                  │
                                  ▼
                             Gold Layer
```

---

# Technology Stack

| Category       | Technologies           |
| -------------- | ---------------------- |
| Orchestration  | Apache Airflow         |
| Processing     | Apache Spark (PySpark) |
| Storage        | Amazon S3              |
| Data Catalog   | AWS Glue               |
| Query Engine   | Amazon Athena          |
| Transformation | dbt                    |
| Data Quality   | Great Expectations     |
| Infrastructure | Terraform              |
| Language       | Python                 |
| File Format    | Parquet                |

---

# Medallion Architecture

The project follows the Bronze → Silver → Gold architecture.

## Bronze

Raw taxi datasets are downloaded directly from the NYC Taxi public dataset and stored without modifications.

Purpose:

* Preserve raw data
* Immutable storage
* Recovery point
* Historical archive

---

## Silver

The Silver layer performs all cleansing and standardization.

Transformations include:

* Standardizing Yellow and Green taxi schemas
* Merging datasets
* Adding partition columns
* Business rule validation
* Great Expectations validation
* Separating invalid records into a quarantine layer

Only validated data reaches the Silver layer.

---

## Quarantine

Instead of discarding invalid records, they are stored separately.

Examples include:

* Negative trip distance
* Negative fare amount
* Invalid passenger count
* Missing required fields

Keeping invalid data enables investigation without affecting downstream analytics.

---

## Gold

dbt builds business-ready analytical models from the Silver layer.

These models are optimized for reporting and analytics rather than ingestion.

---

# Pipeline Flow

## 1. Data Ingestion

Airflow runs on a monthly schedule.

For every execution:

* Downloads Green Taxi trips
* Downloads Yellow Taxi trips
* Stores both datasets in the Bronze layer

---

## 2. Data Processing

PySpark reads Bronze data and performs:

* Schema standardization
* Column renaming
* Data type normalization
* Dataset merging
* Partition creation

---

## 3. Data Validation

Validation occurs in two stages.

### Business Rule Validation

Spark separates records into:

* Valid records
* Invalid records

Invalid records are written to the Quarantine layer.

---

### Great Expectations

Additional data quality checks include:

* Required columns are not null
* Passenger count within acceptable range
* Positive trip distance
* Positive fare amount
* Positive total amount

If validation fails, the Airflow DAG stops before writing the Silver dataset.

---

## 4. Cataloging

After writing the Silver layer:

* AWS Glue Crawler scans S3
* Updates the Glue Data Catalog
* Makes data immediately queryable in Athena

---

## 5. Analytics

dbt reads the Silver tables from Athena and builds Gold models for analytics.

This separation allows Spark to focus on heavy transformations while dbt focuses on business logic.

---

# Project Structure

```text
end-to-end-nyc-taxi-pipeline
│
├── airflow/
│   └── dags/
│
├── spark/
│   ├── extract_data.py
│   ├── transform.py
│   ├── validate.py
│   └── load_data.py
│   
│
├── dbt/
│   ├── models/
│   ├── macros/
│   └── dbt_project.yml
│
├── terraform/
│
├── rules/
│
├── docs/
├── utils/
│
└── README.md
```

---

# AWS Services Used

* Amazon S3
* AWS Glue
* AWS Glue Crawler
* AWS Glue Data Catalog
* Amazon Athena
* IAM
* Terraform

---

# Logging

The pipeline includes structured logging across every stage.

Examples:

* Download started
* Bronze upload completed
* Spark transformation completed
* Great Expectations results
* Silver upload completed
* Glue crawler status
* dbt execution

This makes debugging significantly easier than relying on print statements.

---

# Error Handling

The pipeline follows a fail-fast approach.

Examples:

* Download failures stop ingestion.
* Failed Great Expectations validation stops the DAG.
* Spark exceptions are logged and propagated.
* dbt failures fail the Airflow task.
* Invalid records are quarantined instead of discarded.

---

# Data Quality Strategy

The project combines two complementary validation approaches.

### Spark

Fast row-level business rule validation.

Examples:

* Passenger count
* Negative values
* Required business logic

### Great Expectations

Dataset-level quality validation.

Examples:

* Null checks
* Value ranges
* Required columns

Using both approaches provides both performance and maintainability.

---

# What I Learned

This project helped me gain practical experience with modern data engineering tools and patterns.

### Apache Airflow

* TaskFlow API
* Dynamic Task Mapping
* DAG orchestration
* Scheduling
* Task dependencies
* Logging

### Apache Spark

* Distributed DataFrames
* Schema evolution
* Partitioning
* DataFrame transformations
* Parquet optimization
* Spark performance considerations

### AWS

* Amazon S3
* Glue Catalog
* Glue Crawlers
* Athena
* IAM
* Infrastructure as Code with Terraform

### dbt

* Sources
* Models
* Materializations
* Data transformations
* Modular SQL development

### Data Engineering Concepts

* Medallion Architecture
* ETL pipelines
* Data Quality
* Quarantine pattern
* Metadata management
* Separation of responsibilities
* Analytics-ready datasets

---

# Challenges

Some of the challenges solved while building this project include:

* Handling different schemas between Yellow and Green Taxi datasets.
* Designing a reusable Spark transformation pipeline.
* Integrating Great Expectations into an Airflow workflow.
* Automatically updating the Glue Catalog after every load.
* Keeping Spark transformations separate from analytical modeling in dbt.
* Debugging dependency conflicts between Python packages.
* Designing a scalable Medallion Architecture.

---

# Future Improvements

Planned enhancements include:

* Incremental Spark processing
* Incremental dbt models
* GitHub Actions CI/CD
* Unit tests
* Data freshness monitoring
* OpenMetadata integration
* Iceberg tables
* Cost monitoring
* Slack notifications
* Spark cluster deployment

---

# Running the Project

1. Provision AWS infrastructure using Terraform.
2. Start Airflow.
3. Trigger the monthly ingestion DAG.
4. Spark writes Bronze, Quarantine, and Silver datasets.
5. Glue Crawler updates the Glue Catalog.
6. dbt builds Gold models.
7. Query analytical tables using Athena.

---

# Key Takeaways

This project demonstrates how multiple technologies work together to build a complete data platform rather than isolated ETL scripts.

It showcases:

* End-to-end pipeline orchestration
* Distributed data processing
* Cloud-native storage
* Automated data quality
* Metadata management
* Analytics engineering
* Infrastructure as Code
* Production-inspired software engineering practices
