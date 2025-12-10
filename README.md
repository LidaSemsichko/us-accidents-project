# US Accidents. End-to-End Big Data Pipeline Project

This project implements a full Bronze → Silver → Gold data pipeline for the US Accidents Dataset (March 2023) using PySpark, Delta Lake, and Databricks Workflows.
It also includes a dashboard and supports future extensions such as ML classification model.

## Project Overview

The goal of the project is to build a production-ready big data solution that:
Ingests and validates raw accident data
Cleans, transforms, enriches, and structures it

Produces analytical-ready datasets

Supports dashboards and future ML tasks

Follows best practices of data engineering

The pipeline is designed to be scalable, modular, and fully automated.

## Project Structure
/etl_pipeline.py        # Main ETL pipeline (Bronze → Silver → Gold)
/dashboards/            # Visual analytics (Databricks SQL + dashboard)
/docs/                  # Documentation for final submission
/README.md              # This file

## Architecture Overview
Bronze Layer

Reads raw CSV files

Cleans column names

Adds metadata (ingestion_time, event_date)

Writes Delta + Parquet

Performs schema and row-count validation

Silver Layer

Removes low-quality columns

Converts units to metric

Performs data cleaning, imputation, outlier handling

Adds feature engineering:

accident duration

start hour, weekday, weekend

weather-based risk indicator

Validates key fields (lat/lng, severity, distances)

Writes partitioned Delta tables

Gold Layer

Produces aggregated KPI datasets:

per-state accident metrics

per-hour accident patterns

These tables feed dashboards and analytics

## Pipeline Execution
Run the entire ETL pipeline:
python etl_pipeline.py '{"raw_filename": "US_Accidents_March23_trimmed.csv"}'


Or in Databricks Workflow — simply pass a parameter { "raw_filename": "..." }.

## Key Features
✔ Validation on every stage
✔ Intelligent missing-value imputation (mean/median based on skewness)
✔ IQR winsorization for continuous outliers
✔ Feature engineering for weather, time, and duration
✔ Partitioning for query performance
✔ Delta Lake storage & schema enforcement
✔ Modular functions for maintainability
✔ Logging with timestamps
## Dashboard

The dashboard built on top of Gold tables includes:

Accident count by state

Severity distribution

Hour-of-day accident patterns

Weather-related accident analysis

This demonstrates how downstream analytics consume cleaned data.

## Future Extensions

Possible extensions (optional for this project):

Predictive model (accident severity classification)

Real-time streaming ingestion

Live accident alerting system

Geospatial clustering

## Requirements

Python 3.9+

Databricks Runtime with PySpark

Delta Lake

## Authors

Vika, Edvard, Anna, Lida, Dima

Big Data Final Project, 2025