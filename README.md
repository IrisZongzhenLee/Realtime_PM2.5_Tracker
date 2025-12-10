# US Air Quality PM2.5 Analytics Pipeline & Dashboard

This project builds an end-to-end analytics pipeline and dashboard for **real-time PM2.5 air quality in the United States**.

It pulls the latest readings from an external air quality API (e.g., OpenAQ), lands the data in Snowflake via a custom connector, models it with **dbt**, and visualizes current conditions in **Tableau** on an interactive US map.

The goal is to answer a simple question in an intuitive way:

> **What does PM2.5 air quality look like across the US today, and where should we be concerned?**

---

## Features

- **Daily refreshed data** for US PM2.5 (fine particulate matter) readings
- **Custom connector** (`connector.py`) to pull data from an external air quality API
- **dbt project** to transform raw sensor & reading data into clean, analytics-ready models:
  - Per-sensor latest PM2.5
  - US-level summary metrics
- **Tableau dashboard** (`airquality_visual.twbx`) with:
  - Interactive **map of monitoring locations**
  - **KPIs** (average PM2.5, worst locations, % in each AQ band)
  - Trend / distribution views for deeper analysis

---

## Tech Stack

- **Data Source:** External air quality API (e.g., OpenAQ)
- **Ingestion:** Custom Python connector (designed for Fivetran Connector)
- **Warehouse:** Snowflake
- **Transformations:** dbt (data build tool)
- **Visualization:** Tableau

---

## Repository Structure

```bash
.
├── airquality_project_dbt/        # dbt project
│   ├── models/
│   │   ├── marts/
│   │   │   ├── fact_us_airquality_summary.sql
│   │   │   ├── us_airquality_latest.sql
│   │   │   └── schema.yml
│   │   └── staging/
│   │       ├── stg_airquality_latest_pm25.sql
│   │       ├── stg_airquality_us_sensor_metadata.sql
│   │       └── schema.yml
│   ├── sources.yml                # Source definitions for raw tables
│   ├── dbt_project.yml            # dbt project config
│   └── profiles.yml               # Local dbt profile (usually not committed in real projects)
│
├── airquality_visual.twbx         # Tableau workbook for US PM2.5 dashboard
├── connector.py                   # Custom Python connector for air quality API
├── configuration.json             # Connector configuration (API URL, params, etc.)
├── requirements.txt               # Python dependencies for the connector
├── README.md                      # (this file)
└── .gitignore
