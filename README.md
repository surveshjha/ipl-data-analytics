# 🏏 IPL Data Analytics Project

This project performs data extraction, transformation, and analytics on IPL (Indian Premier League) data using PySpark, Jupyter Notebooks, and Google Cloud Storage. The objective is to derive rich insights from cricket match data and enable reporting via visualization and BI tools.

---

## 📌 Project Architecture


### ✅ Components:
- **Extraction**: Manual IPL dataset uploaded to Google Cloud Storage.
- **Transformation**: Performed locally using PySpark and Jupyter Notebooks.
- **Load**: Cleaned datasets are stored in GCS and FileStore.
- **BI/Analytics**: Data is visualized using Jupyter and Looker.

> ![Architecture](./IPL-DATA-ARCHITECHTURE .jpeg)

---

## 🗂 Folder Structure

```bash
IPL-ANALYTICS/
│
├── raw-data/                # Raw IPL CSV files (Ball_by_Ball, Match, Player, PlayerMatch, Team)
├── cleaned-data/            # Output of cleaned & transformed data
├── jars/                    # GCS connector JAR for PySpark
├── projects/
│   ├── ipynb/               # Jupyter notebooks for E2E ETL
│   │   ├── 00_Set_Environment.ipynb
│   │   ├── 01_Ingestion.ipynb
│   │   ├── 02_Cleaning.ipynb
│   │   ├── 03_Transformation.ipynb
│   │   └── 04_Data_Visualisation.ipynb
│   └── py/                  # (Optional) Python scripts version of ETL
│
├── .gitignore
├── IPL-DATA-ARCHITECHTURE.jpeg
├── DATA-MODELLING.jpeg
└── README.md
