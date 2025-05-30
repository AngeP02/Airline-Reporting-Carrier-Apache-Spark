import streamlit as st
from streamlit_option_menu import option_menu
from pyspark import StorageLevel
from pyspark.sql import SparkSession
import pandas as pd
import sys
import os

st.set_page_config(page_title="Analisi Voli", layout="wide")

os.environ["PYSPARK_PYTHON"] = "python"
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Frontend.Aeroporto import mostra_aeroporto
from Frontend.CompagnieAeree import mostra_compagnia
from Frontend.Citta import mostra_citta
from Frontend.RitardiCancellazioni import mostra_ritardi_cancellazioni
from Frontend.TratteAeree import mostra_tratte
from Frontend.Home import mostra_home
from Frontend.AI import mostra_ai
from Frontend.GraphFrame import mostra_grafo

@st.cache_resource
def init_spark(app_name: str = "ProgettoVoli") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "18g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "104857600")  # 100 MB
        .config("spark.jars.packages","graphframes:graphframes:0.8.2-spark3.2-s_2.12")
        .getOrCreate()
    )
    checkpoint_dir = "./tmp/checkpoints"
    spark.sparkContext.setCheckpointDir(checkpoint_dir)
    return spark


@st.cache_resource
def carica_dati(file_paths: list[str]) -> 'DataFrame':
    spark = init_spark()
    parquet_path = "./Dati/voli_parquet"
    if not os.path.exists(parquet_path):
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(file_paths)
        )
        df.count()
        df.write.mode("overwrite").parquet(parquet_path)
    else:
        df = spark.read.parquet(parquet_path).persist(StorageLevel.MEMORY_ONLY)
        df.count()
    return df


@st.cache_data(show_spinner=False)
def carica_coordinate_aeroporto(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    return df

path_dataset_pulito = ["./Preprocessing/Puliti/On_Time_Reporting_Carrier_*_clean.csv"]
aeroporti_info = carica_coordinate_aeroporto("./distutils/usa-airports.csv")
voli_df = carica_dati(path_dataset_pulito)

menu = [
    ("Home", "house"),
    ("Aeroporto", "geo-alt"),
    ("Compagnie aeree", "airplane"),
    ("Città", "building"),
    ("Ritardi e cancellazioni", "clock-history"),
    ("Tratte aeree", "map"),
    ("Previsione AI", "cpu"),
    ("Grafi", "graph-up"),
]

scelta = option_menu(
    menu_title=None,
    options=[opt[0] for opt in menu],
    icons=[opt[1] for opt in menu],
    default_index=0,
    orientation="horizontal",
    styles={
        "container": {"background-color": "#f0f2f6", "padding": "0"},
        "nav-link": {"font-size": "16px", "padding": "10px 0"},
        "nav-link-selected": {"background-color": "#0275d8", "color": "white"}
    }
)

funzionalita_pagine = {
    "Home": lambda: mostra_home(voli_df, aeroporti_info),
    "Aeroporto": lambda: mostra_aeroporto(voli_df, aeroporti_info),
    "Compagnie aeree": lambda: mostra_compagnia(voli_df),
    "Città": lambda: mostra_citta(voli_df, aeroporti_info),
    "Ritardi e cancellazioni": lambda: mostra_ritardi_cancellazioni(voli_df),
    "Tratte aeree": lambda: mostra_tratte(voli_df, aeroporti_info),
    "Previsione AI": lambda: mostra_ai(voli_df),
    "Grafi": lambda: mostra_grafo(voli_df, aeroporti_info)
}

funzionalita_pagine[scelta]()
