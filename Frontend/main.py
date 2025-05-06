import streamlit as st
from streamlit_option_menu import option_menu
import sys
import os
from pyspark import StorageLevel
from pyspark.sql import SparkSession
import pandas as pd

os.environ["PYSPARK_PYTHON"] = "python"
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Frontend.Aeroporto import mostra_aeroporto
from Frontend.CompagnieAeree import mostra_compagnia
from Frontend.Citta import mostra_citta
from Frontend.RitardiCancellazioni import mostra_ritardi_cancellazioni
from Frontend.TratteAeree import mostra_tratte
from Frontend.Home import mostra_home
from Frontend.AI import mostra_ai

file_paths = [
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Preprocessing/Puliti/On_Time_Reporting_Carrier_1_clean.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Preprocessing/Puliti/On_Time_Reporting_Carrier_2_clean.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Preprocessing/Puliti/On_Time_Reporting_Carrier_3_clean.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Preprocessing/Puliti/On_Time_Reporting_Carrier_4_clean.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Preprocessing/Puliti/On_Time_Reporting_Carrier_5_clean.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Preprocessing/Puliti/On_Time_Reporting_Carrier_6_clean.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Preprocessing/Puliti/On_Time_Reporting_Carrier_7_clean.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Preprocessing/Puliti/On_Time_Reporting_Carrier_8_clean.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Preprocessing/Puliti/On_Time_Reporting_Carrier_9_clean.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Preprocessing/Puliti/On_Time_Reporting_Carrier_10_clean.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Preprocessing/Puliti/On_Time_Reporting_Carrier_11_clean.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Preprocessing/Puliti/On_Time_Reporting_Carrier_12_clean.csv",

]
aeroporti_info = pd.read_csv("C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/distutils/usa-airports.csv")

def get_spark_session(app_name="ProgettoVoli"):
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "16g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

spark = get_spark_session()


df = None
for file in file_paths:
    temp_df = spark.read.csv(file, header=True, inferSchema=True)
    df = temp_df if df is None else df.union(temp_df)
df.persist(StorageLevel.MEMORY_AND_DISK)


def main():
    st.set_page_config(page_title="Analisi Voli", layout="wide")
    menu_items = [
        "Home",
        "Aeroporto",
        "Compagnie aeree",
        "Città",
        "Ritardi e cancellazioni",
        "Tratte aeree",
        "Previsione AI"
    ]
    menu_icons = [
        "house",
        "geo-alt",
        "airplane",
        "building",
        "clock-history",
        "map",
        "cpu"
    ]

    scelta = option_menu(
        menu_title=None,
        options=menu_items,
        icons=menu_icons,
        default_index=0,
        orientation="horizontal",
        styles={
            "container": {"padding": "0px", "background-color": "#f0f2f6"},
            "nav-link": {
                "font-size": "16px",
                "text-align": "center",
                "margin": "0px",
                "padding": "10px 0px",
                "flex": "1"
            },
            "nav-link-selected": {
                "background-color": "#0275d8",
                "color": "white",
                "font-weight": "bold"
            }
        }
    )

    if scelta == "Home":
        mostra_home(df, aeroporti_info)

    elif scelta == "Aeroporto":
        mostra_aeroporto(df, aeroporti_info)


    elif scelta == "Compagnie aeree":
        mostra_compagnia(df, aeroporti_info)

    elif scelta == "Città":
        mostra_citta(df,aeroporti_info)


    elif scelta == "Ritardi e cancellazioni":
        mostra_ritardi_cancellazioni(df)

    elif scelta == "Tratte aeree":
        mostra_tratte(df, aeroporti_info)


    elif scelta == "Previsione AI":
        mostra_ai(df)

if __name__ == "__main__":
    main()
