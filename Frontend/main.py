import streamlit as st
from streamlit_option_menu import option_menu
from pyspark import StorageLevel
from pyspark.sql import SparkSession
import pandas as pd
import sys
import os


# Layout Streamlit
st.set_page_config(page_title="Analisi Voli", layout="wide")

os.environ["PYSPARK_PYTHON"] = "python"
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


# Import delle funzioni di visualizzazione ottimizzate
from Frontend.Aeroporto import mostra_aeroporto
from Frontend.CompagnieAeree import mostra_compagnia
from Frontend.Citta import mostra_citta
from Frontend.RitardiCancellazioni import mostra_ritardi_cancellazioni
from Frontend.TratteAeree import mostra_tratte
from Frontend.Home import mostra_home
from Frontend.AI import mostra_ai
from Frontend.Graph import mostra_graph


# Impostazioni Spark e caching
@st.cache_resource
def init_spark(app_name: str = "ProgettoVoli") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        # Configurazioni di ottimizzazione
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
def load_flights(file_paths: list[str]) -> 'DataFrame':
    spark = init_spark()
    parquet_path = "./dataset/voli_parquet"

    if not os.path.exists(parquet_path):
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(file_paths)
        )
        #df = df.repartition("Origin").persist(StorageLevel.MEMORY_ONLY)
        df.count()
        df.write.mode("overwrite").parquet(parquet_path)
    else:
        df = spark.read.parquet(parquet_path).persist(StorageLevel.MEMORY_ONLY)
        df.count()

    return df


@st.cache_data(show_spinner=False)
def load_airports(csv_path: str) -> pd.DataFrame:
    # Carica solo colonne necessarie
    df = pd.read_csv(csv_path)
    return df

# Inizializza dati
file_paths = [
    "./Preprocessing/Puliti/On_Time_Reporting_Carrier_*_clean.csv"
]
aeroporti_info = load_airports("./distutils/usa-airports.csv")
flights_df = load_flights(file_paths)


# Definisce il menu di navigazione
menu_options = [
    ("Home", "house"),
    ("Aeroporto", "geo-alt"),
    ("Compagnie aeree", "airplane"),
    ("Città", "building"),
    ("Ritardi e cancellazioni", "clock-history"),
    ("Tratte aeree", "map"),
    ("Previsione AI", "cpu"),
    ("Graph", "graph-up"),
]

choice = option_menu(
    menu_title=None,
    options=[opt[0] for opt in menu_options],
    icons=[opt[1] for opt in menu_options],
    default_index=0,
    orientation="horizontal",
    styles={
        "container": {"background-color": "#f0f2f6", "padding": "0"},
        "nav-link": {"font-size": "16px", "padding": "10px 0"},
        "nav-link-selected": {"background-color": "#0275d8", "color": "white"}
    }
)

# Mappa scelta a funzione
page_funcs = {
    "Home": lambda: mostra_home(flights_df, aeroporti_info),
    "Aeroporto": lambda: mostra_aeroporto(flights_df, aeroporti_info),
    "Compagnie aeree": lambda: mostra_compagnia(flights_df, aeroporti_info),
    "Città": lambda: mostra_citta(flights_df, aeroporti_info),
    "Ritardi e cancellazioni": lambda: mostra_ritardi_cancellazioni(flights_df),
    "Tratte aeree": lambda: mostra_tratte(flights_df, aeroporti_info),
    "Previsione AI": lambda: mostra_ai(flights_df),
    "Graph": lambda: mostra_graph(flights_df)
}

# Esecuzione della pagina selezionata
page_funcs[choice]()
