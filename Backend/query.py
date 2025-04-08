from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, mean, sum, asc, desc, when, avg, expr, coalesce, stddev, lit, hour
from geopy.geocoders import Nominatim
import plotly.graph_objects as go
from pyspark.sql import functions as F
import plotly.express as px

#spark = SparkSession.builder.master("local").appName("FlightAnalysis").getOrCreate()

# # Funzione per caricare i dati
# def carica_dati(path):
#     return spark.read.csv(path, header=True, inferSchema=True).cache()
#     #return spark.read.options(delimiter=',').csv(path, header=True, inferSchema=True).cache()

def carica_dati_tutti(directory_path):
    spark = SparkSession.builder.appName("Analisi Voli").getOrCreate()
    # Carica tutti i file CSV nella cartella
    df = spark.read.option("header", True).csv(directory_path + "/*.csv")
    return df


#query per analisi dati

from pyspark.sql.functions import col, avg, count

def statistiche_aeroporto(aeroporto,df=None):
    query = df.filter((col("Origin") == aeroporto) | (col("Dest") == aeroporto))

    totale_voli = query.count()
    # Media dei ritardi in partenza e in arrivo
    ritardo_medio_partenza = query.agg(avg(col("DepDelayMinutes"))).collect()[0][0]
    ritardo_medio_arrivo = query.agg(avg(col("ArrDelayMinutes"))).collect()[0][0]
    # Numero di voli cancellati
    voli_cancellati = query.filter(col("Cancelled") == 1).count()
    # Numero di voli dirottati
    voli_dirottati = query.filter(col("Diverted") == 1).count()
    # Creazione dizionario risultati
    statistiche = {
        "Aeroporto": aeroporto,
        "Totale Voli": totale_voli,
        "Ritardo Medio Partenza": ritardo_medio_partenza,
        "Ritardo Medio Arrivo": ritardo_medio_arrivo,
        "Voli Cancellati": voli_cancellati,
        "Voli Dirottati": voli_dirottati
    }
    return statistiche

def elenco_aeroporti_NY(df=None):
    aeroporti = df.select("Origin").distinct().orderBy("Origin")
    return aeroporti


def elenco_tutti_aeroporti(df=None):
    aeroporti = df.select("Origin").union(df.select("Dest")).distinct().orderBy("Origin")
    return aeroporti

def collegamenti_aeroporto(aeroporto, df=None):
    destinazioni = df.filter(df['Origin'] == aeroporto).select('Dest').distinct().collect()
    destinazioni_list = [row['Dest'] for row in destinazioni]
    return destinazioni_list


def aggregazione_voli_per_mese(df):
    agg_df = df.groupBy("Month").agg(F.count("*").alias("NumeroVoli")).orderBy("Month")
    return agg_df

def aggregazione_voli_per_giorno(df, month):
    agg_df = df.filter(F.col("Month") == month).groupBy("DayofMonth").agg(F.count("*").alias("NumeroVoli")).orderBy("DayofMonth")
    return agg_df


def calcola_puntualita(df):
    # Aggiungi una colonna per identificare se il volo è in orario, in ritardo o deviato
    df = df.withColumn(
        "Status",
        F.when((df["ArrDelay"] <= 0) & (df["Cancelled"] == 0), "In Orario")
        .when((df["ArrDelay"] > 0) & (df["Cancelled"] == 0), "In Ritardo")
        .otherwise("Deviato")
    )

    # Conta il numero di voli per ogni stato (In Orario, In Ritardo, Deviato)
    status_count = df.groupBy("Status").agg(F.count("*").alias("Count"))

    # Calcola il totale dei voli per calcolare la percentuale
    total_flights = df.count()

    # Calcola la percentuale per ciascuno stato
    status_percentage = status_count.withColumn(
        "Percentage", (status_count["Count"] / total_flights) * 100
    )

    return status_percentage

from pyspark.sql import functions as F

import folium
from streamlit.components.v1 import html


from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time

# Funzione per ottenere le coordinate con caching
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
import time


def get_airport_coords(iata, cache={}):
    if iata in cache:
        return cache[iata]

    geolocator = Nominatim(user_agent="airport_mapper", timeout=100)  # Aumentato il timeout a 100 secondi

    try:
        location = geolocator.geocode(f"{iata} airport")
        if location:
            coords = {"lat": location.latitude, "lon": location.longitude}
            cache[iata] = coords
            return coords
        else:
            cache[iata] = None
            return None
    except (GeocoderTimedOut, GeocoderUnavailable) as e:
        print(f"Errore nel recupero delle coordinate per {iata}: {e}")
        time.sleep(2)  # Attende 2 secondi prima di riprovare
        try:
            location = geolocator.geocode(f"{iata} airport")
            if location:
                coords = {"lat": location.latitude, "lon": location.longitude}
                cache[iata] = coords
                return coords
        except Exception as e2:
            print(f"Seconda eccezione per {iata}: {e2}")
        cache[iata] = None
        return None


def display_routes_map(df):
    import plotly.graph_objects as go
    import pandas as pd
    import streamlit as st
    from pyspark.sql import functions as F

    # Raggruppa per coppia di aeroporti e conta il numero di voli per rotta
    routes_df = df.groupBy("Origin", "Dest").agg(F.count("*").alias("flights"))
    routes_pd = routes_df.toPandas()

    # Ottieni le coordinate per ogni aeroporto presente nel dataset (sia in origine che in destinazione)
    # Creiamo un insieme di codici unici
    aeroporti = set(routes_pd["Origin"]).union(set(routes_pd["Dest"]))
    airport_coords = {}
    st.write("Recupero coordinate degli aeroporti, attendere...")  # Messaggio per l'utente
    for airport in aeroporti:
        coords = get_airport_coords(airport)
        if coords:
            airport_coords[airport] = coords

    # Filtra le rotte per cui esistono coordinate per entrambi gli aeroporti
    routes_pd = routes_pd[
        routes_pd["Origin"].isin(airport_coords.keys()) &
        routes_pd["Dest"].isin(airport_coords.keys())
    ]

    # Crea la figura per la mappa
    fig = go.Figure()

    # Aggiungi una traccia per ogni rotta: disegna una linea tra origine e destinazione
    for index, row in routes_pd.iterrows():
        origin = row["Origin"]
        dest = row["Dest"]
        flights = row["flights"]

        origin_coord = airport_coords[origin]
        dest_coord = airport_coords[dest]
#TODO modificare grandezza delle linee grafico
        fig.add_trace(go.Scattergeo(
            locationmode='USA-states',
            lon=[origin_coord["lon"], dest_coord["lon"]],
            lat=[origin_coord["lat"], dest_coord["lat"]],
            mode='lines',
            line=dict(width=max(flights/10, 1), color='blue'),
            opacity=0.5,
            hoverinfo='text',
            text=f"{origin} → {dest}<br>Voli: {flights}"
        ))

    # Aggiungi i marker per ogni aeroporto con coordinate disponibili
    for airport, coords in airport_coords.items():
        fig.add_trace(go.Scattergeo(
            locationmode='USA-states',
            lon=[coords["lon"]],
            lat=[coords["lat"]],
            mode='markers',
            marker=dict(size=8, color='red'),
            name=airport,
            hoverinfo='text',
            text=airport
        ))

    # Configura il layout della mappa
    fig.update_layout(
        title_text='Mappa delle Rotte Aeree',
        showlegend=False,
        geo=dict(
            scope='world',
            projection_type='equirectangular',
            showland=True,
            landcolor="rgb(217, 217, 217)",
            subunitwidth=1,
            countrywidth=1,
            subunitcolor="rgb(255, 255, 255)",
            countrycolor="rgb(255, 255, 255)"
        )
    )

    # Visualizza la mappa in Streamlit
    st.plotly_chart(fig)




def elenco_aeroporti(df):
    # Estrai le informazioni sugli aeroporti (ad esempio, codici IATA e latitudine/longitudine)
    aeroporti_df = df.select("Origin", "Dest", "OriginLat", "OriginLon").distinct()
    return aeroporti_df

def rotte_principali(df):
    # Calcolare il numero di voli per ogni coppia di aeroporti (origine, destinazione)
    rotte_df = df.groupBy("Origin", "Dest").agg(F.count("*").alias("NumeroVolamenti")).orderBy(F.col("NumeroVolamenti").desc())
    return rotte_df


def mappa_aeroporti(df):
    # Estrai gli aeroporti e le loro informazioni (ad esempio, latitudine, longitudine)
    aeroporti_df = elenco_aeroporti(df)

    # Converte il dataframe PySpark in Pandas per l'utilizzo con Plotly
    aeroporti_df_pandas = aeroporti_df.toPandas()

    # Crea la mappa Plotly
    fig = go.Figure(go.Scattergeo(
        locationmode='USA-states',
        lat=aeroporti_df_pandas['OriginLat'],
        lon=aeroporti_df_pandas['OriginLon'],
        mode='markers',
        marker=dict(
            size=6,
            color='blue',
            opacity=0.7,
            line=dict(width=0.5, color='black')
        ),
        text=aeroporti_df_pandas['Origin']  # Mostra il codice dell'aeroporto quando si passa sopra
    ))

    # Aggiungi titolo alla mappa
    fig.update_layout(
        title='Posizione degli Aeroporti',
        geo=dict(
            projection_type='albers usa',
            showland=True,
            landcolor='rgb(255, 255, 255)',
            subunitwidth=1,
            countrywidth=1,
            showcoastline=True,
            coastlinecolor='Black'
        ),
    )

    return fig


# Funzione per aggiungere le rotte principali sulla mappa
def aggiungi_rotte(df, fig):
    # Estrai le rotte principali
    rotte_df = rotte_principali(df)

    # Converte il dataframe PySpark in Pandas per l'utilizzo con Plotly
    rotte_df_pandas = rotte_df.toPandas()

    # Aggiungi le rotte come linee sulla mappa
    for _, row in rotte_df_pandas.iterrows():
        origine = row['Origin']
        destinazione = row['Dest']

        # Ottieni le coordinate latitudine/longitudine per origine e destinazione
        orig_lat = df.filter(df["Origin"] == origine).first()["OriginLat"]
        orig_lon = df.filter(df["Origin"] == origine).first()["OriginLon"]
        dest_lat = df.filter(df["Dest"] == destinazione).first()["OriginLat"]
        dest_lon = df.filter(df["Dest"] == destinazione).first()["OriginLon"]

        # Aggiungi una linea tra l'origine e la destinazione
        fig.add_trace(go.Scattergeo(
            locationmode='USA-states',
            lon=[orig_lon, dest_lon],
            lat=[orig_lat, dest_lat],
            mode='lines',
            line=dict(width=2, color='orange'),
            opacity=0.6,
            text=f"Voli: {row['NumeroVolamenti']}",  # Mostra il numero di voli sulla rotta
            hoverinfo="text"
        ))

    return fig

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut


def coordinate_aeroporto(aeroporto):
    geolocator = Nominatim(user_agent="airport_locator")

    try:
        location = geolocator.geocode(aeroporto, timeout=10)
        if location:
            return location.latitude, location.longitude
    except GeocoderTimedOut:
        print(f"Timeout nel recupero delle coordinate per {aeroporto}")

    return None, None


# Creazione della mappa
def mappa_aeroporti(df):
    aeroporti_df = df.select("Origin").distinct().collect()  # Selezioniamo gli aeroporti unici
    aeroporti = [row['Origin'] for row in aeroporti_df]

    # Otteniamo le coordinate geografiche di ciascun aeroporto
    airports_locations = []
    for aeroporto in aeroporti:
        lat, lon = coordinate_aeroporto(aeroporto)
        if lat and lon:
            airports_locations.append((aeroporto, lat, lon))

    # Creiamo la mappa
    fig = go.Figure(go.Scattermapbox(
        lat=[loc[1] for loc in airports_locations],
        lon=[loc[2] for loc in airports_locations],
        mode='markers',
        text=[loc[0] for loc in airports_locations],
        marker=dict(size=10, color='blue'),
        name='Aeroporti'
    ))

    fig.update_layout(
        mapbox_style="carto-positron",
        mapbox_zoom=3,
        mapbox_center={"lat": 37.0902, "lon": -95.7129},
        title="Mappa Aeroporti e Collegamenti Aerei",
    )

    return fig, airports_locations


def disegna_collegamenti(aeroporto_selezionato, df, airports_locations):
    destinazioni = collegamenti_aeroporto(aeroporto_selezionato, df)

    aeroporto_pos = next((loc for loc in airports_locations if loc[0] == aeroporto_selezionato), None)
    if not aeroporto_pos:
        return []

    linee = []
    for destinazione in destinazioni:
        dest_pos = next((loc for loc in airports_locations if loc[0] == destinazione), None)
        if dest_pos:
            linee.append(go.Scattermapbox(
                lat=[aeroporto_pos[1], dest_pos[1]],
                lon=[aeroporto_pos[2], dest_pos[2]],
                mode='lines',
                line=dict(width=2, color='gray'),
                name=f'{aeroporto_selezionato} -> {destinazione}'
            ))

    return linee

def calcola_kpi(df):
    # Totale voli
    totale_voli = df.count()

    # Voli cancellati: si assume che il campo "Cancelled" contenga 1 per i voli cancellati
    voli_cancellati = df.filter(col("Cancelled") == 1).count()

    # Voli deviati: si assume che il campo "Diverted" contenga 1 per i voli deviati
    voli_deviati = df.filter(col("Diverted") == 1).count()

    # Ritardo medio partenza (considera solo i voli dove DepDelay è valorizzato)
    ritardo_medio_partenza = df.filter(col("DepDelay").isNotNull()) \
        .agg(avg("DepDelay").alias("avgDepDelay")).collect()[0]["avgDepDelay"]

    # Ritardo medio arrivo
    ritardo_medio_arrivo = df.filter(col("ArrDelay").isNotNull()) \
        .agg(avg("ArrDelay").alias("avgArrDelay")).collect()[0]["avgArrDelay"]

    # Distanza media percorsa
    distanza_media = df.filter(col("Distance").isNotNull()) \
        .agg(avg("Distance").alias("avgDistance")).collect()[0]["avgDistance"]

    return {
        "totale_voli": totale_voli,
        "voli_cancellati": voli_cancellati,
        "voli_deviati": voli_deviati,
        "ritardo_medio_partenza": ritardo_medio_partenza,
        "ritardo_medio_arrivo": ritardo_medio_arrivo,
        "distanza_media": distanza_media
    }


















def tratte_con_maggiori_ritardi(dataInizio =None, dataFine=None, df=None):
    query = df.filter(col("ArrDelayMinutes").isNotNull())
    if dataInizio and dataFine:
        query = query.filter((col("FlightDate") >= dataInizio) & (col("FlightDate") <= dataFine))
    tratte = query.groupBy("Origin", "Dest").agg(
        sum("DepDelayMinutes").alias("MinutiRitardoPartenza"),
        sum("ArrDelayMinutes").alias("MinutiRitardoArrivo")
    ).withColumn(
        "TotaleMinutiRitardo", col("MinutiRitardoPartenza") + col("MinutiRitardoArrivo")
    ).select("Origin", "Dest", "TotaleMinutiRitardo").orderBy(desc("TotaleMinutiRitardo")).limit(10)
    return tratte

def voli_10_piu_puntuali(dataInizio=None, dataFine=None, df =None):
    query = df.filter((col("ArrDelayMinutes") == 0) & (col("DepDelayMinutes") == 0))
    if dataInizio and dataFine:
        query = query.filter((col("FlightDate") >= dataInizio) & (col("FlightDate") <= dataFine))
    puntuali = query.groupBy("Flights").count().orderBy(col("count").desc()).limit(10)
    return puntuali

def aereo_piu_frequentemente_utilizzato(dataInizio=None, dataFine=None, df=None):
    query = df.filter(col("Tail_Number").isNotNull())
    if dataInizio and dataFine:
        query = query.filter((col("FlightDate") >= dataFine) & (col("FlightDate") <= dataFine))
    aereo = query.groupBy("Tail_Number").count().orderBy(col("count").desc()).limit(1)
    return aereo

def tratte_piu_frequenti(data_inizio=None, data_fine=None, df=None):
    query = df.filter(col("Origin").isNotNull() & col("Dest").isNotNull())
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    tratte = query.groupBy("Origin", "Dest").count().orderBy(desc("count")).limit(10)
    return tratte

def ritardi_per_giorno_settimana(data_inizio=None, data_fine=None, df=None):
    query = df.filter(col("DayOfWeek").isNotNull() & col("ArrDelayMinutes").isNotNull())
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    ritardi = query.groupBy("DayOfWeek").agg(
        sum("ArrDelayMinutes").alias("TotaleRitardoArrivo")
    ).orderBy("DayOfWeek")
    return ritardi


def cancellazioni_per_giorno_settimana(data_inizio=None, data_fine=None, df=None):
    query = df.filter(col("Cancelled") == 1)
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    cancellazioni = query.groupBy("DayOfWeek").count().orderBy("DayOfWeek")
    return cancellazioni


def ritardo_medio_per_compagnia(data_inizio=None, data_fine=None, df=None):
    query = df.filter(col("Reporting_Airline").isNotNull() & col("ArrDelayMinutes").isNotNull())
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    compagnia = query.groupBy("Reporting_Airline").agg(
        avg("ArrDelayMinutes").alias("RitardoMedioArrivo")
    ).orderBy(desc("RitardoMedioArrivo"))
    return compagnia

def compagnia_meno_cancellazioni(data_inizio=None, data_fine=None, df=None):
    query = df.filter(col("Cancelled") == 1)
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    compagnia = query.groupBy("Reporting_Airline").count().orderBy("count").limit(1)
    return compagnia

def ritardo_medio_partenza_per_aeroporto(data_inizio=None, data_fine=None, df=None):
    query = df.filter(col("Origin").isNotNull() & col("DepDelayMinutes").isNotNull())
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    aeroporti = query.groupBy("Origin").agg(
        avg("DepDelayMinutes").alias("RitardoMedioPartenza")
    ).orderBy(desc("RitardoMedioPartenza"))
    return aeroporti

def voli_con_ritardi_superiori_a_30min(data_inizio=None, data_fine=None, df=None):
    query = df.filter(col("ArrDelayMinutes") > 30)
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    voli = query.select("FlightNumber", "Origin", "Dest", "ArrDelayMinutes").orderBy(desc("ArrDelayMinutes"))
    return voli


def aerei_piu_frequenti_da_aeroporto(aeroporto, data_inizio=None, data_fine=None, df=None):
    query = df.filter(col("Origin") == aeroporto)
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    aerei = query.groupBy("Tail_Number").count().orderBy(desc("count"))
    return aerei


def aerei_piu_frequenti_verso_aeroporto(aeroporto, data_inizio=None, data_fine=None, df=None):
    query = df.filter(col("Dest") == aeroporto)
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    aerei = query.groupBy("Tail_Number").count().orderBy(desc("count"))
    return aerei


def aeroporti_con_maggiori_ritardi_totali(data_inizio=None, data_fine=None, df=None):
    query = df.filter(col("Origin").isNotNull() & col("ArrDelayMinutes").isNotNull())
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    aeroporti = query.groupBy("Origin").agg(
        sum("ArrDelayMinutes").alias("TotaleRitardoArrivo")
    ).orderBy(desc("TotaleRitardoArrivo")).limit(10)
    return aeroporti


def tratte_piu_brevi(data_inizio=None, data_fine=None, df=None):
    query = df.filter(col("Distance").isNotNull())
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    tratte = query.groupBy("Origin", "Dest").agg(
        sum("Distance").alias("TotaleDistanza")
    ).orderBy("TotaleDistanza").limit(10)
    return tratte

def tratte_piu_lunghe(data_inizio=None, data_fine=None, df=None):
    query = df.filter(col("Distance").isNotNull())
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    tratte = query.groupBy("Origin", "Dest").agg(
        sum("Distance").alias("TotaleDistanza")
    ).orderBy(desc("TotaleDistanza")).limit(10)
    return tratte

def aeroporti_con_tempi_arrivo_piu_lunghi(data_inizio=None, data_fine=None, df=None):
    query = df.filter(col("Origin").isNotNull() & col("ArrDelayMinutes").isNotNull())
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    aeroporti = query.groupBy("Origin").agg(
        avg("ArrDelayMinutes").alias("RitardoMedioArrivo")
    ).orderBy(desc("RitardoMedioArrivo"))
    return aeroporti

def compagnie_piu_puntuali(data_inizio=None, data_fine=None, df=None):
    query = df.filter(col("Reporting_Airline").isNotNull())
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    compagnia = query.groupBy("Reporting_Airline").agg(
        avg("ArrDelayMinutes").alias("RitardoMedioArrivo")
    ).orderBy(desc("RitardoMedioArrivo"))
    return compagnia

def aerei_con_i_tempi_di_volo_piu_lunghi(data_inizio=None, data_fine=None, df=None):
    query = df.filter(col("Tail_Number").isNotNull() & col("AirTime").isNotNull())
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    aerei = query.groupBy("Tail_Number").agg(
        sum("AirTime").alias("TotaleTempoVolo")
    ).orderBy(desc("TotaleTempoVolo")).limit(10)
    return aerei

def giorni_con_piu_cancellazioni(data_inizio=None, data_fine=None, df=None):
    query = df.filter(col("Cancelled") == 1)
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    giorni = query.groupBy("FlightDate").count().orderBy(desc("count")).limit(10)
    return giorni

def voli_breve_durata(data_inizio=None, data_fine=None, df=None):
    query = df.filter(col("AirTime").isNotNull())
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    durata_media = query.agg(avg("AirTime").alias("DurataMedia")).collect()[0]["DurataMedia"]
    voli = query.filter(col("AirTime") < durata_media).select("FlightNumber", "AirTime").orderBy("AirTime")
    return voli

def ritardo_medio_per_stato(arrivo=None, data_inizio=None, data_fine=None, df=None):
    query = df.filter(col("DestStateName").isNotNull() & col("ArrDelayMinutes").isNotNull())
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    stati = query.groupBy("DestStateName").agg(
        avg("ArrDelayMinutes").alias("RitardoMedioArrivo")
    ).orderBy("RitardoMedioArrivo")
    return stati

def aeroporti_con_aerei_disponibili(data_inizio=None, data_fine=None, df=None):
    if data_inizio and data_fine:
        query = df.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine)).select("Tail_Number", "Origin").distinct()
    query = df.select("Tail_Number", "Origin").distinct().filter(col("Tail_Number").isNotNull())
    return query


def durata_media_per_compagnia(data_inizio=None, data_fine=None, df=None):
    query = df.filter(col("Reporting_Airline").isNotNull() & col("AirTime").isNotNull())
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    compagnia = query.groupBy("Reporting_Airline").agg(
        avg("AirTime").alias("DurataMediaVolo")
    ).orderBy("DurataMediaVolo")
    return compagnia


def tempo_medio_per_aeroporto(data_inizio=None, data_fine=None, df=None):
    query = df.filter(col("Origin").isNotNull() & col("AirTime").isNotNull())
    if data_inizio and data_fine:
        query = query.filter((col("FlightDate") >= data_inizio) & (col("FlightDate") <= data_fine))
    aeroporti = query.groupBy("Origin").agg(
        avg("AirTime").alias("DurataMediaVolo")
    ).orderBy("DurataMediaVolo")
    return aeroporti

def voli_per_mese(df=None):
    query = df.groupBy("Month").count().orderBy("Month")
    return query

def aeroporti_con_maggiori_ritardi_arrivo(df=None):
    query = df.filter(col("ArrDelayMinutes").isNotNull())
    aeroporti = query.groupBy("Origin").agg(
        avg("ArrDelayMinutes").alias("RitardoMedioArrivo")
    ).orderBy(desc("RitardoMedioArrivo"))
    return aeroporti

def voli_per_compagnia_aerea(df=None):
    query = df.groupBy("Reporting_Airline").count().orderBy(desc("count"))
    return query

def voli_cancellati_per_compagnia_aerea(df=None):
    query = df.filter(col("Cancelled") == 1)
    compagnie = query.groupBy("Reporting_Airline").count().orderBy(desc("count"))
    return compagnie

def voli_medi_per_mese(df=None):
    query = df.groupBy("Month").agg(count("FlightNumber").alias("NumeroVoli")).orderBy("Month")
    return query

def voli_con_ritardo_arrivo_grande(df=None):
    query = df.filter(col("ArrDel15") == 1)
    voli = query.groupBy("FlightNumber", "ArrDelayMinutes").count().orderBy(desc("ArrDelayMinutes"))
    return voli

def voli_con_maggiori_cancellazioni_per_compagnia(df=None):
    query = df.filter(col("Cancelled") == 1)
    cancellazioni = query.groupBy("Reporting_Airline").count().orderBy(desc("count"))
    return cancellazioni

def distanza_media_per_aeroporto(df=None):
    query = df.filter(col("Distance").isNotNull())
    distanze = query.groupBy("Origin", "Dest").agg(
        avg("Distance").alias("DistanzaMedia")
    ).orderBy("DistanzaMedia")
    return distanze


def aeroporti_con_maggiore_distanza_media(df=None):
    query = df.filter(col("Distance").isNotNull())
    aeroporti = query.groupBy("Origin").agg(
        avg("Distance").alias("DistanzaMediaVolo")
    ).orderBy(desc("DistanzaMediaVolo"))
    return aeroporti

def ritardi_per_ora(df=None):
    query = df.filter(col("DepDelayMinutes").isNotNull())
    query = query.withColumn("OraVolo", hour(col("DepTime")))
    ritardi = query.groupBy("OraVolo").agg(
        avg("DepDelayMinutes").alias("RitardoMedio")
    ).orderBy("OraVolo")
    return ritardi

def voli_per_stato_destinazione(df=None):
    query = df.groupBy("DestState").count().orderBy("DestState")
    return query

def voli_per_aeroporto_partenza(df=None):
    query = df.groupBy("Origin").count().orderBy(desc("count"))
    return query


def ritardo_medio_arrivo_destinazione(df=None):
    query = df.filter(col("ArrDelayMinutes").isNotNull())
    aeroporti = query.groupBy("Dest").agg(
        avg("ArrDelayMinutes").alias("RitardoMedioArrivo")
    ).orderBy(desc("RitardoMedioArrivo"))
    return aeroporti

def compagnie_aeree_con_maggiori_ritardi(df=None):
    query = df.filter(col("DepDelayMinutes").isNotNull())
    compagnie = query.groupBy("Reporting_Airline").agg(
        avg("DepDelayMinutes").alias("RitardoMedioPartenza")
    ).orderBy(desc("RitardoMedioPartenza"))
    return compagnie
def aeroporti_con_maggiori_ritardi_partenza(df=None):
    query = df.filter(col("DepDelayMinutes").isNotNull())
    aeroporti = query.groupBy("Origin").agg(
        avg("DepDelayMinutes").alias("RitardoMedioPartenza")
    ).orderBy(desc("RitardoMedioPartenza"))
    return aeroporti

def compagnie_con_ritardi_grandi(df=None):
    query = df.filter(col("DepDelayMinutes") > 30)
    compagnie = query.groupBy("Reporting_Airline").count().orderBy(desc("count"))
    return compagnie


def durata_totale_voli_per_compagnia(df=None):
    query = df.filter(col("AirTime").isNotNull())
    durata = query.groupBy("Reporting_Airline").agg(
        sum("AirTime").alias("DurataTotaleVoli")
    ).orderBy(desc("DurataTotaleVoli"))
    return durata

def voli_in_ritardo_per_causa(df=None):
    query = df.filter(col("DepDelayMinutes").isNotNull())
    cause = query.groupBy("CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay").count().orderBy(desc("count"))
    return cause

def join_coordinate_aeroporti(df_aeroporti, df=None):
    query = df.join(df_aeroporti, df["OriginAirportID"] == df_aeroporti["AirportID"], "left").select(
        "Origin", "Latitude", "Longitude"
    )
    return query

def compagnie_aeree_lungo_raggio(df=None):
    query = df.filter(col("Distance") > 1000)
    compagnie = query.groupBy("Reporting_Airline").count().orderBy(desc("count"))
    return compagnie
