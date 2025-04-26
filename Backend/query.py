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

def mappa_rotte(df, aeroporti_dict):
    fig = go.Figure()

    for _, row in df.iterrows():
        # Estrai le coordinate degli aeroporti di origine e destinazione
        lat_origin, lon_origin = aeroporti_dict.get(row['Origin'], (None, None))
        lat_dest, lon_dest = aeroporti_dict.get(row['Dest'], (None, None))

        # Verifica se le coordinate sono valide
        if lat_origin and lon_origin and lat_dest and lon_dest:
            fig.add_trace(go.Scattergeo(
                locationmode='ISO-3',
                locations=[row['Origin'], row['Dest']],
                line=dict(
                    width=2,
                    color='rgba(0, 0, 255, 0.5)'  # Colore blu con trasparenza
                ),
                opacity=0.7
            ))

    fig.update_layout(
        geo=dict(
            scope='world',
            projection_type='mercator',
            showland=True,
            landcolor='rgb(255, 255, 255)',
            countrycolor='rgb(204, 204, 204)'
        ),
        title="Mappa delle Rotte Aeree"
    )

    return fig

def get_lat_lon(iata_code, aeroporti_dict):
    return aeroporti_dict.get(iata_code, (None, None))

def mappa_rotteNO(df, aeroporti_info):
    """
    Funzione per generare una mappa interattiva delle rotte aeree.

    :param df: DataFrame contenente i voli, con le colonne 'Origin' e 'Dest'.
    :param aeroporti_info: DataFrame contenente le informazioni sugli aeroporti (inclusi latitudine e longitudine).
    :return: grafico Plotly con la mappa interattiva delle rotte.
    """
    # Creiamo un dizionario per ottenere latitudine e longitudine per ogni aeroporto
    aeroporti_dict = aeroporti_info.set_index('iata')[['latitude', 'longitude']].to_dict('index')

    # Creiamo una lista per memorizzare le rotte
    rotte = []

    # Calcoliamo la frequenza dei voli tra ogni coppia di aeroporti (origine e destinazione)
    rotte_frequenza = df.groupBy('Origin', 'Dest').count().toPandas()

    for index, row in rotte_frequenza.iterrows():
        origine = row['Origin']
        destinazione = row['Dest']
        frequenza = row['count']

        # Otteniamo latitudine e longitudine per origine e destinazione
        lat_origine, lon_origine = aeroporti_dict.get(origine, (None, None))
        lat_destinazione, lon_destinazione = aeroporti_dict.get(destinazione, (None, None))

        if lat_origine is not None and lat_destinazione is not None:
            rotte.append({
                'origine': origine,
                'destinazione': destinazione,
                'frequenza': frequenza,
                'lat_origine': lat_origine,
                'lon_origine': lon_origine,
                'lat_destinazione': lat_destinazione,
                'lon_destinazione': lon_destinazione
            })

    # Creiamo la mappa con Plotly
    fig = go.Figure()

    for rotta in rotte:
        fig.add_trace(go.Scattergeo(
            locationmode='USA-states',
            lon=[rotta['lon_origine'], rotta['lon_destinazione']],
            lat=[rotta['lat_origine'], rotta['lat_destinazione']],
            mode='lines',
            line=dict(width=rotta['frequenza'] / 10, color='blue', opacity=0.5),
            hoverinfo='text',
            text=f"Rotta: {rotta['origine']} -> {rotta['destinazione']}<br>Frequenza: {rotta['frequenza']}"
        ))

    # Aggiungiamo le informazioni della mappa
    fig.update_layout(
        title="Mappa Interattiva delle Rotte Aeree",
        geo=dict(
            scope='world',
            showland=True,
            landcolor='white',
            showlakes=True,
            lakecolor='rgb(255, 255, 255)',
            projection_type='mercator'
        ),
        showlegend=False
    )

    return fig


def statistiche_aeroporto(aeroporto,df=None):
    query = df.filter((col("Origin") == aeroporto) | (col("Dest") == aeroporto))

    totale_voli = query.count()
    # Media dei ritardi in partenza e in arrivo
    res = query.agg(
        avg("DepDelayMinutes").alias("ritardo_partenza"),
        avg("ArrDelayMinutes").alias("ritardo_arrivo")
    ).collect()[0]
    ritardo_medio_partenza = res["ritardo_partenza"]
    ritardo_medio_arrivo = res["ritardo_arrivo"]
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

def statistiche_aeroportoNO(df, codice_aeroporto):
    query = df.filter(
        (col("Origin") == codice_aeroporto) | (col("Dest") == codice_aeroporto)
    )

    # Raggruppo tutte le metriche in una singola azione
    result = query.agg(
        avg("DepDelayMinutes").alias("ritardo_partenza"),
        avg("ArrDelayMinutes").alias("ritardo_arrivo"),
        count("*").alias("totale_voli")
    ).collect()[0]

    return (
        round(result["ritardo_partenza"], 2) if result["ritardo_partenza"] else 0,
        round(result["ritardo_arrivo"], 2) if result["ritardo_arrivo"] else 0,
        result["totale_voli"]
    )


def calcola_kpiNO(df, codice_aeroporto):
    query = df.filter((col("Origin") == codice_aeroporto) | (col("Dest") == codice_aeroporto))

    stats = query.agg(
        count("*").alias("num_voli"),
        avg("DepDelayMinutes").alias("ritardo_medio_partenza"),
        avg("ArrDelayMinutes").alias("ritardo_medio_arrivo")
    ).collect()[0]

    return {
        "numero_voli": stats["num_voli"],
        "ritardo_medio_partenza": round(stats["ritardo_medio_partenza"], 2) if stats["ritardo_medio_partenza"] else 0,
        "ritardo_medio_arrivo": round(stats["ritardo_medio_arrivo"], 2) if stats["ritardo_medio_arrivo"] else 0
    }


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


def coordinate_aeroporto(codice_aeroporto, coords_dict):
    return coords_dict.get(codice_aeroporto, (None, None))

def estrai_coordinate_aeroporti(df):
    coords_df = df.select("Origin", "OriginLat", "OriginLon").distinct()
    return {row["Origin"]: (row["OriginLat"], row["OriginLon"]) for row in coords_df.collect()}


def coordinate_aeroportoNO(aeroporto):
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

    # Voli cancellati
    voli_cancellati = df.filter(col("Cancelled") == 1).count()

    # Voli deviati
    voli_deviati = df.filter(col("Diverted") == 1).count()

    # Ritardo medio partenza
    ritardo_medio_partenza = df.filter(col("DepDelay").isNotNull()) \
        .agg(avg("DepDelay").alias("avgDepDelay")).collect()[0]["avgDepDelay"]

    # Ritardo medio arrivo
    ritardo_medio_arrivo = df.filter(col("ArrDelay").isNotNull()) \
        .agg(avg("ArrDelay").alias("avgArrDelay")).collect()[0]["avgArrDelay"]

    # Distanza media
    distanza_media = df.filter(col("Distance").isNotNull()) \
        .agg(avg("Distance").alias("avgDistance")).collect()[0]["avgDistance"]

    # Percentuale voli cancellati
    percentuale_cancellati = (voli_cancellati / totale_voli) * 100

    return {
        "totale_voli": totale_voli,
        "voli_cancellati": voli_cancellati,
        "voli_deviati": voli_deviati,
        "ritardo_medio_partenza": ritardo_medio_partenza,
        "ritardo_medio_arrivo": ritardo_medio_arrivo,
        "distanza_media": distanza_media,
        "percentuale_cancellati": percentuale_cancellati,
    }


from pyspark.sql import functions as F
from pyspark.sql.window import Window

def indicatori_di_performance(df):
    risultati = {}



    # 2. Top 5 aeroporti più congestionati (ritardo medio in partenza)
    congestionati = (df.groupBy("Origin", "OriginCityName")
                       .agg(F.avg("DepDelayMinutes").alias("RitardoMedioPartenza"))
                       .orderBy(F.desc("RitardoMedioPartenza"))
                       .limit(5))
    risultati["Top5_Aeroporti_Congestionati"] = congestionati

    # 3. Top 5 rotte più frequenti
    rotte_frequenti = (df.groupBy("Origin", "Dest")
                         .count()
                         .orderBy(F.desc("count"))
                         .limit(5))
    risultati["Top5_Rotte_Frequenti"] = rotte_frequenti

    # 5. Compagnie aeree più puntuali (minor ritardo medio in arrivo)
    compagnie_puntuali = (df.groupBy("Reporting_Airline", "IATA_CODE_Reporting_Airline")
                            .agg(F.avg("ArrDelayMinutes").alias("RitardoMedioArrivo"))
                            .orderBy("RitardoMedioArrivo")
                            .filter(F.col("RitardoMedioArrivo").isNotNull())
                            .limit(5))
    risultati["Top5_Compagnie_Puntuali"] = compagnie_puntuali

    # 6. Giorni della settimana con più ritardi
    ritardi_giorno = (df.groupBy("DayOfWeek")
                        .agg(F.avg("ArrDelayMinutes").alias("RitardoMedio"))
                        .orderBy(F.desc("RitardoMedio")))
    risultati["Ritardi_Per_GiornoDellaSettimana"] = ritardi_giorno

    return risultati


def analisi_completa_ritardi(df):
    """
    Funzione ottimizzata che calcola più metriche in una sola passata sui dati.
    Riduce lo shuffling eseguendo più analisi insieme.
    """
    # Selezioniamo solo le colonne necessarie per tutte le analisi
    cols_needed = ["Reporting_Airline", "Origin", "Dest", "DepDelay", "ArrDelay",
                   "DepDel15", "ArrDel15", "Cancelled", "DayOfWeek", "Month"]

    df_filtered = df.select(*cols_needed)
    df_filtered.cache()  # Cache per riutilizzo

    # Effettuiamo diverse aggregazioni in paralleo
    airline_stats = df_filtered.groupBy("Reporting_Airline").agg(
        F.count("*").alias("totale_voli"),
        F.avg("DepDelay").alias("ritardo_medio_partenza"),
        F.avg("ArrDelay").alias("ritardo_medio_arrivo"),
        F.sum(F.when(F.col("DepDel15") == 1, 1).otherwise(0)).alias("voli_ritardo_partenza"),
        F.sum(F.when(F.col("ArrDel15") == 1, 1).otherwise(0)).alias("voli_ritardo_arrivo")
    )

    origin_stats = df_filtered.groupBy("Origin").count().withColumnRenamed("count", "partenze")
    dest_stats = df_filtered.groupBy("Dest").count().withColumnRenamed("count", "arrivi")

    # Possiamo anche calcolare le statistiche giornaliere e mensili in una passata
    time_stats = df_filtered.groupBy("DayOfWeek", "Month").agg(
        F.count("*").alias("voli"),
        F.avg("DepDelay").alias("ritardo_medio_partenza"),
        F.avg("ArrDelay").alias("ritardo_medio_arrivo")
    )

    # Ritorniamo un dizionario con tutti i risultati
    return {
        "airline_stats": airline_stats,
        "origin_stats": origin_stats.orderBy("partenze", ascending=False),
        "dest_stats": dest_stats.orderBy("arrivi", ascending=False),
        "time_stats": time_stats
    }

def calcola_ritardi_medi(df):
    """
    Calcola la percentuale media di ritardi per compagnia aerea in modo ottimizzato.
    """
    # Selezioniamo solo le colonne necessarie per ridurre il carico di lavoro
    df_filtered = df.select("Reporting_Airline", "DepDel15", "ArrDel15", "DepDelay", "ArrDelay")

    # Cache utile se questo DataFrame verrà riutilizzato in altre analisi
    df_filtered.cache()

    return df_filtered.groupBy("Reporting_Airline") \
        .agg(
        F.count("*").alias("totale_voli"),
        F.sum(F.when(F.col("DepDel15") == 1, 1).otherwise(0)).alias("voli_ritardo_partenza"),
        F.sum(F.when(F.col("ArrDel15") == 1, 1).otherwise(0)).alias("voli_ritardo_arrivo"),
        F.round(F.avg(F.col("DepDelay")), 2).alias("ritardo_medio_partenza_minuti"),
        F.round(F.avg(F.col("ArrDelay")), 2).alias("ritardo_medio_arrivo_minuti"),
        F.round((F.sum(F.when(F.col("DepDel15") == 1, 1).otherwise(0)) / F.count("*")) * 100, 2)
        .alias("percentuale_ritardi_partenza"),
        F.round((F.sum(F.when(F.col("ArrDel15") == 1, 1).otherwise(0)) / F.count("*")) * 100, 2)
        .alias("percentuale_ritardi_arrivo")
    ) \
        .orderBy("percentuale_ritardi_arrivo", ascending=False)


def top_aeroporti_congestionati_partenze(df, n=5):
    """
    Identifica gli aeroporti più congestionati in base al numero di partenze, ottimizzato.
    """
    # Selezione anticipata delle sole colonne necessarie
    return df.select("Origin", "OriginCityName", "OriginState") \
        .groupBy("Origin", "OriginCityName", "OriginState") \
        .count() \
        .withColumnRenamed("count", "numero_partenze") \
        .orderBy("numero_partenze", ascending=False) \
        .limit(n)

def top_aeroporti_congestionati_arrivi(df, n=5):
    """
    Identifica gli aeroporti più congestionati in base al numero di arrivi.
    """
    return df.select("Dest", "DestCityName", "DestState") \
        .groupBy("Dest", "DestCityName", "DestState") \
        .count() \
        .withColumnRenamed("count", "numero_arrivi") \
        .orderBy("numero_arrivi", ascending=False) \
        .limit(n)


def top_rotte_frequenti(df, n=5):
    """
    Identifica le rotte più frequenti.
    """
    return df.withColumn("Rotta", F.concat(F.col("Origin"), F.lit(" -> "), F.col("Dest"))) \
        .withColumn("CittaRotta", F.concat(F.col("OriginCityName"), F.lit(" -> "), F.col("DestCityName"))) \
        .groupBy("Rotta", "CittaRotta") \
        .count() \
        .withColumnRenamed("count", "numero_voli") \
        .orderBy("numero_voli", ascending=False) \
        .limit(n)


def ritardi_per_giorno_settimana(df):
    """
    Analizza i ritardi medi per giorno della settimana.
    """
    return df.groupBy("DayOfWeek") \
        .agg(
        F.count("*").alias("numero_voli"),
        F.round(F.avg(F.col("DepDelay")), 2).alias("ritardo_medio_partenza"),
        F.round(F.avg(F.col("ArrDelay")), 2).alias("ritardo_medio_arrivo"),
        F.round((F.sum(F.when(F.col("DepDel15") == 1, 1).otherwise(0)) / F.count("*")) * 100, 2)
        .alias("percentuale_ritardi_partenza"),
        F.round((F.sum(F.when(F.col("ArrDel15") == 1, 1).otherwise(0)) / F.count("*")) * 100, 2)
        .alias("percentuale_ritardi_arrivo")
    ) \
        .orderBy("DayOfWeek")


def ritardi_per_mese(df):
    """
    Analizza i ritardi medi per mese.
    """
    return df.groupBy("Month") \
        .agg(
        F.count("*").alias("numero_voli"),
        F.round(F.avg(F.col("DepDelay")), 2).alias("ritardo_medio_partenza"),
        F.round(F.avg(F.col("ArrDelay")), 2).alias("ritardo_medio_arrivo"),
        F.round((F.sum(F.when(F.col("DepDel15") == 1, 1).otherwise(0)) / F.count("*")) * 100, 2)
        .alias("percentuale_ritardi_partenza"),
        F.round((F.sum(F.when(F.col("ArrDel15") == 1, 1).otherwise(0)) / F.count("*")) * 100, 2)
        .alias("percentuale_ritardi_arrivo")
    ) \
        .orderBy("Month")


def cause_ritardi(df):
    """
    Analizza le diverse cause dei ritardi.
    """
    return df.filter((F.col("CarrierDelay").isNotNull()) |
                     (F.col("WeatherDelay").isNotNull()) |
                     (F.col("NASDelay").isNotNull()) |
                     (F.col("SecurityDelay").isNotNull()) |
                     (F.col("LateAircraftDelay").isNotNull())) \
        .agg(
        F.sum("CarrierDelay").alias("totale_ritardo_compagnia"),
        F.sum("WeatherDelay").alias("totale_ritardo_meteo"),
        F.sum("NASDelay").alias("totale_ritardo_sistema"),
        F.sum("SecurityDelay").alias("totale_ritardo_sicurezza"),
        F.sum("LateAircraftDelay").alias("totale_ritardo_aeromobile")
    )


def percentuale_cause_ritardi(df):
    """
    Calcola la percentuale di ciascuna causa di ritardo sul totale, con ottimizzazioni.
    """
    # Selezione anticipata delle colonne necessarie e filtraggio
    delay_cols = ["CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay"]

    ritardi = df.select(*delay_cols) \
        .filter(F.coalesce(*[F.col(c).isNotNull() for c in delay_cols]))

    ritardi.cache()  # Caching utile dato che faremo più operazioni su questo DF

    # Calcolo una sola volta i totali per ogni tipo di ritardo
    totali = ritardi.agg(*[F.sum(col).alias(f"sum_{col}") for col in delay_cols])
    totali.cache()

    # Recupero il totale assoluto dei minuti di ritardo
    totale_row = totali.select(sum([F.col(f"sum_{col}") for col in delay_cols]).alias("totale")).collect()
    totale_minuti = totale_row[0]["totale"]

    # Se non ci sono ritardi registrati, ritorniamo None
    if totale_minuti is None or totale_minuti == 0:
        return None

    # Creiamo il DataFrame con le percentuali
    result = totali.select(*[
        F.round((F.col(f"sum_{col}") / totale_minuti) * 100, 2).alias(
            f"percentuale_ritardo_{col.lower().replace('delay', '')}")
        for col in delay_cols
    ])

    # Puliamo la cache quando abbiamo finito per liberare memoria
    ritardi.unpersist()
    totali.unpersist()

    return result

def performance_compagnie_aeree(df):
    """
    Analizza le performance delle compagnie aeree in termini di puntualità.
    """
    return df.groupBy("Reporting_Airline") \
        .agg(
        F.count("*").alias("totale_voli"),
        F.sum(F.when(F.col("Cancelled") == 1, 1).otherwise(0)).alias("voli_cancellati"),
        F.sum(F.when(F.col("Diverted") == 1, 1).otherwise(0)).alias("voli_deviati"),
        F.round(F.avg(F.col("DepDelay")), 2).alias("ritardo_medio_partenza"),
        F.round(F.avg(F.col("ArrDelay")), 2).alias("ritardo_medio_arrivo"),
        F.round((F.sum(F.when(F.col("DepDel15") == 1, 1).otherwise(0)) / F.count("*")) * 100, 2)
        .alias("percentuale_ritardi_partenza"),
        F.round((F.sum(F.when(F.col("ArrDel15") == 1, 1).otherwise(0)) / F.count("*")) * 100, 2)
        .alias("percentuale_ritardi_arrivo"),
        F.round((F.sum(F.when(F.col("Cancelled") == 1, 1).otherwise(0)) / F.count("*")) * 100, 2)
        .alias("percentuale_cancellazioni")
    ) \
        .orderBy("percentuale_ritardi_arrivo")


def analisi_distanze_ritardi(df):
    """
    Analizza la relazione tra distanza del volo e ritardi.
    """
    return df.groupBy("DistanceGroup") \
        .agg(
        F.count("*").alias("numero_voli"),
        F.round(F.avg("Distance"), 2).alias("distanza_media"),
        F.round(F.avg(F.col("DepDelay")), 2).alias("ritardo_medio_partenza"),
        F.round(F.avg(F.col("ArrDelay")), 2).alias("ritardo_medio_arrivo"),
        F.round((F.sum(F.when(F.col("DepDel15") == 1, 1).otherwise(0)) / F.count("*")) * 100, 2)
        .alias("percentuale_ritardi_partenza"),
        F.round((F.sum(F.when(F.col("ArrDel15") == 1, 1).otherwise(0)) / F.count("*")) * 100, 2)
        .alias("percentuale_ritardi_arrivo")
    ) \
        .orderBy("DistanceGroup")


def orari_critici_ritardi(df):
    """
    Identifica le fasce orarie con più ritardi.
    """
    return df.groupBy("DepTimeBlk") \
        .agg(
        F.count("*").alias("numero_voli"),
        F.round(F.avg(F.col("DepDelay")), 2).alias("ritardo_medio_partenza"),
        F.round((F.sum(F.when(F.col("DepDel15") == 1, 1).otherwise(0)) / F.count("*")) * 100, 2)
        .alias("percentuale_ritardi_partenza")
    ) \
        .orderBy("percentuale_ritardi_partenza", ascending=False)


def statistiche_cancellazioni(df):
    """
    Analizza le cancellazioni dei voli.
    """
    return df.filter(F.col("Cancelled") == 1) \
        .groupBy("CancellationCode") \
        .count() \
        .withColumn("percentuale",
                    F.round(F.col("count") / F.sum("count").over(Window.partitionBy()) * 100, 2)) \
        .orderBy("count", ascending=False)

