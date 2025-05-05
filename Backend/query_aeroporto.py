from pyspark.sql import functions as F, Window
import pydeck as pdk
import pandas as pd
from pyspark.sql.functions import col, avg

def lista_aeroporti_unici(df):
    origin_airports = df.select("Origin").distinct().withColumnRenamed("Origin", "IATA")
    dest_airports = df.select("Dest").distinct().withColumnRenamed("Dest", "IATA")
    all_airports = origin_airports.union(dest_airports).distinct()
    return [row["IATA"] for row in all_airports.collect()]


def num_voli_partiti(df, aeroporto):
    return df.filter(F.col("Origin") == aeroporto) \
             .agg(F.count("*").alias("NumVoliPartiti"))

def num_voli_arrivati(df, aeroporto):
    return df.filter(F.col("Dest") == aeroporto) \
             .agg(F.count("*").alias("NumVoliArrivati"))

def num_voli_cancellati(df, aeroporto):
    return df.filter((F.col("Origin") == aeroporto) & (F.col("Cancelled") == 1)) \
             .agg(F.count("*").alias("NumVoliCancellati"))

def nome_citta_aeroporto(df, aeroporto):
    citta_sorgente = df.filter(F.col("Origin") == aeroporto) \
                       .select("OriginCityName") \
                       .distinct() \
                       .collect()
    if not citta_sorgente:
        citta_destinazione = df.filter(F.col("Dest") == aeroporto) \
                               .select("DestCityName") \
                               .distinct() \
                               .collect()
        return citta_destinazione
    return citta_sorgente




def mappa_tratte_aeroporto(flights_df, airports_pd, codice_aeroporto, tipo="partenza"):
    """
    Restituisce un DataFrame con le tratte in cui l'aeroporto specificato è di partenza o di arrivo.

    :param flights_df: DataFrame Spark con i dati dei voli
    :param airports_pd: DataFrame Pandas con info aeroporti (iata, lat, lon)
    :param codice_aeroporto: Codice IATA dell'aeroporto
    :param tipo: 'partenza' o 'arrivo'
    :return: DataFrame Spark con info delle tratte e coordinate
    """
    from pyspark.sql import functions as F

    spark = flights_df.sql_ctx.sparkSession
    airports_df = spark.createDataFrame(airports_pd)

    if tipo == "partenza":
        filtered_routes = flights_df.filter(F.col("Origin") == codice_aeroporto) \
            .groupBy("Origin", "Dest") \
            .agg(
                F.count("*").alias("flight_count"),
                F.collect_set("Reporting_Airline").alias("airlines")
            )
    else:  # tipo == "arrivo"
        filtered_routes = flights_df.filter(F.col("Dest") == codice_aeroporto) \
            .groupBy("Origin", "Dest") \
            .agg(
                F.count("*").alias("flight_count"),
                F.collect_set("Reporting_Airline").alias("airlines")
            )

    origin_info = airports_df.select(
        F.col("iata").alias("Origin"),
        F.col("latitude").alias("origin_lat"),
        F.col("longitude").alias("origin_lon")
    )

    dest_info = airports_df.select(
        F.col("iata").alias("Dest"),
        F.col("latitude").alias("dest_lat"),
        F.col("longitude").alias("dest_lon")
    )

    enriched_routes = filtered_routes \
        .join(origin_info, on="Origin", how="left") \
        .join(dest_info, on="Dest", how="left") \
        .dropna(subset=["origin_lat", "origin_lon", "dest_lat", "dest_lon"])

    return enriched_routes


def cause_ritardi_per_aeroporto(df, aeroporto):
    return df.filter(F.col("Origin") == aeroporto) \
             .agg(
                 F.sum("CarrierDelay").alias("Carrier"),
                 F.sum("WeatherDelay").alias("Weather"),
                 F.sum("NASDelay").alias("NAS"),
                 F.sum("SecurityDelay").alias("Security"),
                 F.sum("LateAircraftDelay").alias("LateAircraft")
             )

def distribuzione_ritardi_mensile(df, aeroporto):
    return df.filter((F.col("Origin") == aeroporto) & (F.col("DepDelay") > 0)) \
             .withColumn("Month", F.month("FlightDate")) \
             .groupBy("Month") \
             .agg(F.avg("DepDelay").alias("AvgDepDelay")) \
             .orderBy("Month")

def trend_cancellazioni_stagionale(df, aeroporto):
    return df.filter((F.col("Origin") == aeroporto) & (F.col("Cancelled") == 1)) \
             .withColumn("Month", F.month("FlightDate")) \
             .groupBy("Month") \
             .agg(F.count("*").alias("Cancellazioni")) \
             .orderBy("Month")


def metriche_compagnie(df, aeroporto):
    """
    Calcola per ogni compagnia (IATA_CODE_Reporting_Airline) che parte da `aeroporto`:
    - NumVoli (count)
    - RitardoMedio (avg DepDelayMinutes)
    - PercCancellazioni (percentuale di Cancelled)
    """
    df_filtrato = df.filter(F.col("Origin") == aeroporto)
    return df_filtrato.groupBy("IATA_CODE_Reporting_Airline").agg(
        F.count("*").alias("NumVoli"),
        F.avg("DepDelayMinutes").alias("RitardoMedio"),
        (F.sum(F.col("Cancelled")) / F.count("*") * 100).alias("PercCancellazioni")
    )


def valutazione_compagnie_aeroporto(df, aeroporto):
    df_filtrato = df.filter(df["Origin"] == aeroporto)

    stats = df_filtrato.groupBy("Carrier").agg(
        F.count("*").alias("NumVoli"),
        F.avg("DepDelay").alias("RitardoMedio"),
        F.avg(F.col("Cancelled").cast("double")).alias("PercCancellazioni")
    )

    # Normalizza ogni metrica su scala 0-1
    stats = stats.withColumn("Norm_Ritardo", (F.max("RitardoMedio").over(Window.partitionBy()) - F.col("RitardoMedio")) / (F.max("RitardoMedio").over(Window.partitionBy()) - F.min("RitardoMedio").over(Window.partitionBy())))
    stats = stats.withColumn("Norm_Cancellazioni", (F.max("PercCancellazioni").over(Window.partitionBy()) - F.col("PercCancellazioni")) / (F.max("PercCancellazioni").over(Window.partitionBy()) - F.min("PercCancellazioni").over(Window.partitionBy())))
    stats = stats.withColumn("Norm_Voli", (F.col("NumVoli") - F.min("NumVoli").over(Window.partitionBy())) / (F.max("NumVoli").over(Window.partitionBy()) - F.min("NumVoli").over(Window.partitionBy())))

    # Calcola punteggio: media pesata
    stats = stats.withColumn("Punteggio", F.col("Norm_Ritardo") * 0.4 + F.col("Norm_Cancellazioni") * 0.3 + F.col("Norm_Voli") * 0.3)

    return stats.select("Carrier", "NumVoli", "RitardoMedio", "PercCancellazioni", "Punteggio")

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


def elenco_tutti_aeroporti(df=None):
    aeroporti = df.select("Origin").union(df.select("Dest")).distinct().orderBy("Origin")
    return aeroporti
