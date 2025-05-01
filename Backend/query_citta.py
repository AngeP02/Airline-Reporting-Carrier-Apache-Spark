### Backend: Backend/query_citta.py
import pyspark.sql.functions as F
from pyspark.sql import Window

def lista_citta_uniche(df):
    citta_orig = df.select("OriginCityName").distinct().withColumnRenamed("OriginCityName", "Citta")
    citta_dest = df.select("DestCityName").distinct().withColumnRenamed("DestCityName", "Citta")
    all_airports = citta_orig.union(citta_dest).distinct()
    return [row["Citta"] for row in all_airports.collect()]


# --- Statistiche generali per città ---
def num_voli_partiti_citta(df, city_name):
    return df.filter(F.col("OriginCityName") == city_name) \
             .agg(F.count("*").alias("NumVoliPartiti"))


def num_voli_arrivati_citta(df, city_name):
    return df.filter(F.col("DestCityName") == city_name) \
             .agg(F.count("*").alias("NumVoliArrivati"))


def num_voli_cancellati_citta(df, city_name):
    return df.filter((F.col("OriginCityName") == city_name) & (F.col("Cancelled") == 1)) \
             .agg(F.count("*").alias("NumVoliCancellati"))

from pyspark.sql.functions import countDistinct

def num_aeroporti_citta(df, citta):
    return df.filter(F.col("OriginCityName") == citta) \
             .agg(countDistinct("Origin").alias("NumAeroportiCitta"))


def mappa_tratte_citta(flights_df, airports_pd, nome_citta, tipo="partenza"):
    """
    Restituisce un DataFrame Spark con le tratte in cui la città specificata è origine o destinazione.

    :param flights_df: DataFrame Spark con i dati dei voli
    :param airports_pd: DataFrame Pandas con info aeroporti (iata, lat, lon)
    :param nome_citta: Nome della città (es. "New York, NY")
    :param tipo: 'partenza' o 'arrivo'
    :return: DataFrame Spark con info delle tratte e coordinate
    """
    from pyspark.sql import functions as F

    spark = flights_df.sql_ctx.sparkSession
    airports_df = spark.createDataFrame(airports_pd)

    if tipo == "partenza":
        filtered_routes = flights_df.filter(F.col("OriginCityName") == nome_citta) \
            .groupBy("Origin", "Dest") \
            .agg(
                F.count("*").alias("flight_count"),
                F.collect_set("Reporting_Airline").alias("airlines")
            )
    else:  # tipo == "arrivo"
        filtered_routes = flights_df.filter(F.col("DestCityName") == nome_citta) \
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


# --- Aeroporti in città ---
def aeroporti_in_citta(airports_pd, city_name):
    # airports_pd: pandas DataFrame con colonne ['iata','city',...]
    subset = airports_pd[airports_pd['city'] == city_name]
    return subset['iata'].unique().tolist()

# --- Distribuzioni partenze/arrivi tra aeroporti della città ---
def distribuzione_partenze_per_aeroporto_in_citta(df, city_name):
    return df.filter(F.col("OriginCityName") == city_name) \
             .groupBy("Origin") \
             .agg(F.count("*").alias("NumVoli")) \
             .orderBy(F.desc("NumVoli"))


def distribuzione_arrivi_per_aeroporto_in_citta(df, city_name):
    return df.filter(F.col("DestCityName") == city_name) \
             .groupBy("Dest") \
             .agg(F.count("*").alias("NumVoli")) \
             .orderBy(F.desc("NumVoli"))

# --- Compagnie e metriche aggregate per città ---
def metriche_compagnie_citta(df, city_name):
    df_filtrato = df.filter(F.col("OriginCityName") == city_name)
    return df_filtrato.groupBy("IATA_CODE_Reporting_Airline").agg(
        F.count("*").alias("NumVoli"),
        F.avg("DepDelayMinutes").alias("RitardoMedio"),
        (F.sum(F.col("Cancelled")) / F.count("*") * 100).alias("PercCancellazioni")
    )


def compagnie_attive_in_citta(df, city_name):
    rows = df.filter(
        (F.col("OriginCityName") == city_name) | (F.col("DestCityName") == city_name)
    ).select("Reporting_Airline").distinct().collect()
    return [r["Reporting_Airline"] for r in rows]

# --- Analisi mensile per città ---
def voli_partiti_mensili_citta(df, city_name):
    return df.filter(F.col("OriginCityName") == city_name) \
             .withColumn("Month", F.month("FlightDate")) \
             .groupBy("Month") \
             .agg(F.count("*").alias("VoliPartiti")) \
             .orderBy("Month")


def voli_arrivati_mensili_citta(df, city_name):
    return df.filter(F.col("DestCityName") == city_name) \
             .withColumn("Month", F.month("FlightDate")) \
             .groupBy("Month") \
             .agg(F.count("*").alias("VoliArrivati")) \
             .orderBy("Month")


def voli_totali_mensili_citta(df, city_name):
    cond = (F.col("OriginCityName") == city_name) | (F.col("DestCityName") == city_name)
    return df.filter(cond) \
             .withColumn("Month", F.month("FlightDate")) \
             .groupBy("Month") \
             .agg(F.count("*").alias("VoliTotali")) \
             .orderBy("Month")


def cancellazioni_mensili_citta(df, city_name):
    return df.filter((F.col("OriginCityName") == city_name) & (F.col("Cancelled") == 1)) \
             .withColumn("Month", F.month("FlightDate")) \
             .groupBy("Month") \
             .agg(F.count("*").alias("VoliCancellati")) \
             .orderBy("Month")