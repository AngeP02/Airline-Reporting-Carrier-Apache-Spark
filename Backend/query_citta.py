import pyspark.sql.functions as F
from pyspark.sql.functions import countDistinct

def lista_citta_uniche(df):
    citta_orig = df.select("OriginCityName").distinct().withColumnRenamed("OriginCityName", "Citta")
    citta_dest = df.select("DestCityName").distinct().withColumnRenamed("DestCityName", "Citta")
    all_airports = citta_orig.union(citta_dest).distinct()
    return [row["Citta"] for row in all_airports.collect()]


def num_voli_partiti_citta(df, city_name):
    return df.filter(F.col("OriginCityName") == city_name) \
             .agg(F.count("*").alias("NumVoliPartiti"))


def num_voli_arrivati_citta(df, city_name):
    return df.filter(F.col("DestCityName") == city_name) \
             .agg(F.count("*").alias("NumVoliArrivati"))


def num_voli_cancellati_citta(df, city_name):
    return df.filter((F.col("OriginCityName") == city_name) & (F.col("Cancelled") == 1)) \
             .agg(F.count("*").alias("NumVoliCancellati"))


def num_aeroporti_citta(df, citta):
    return df.filter(F.col("OriginCityName") == citta) \
             .agg(countDistinct("Origin").alias("NumAeroportiCitta"))

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