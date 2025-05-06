
from pyspark.sql.functions import round as spark_round, lit
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, avg, stddev, when, floor
)
from pyspark.sql import functions as F

def classifica_tratte_ritardo(df: DataFrame) -> DataFrame:
    """
    Restituisce ciascuna tratta (Origin→Dest) con ritardo medio partenza.
    """
    return (
        df.filter((col("Cancelled") == 0) & col("DepDelay").isNotNull())
          .groupBy("Origin", "Dest")
          .agg(spark_round(avg("DepDelay"), 2).alias("AvgDepDelay"),
               count("*").alias("NumFlights"))
          .orderBy(col("AvgDepDelay").desc())
    )


def classifica_tratte_cancellazioni(df: DataFrame) -> DataFrame:
    """
    Restituisce ciascuna tratta con numero di cancellazioni.
    """
    return (
        df.filter(col("Cancelled") == 1)
          .groupBy("Origin", "Dest")
          .agg(count("*").alias("NumCancelled"))
          .orderBy(col("NumCancelled").desc())
    )


def variabilita_ritardi_tratte(df: DataFrame) -> DataFrame:
    """
    Restituisce deviazione standard ritardi partenza per tratta.
    """
    return (
        df.filter((col("Cancelled") == 0) & col("DepDelay").isNotNull())
          .groupBy("Origin", "Dest")
          .agg(spark_round(stddev("DepDelay"), 2).alias("StdDevDepDelay"))
          .orderBy(col("StdDevDepDelay").desc())
    )


def ritardo_per_fascia_oraria(df: DataFrame) -> DataFrame:
    """
    Confronto ritardo medio partenza per fascia oraria (Mattina vs Sera).
    Mattina: DepTime tra 0500 e 1159; Sera: 1700 e 2359.
    """
    return (
        df.filter((col("Cancelled") == 0) & col("DepDelay").isNotNull())
          .withColumn("Hour", floor(col("DepTime")/100))
          .withColumn("TimeBlock", when((col("Hour") >= 5) & (col("Hour") <= 11), "Mattina (05:00 - 11:00)")
                                   .when((col("Hour") >= 17) & (col("Hour") <= 23), "Sera (17:00 - 23:00)")
                                   .otherwise("Notte (23:00 - 05:00)"))
          .groupBy("TimeBlock")
          .agg(spark_round(avg("DepDelay"), 2).alias("AvgDepDelay"))
          .orderBy("TimeBlock")
    )





def mappa_rotte_completa(flights_df, airports_pd):
    spark = flights_df.sql_ctx.sparkSession
    airports_df = spark.createDataFrame(airports_pd)

    # Statistiche di base per tratta
    route_stats = (
        flights_df.groupBy("Origin", "Dest")
        .agg(
            F.count("*").alias("flight_count"),
            F.mean("ArrDelay").alias("avg_delay"),
            F.sum(F.col("Cancelled").cast("int")).alias("cancelled_count"),
            F.collect_list("Reporting_Airline").alias("airlines_list"),
            F.collect_list("Month").alias("months_list")
        )
    )

    # Informazioni su aeroporti (lat/lon + nome città)
    origin_info = (
        airports_df
        .select(
            F.col("iata").alias("Origin"),
            F.col("city").alias("origin_city"),
            F.col("latitude").alias("origin_lat"),
            F.col("longitude").alias("origin_lon")
        )
    )
    dest_info = (
        airports_df
        .select(
            F.col("iata").alias("Dest"),
            F.col("city").alias("dest_city"),
            F.col("latitude").alias("dest_lat"),
            F.col("longitude").alias("dest_lon")
        )
    )

    # Join statistiche + info aeroporti
    routes = (
        route_stats
        .join(origin_info, on="Origin", how="left")
        .join(dest_info, on="Dest", how="left")
        .dropna(subset=["origin_lat", "origin_lon", "dest_lat", "dest_lon"])
    )

    # Calcola voli B->A (inverted)
    inverted = (
        flights_df.groupBy("Origin", "Dest")
        .agg(F.count("*").alias("count_AB"))
        .select(
            F.col("Dest").alias("Origin"),
            F.col("Origin").alias("Dest"),
            F.col("count_AB").alias("count_BA")
        )
    )
    routes = routes.join(inverted, on=["Origin", "Dest"], how="left").fillna({"count_BA": 0})

    # Top 5 compagnie e top 2 mesi
    routes = routes.withColumn("top_airlines", F.expr("slice(array_sort(array_distinct(airlines_list)), 1, 5)"))
    routes = routes.withColumn("top_months", F.expr("slice(array_sort(array_distinct(months_list)), 1, 2)"))

    return routes