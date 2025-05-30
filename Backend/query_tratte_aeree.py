from pyspark.sql.functions import round as spark_round, lit
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, avg, stddev, when, floor
)
from pyspark.sql import functions as F

def classifica_tratte_ritardo(df):
    return (
        df.filter((col("Cancelled") == 0) & col("DepDelay").isNotNull())
          .groupBy("Origin", "Dest")
          .agg(spark_round(avg("DepDelay"), 2).alias("AvgDepDelay"),
               count("*").alias("NumFlights"))
          .orderBy(col("AvgDepDelay").desc())
    )

def classifica_tratte_cancellazioni(df):
    return (
        df.filter(col("Cancelled") == 1)
          .groupBy("Origin", "Dest")
          .agg(count("*").alias("NumCancelled"))
          .orderBy(col("NumCancelled").desc())
    )


def variabilita_ritardi_tratte(df):
    return (
        df.filter((col("Cancelled") == 0) & col("DepDelay").isNotNull())
          .groupBy("Origin", "Dest")
          .agg(spark_round(stddev("DepDelay"), 2).alias("StdDevDepDelay"))
          .orderBy(col("StdDevDepDelay").desc())
    )


def ritardo_per_fascia_oraria(df):
    return (
        df.filter((col("Cancelled") == 0) & col("DepDelay").isNotNull())
          .withColumn("Hour", floor(col("DepTime")/100))
          .withColumn("TimeBlock", when((col("Hour") >= 6) & (col("Hour") <= 12), "Mattina (06:00 - 12:00)")
                                    .when((col("Hour") >= 12) & (col("Hour") <= 18), "Pomeriggio (12:00 - 18:00)")
                                   .when((col("Hour") >= 18) & (col("Hour") <= 24), "Sera (18:00 - 24:00)")
                                   .otherwise("Notte (00:00 - 06:00)"))
          .groupBy("TimeBlock")
          .agg(spark_round(avg("DepDelay"), 2).alias("AvgDepDelay"))
          .orderBy("TimeBlock")
    )


def mappa_rotte_completa(df, aeroporti_info):
    spark = df.sql_ctx.sparkSession
    airports_df = spark.createDataFrame(aeroporti_info)
    statistiche_tratta = (
        df.groupBy("Origin", "Dest")
        .agg(
            F.count("*").alias("flight_count"),
            F.mean("ArrDelay").alias("avg_delay"),
            F.sum(F.col("Cancelled").cast("int")).alias("cancelled_count"),
            F.collect_list("Reporting_Airline").alias("airlines_list"),
            F.collect_list("Month").alias("months_list")
        )
    )
    origine_info = (
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
    rotte = (
        statistiche_tratta
        .join(origine_info, on="Origin", how="left")
        .join(dest_info, on="Dest", how="left")
        .dropna(subset=["origin_lat", "origin_lon", "dest_lat", "dest_lon"])
    )
    inverti = (
        df.groupBy("Origin", "Dest")
        .agg(F.count("*").alias("count_AB"))
        .select(
            F.col("Dest").alias("Origin"),
            F.col("Origin").alias("Dest"),
            F.col("count_AB").alias("count_BA")
        )
    )
    rotte = rotte.join(inverti, on=["Origin", "Dest"], how="left").fillna({"count_BA": 0})
    rotte = rotte.withColumn("top_airlines", F.expr("slice(array_sort(array_distinct(airlines_list)), 1, 5)"))
    rotte = rotte.withColumn("top_months", F.expr("slice(array_sort(array_distinct(months_list)), 1, 2)"))
    return rotte