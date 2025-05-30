from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg, count, dayofweek, date_format, month, round as spark_round, sum as spark_sum
import pandas as pd

def perc_voli_in_orario(df):
    totale = df.count()
    in_orario = df.filter((col("Cancelled") == 0) & (col("DepDelay") <= 0)).count()
    return round(in_orario / totale * 100, 2) if totale != 0 else 0.0


def perc_voli_in_ritardo(df):
    totale = df.count()
    in_ritardo = df.filter((col("Cancelled") == 0) & (col("DepDelay") > 0)).count()
    return round(in_ritardo / totale * 100, 2) if totale != 0 else 0.0


def perc_voli_cancellati(df):
    totale = df.count()
    cancellati = df.filter(col("Cancelled") == 1).count()
    return round(cancellati / totale * 100, 2) if totale != 0 else 0.0


def ritardo_medio_partenza(df):
    row = df.filter(col("Cancelled") == 0).agg(avg("DepDelay")).first()
    return round(row[0], 2) if row and row[0] is not None else 0.0


def ritardo_medio_arrivo(df):
    row = df.filter(col("Cancelled") == 0).agg(avg("ArrDelay")).first()
    return round(row[0], 2) if row and row[0] is not None else 0.0

def serie_settimanale_ritardi_cumulativi(df):
    return (
        df.filter((col("Cancelled") == 0) & (col("DepDelay") > 0))
          .withColumn("DayOfWeekNum", dayofweek("FlightDate"))
          .withColumn("DayOfWeek", date_format("FlightDate", "EEEE"))
          .groupBy("DayOfWeekNum", "DayOfWeek")
          .agg(spark_round(spark_sum("DepDelay"), 2).alias("TotalDepDelay"))
          .orderBy("DayOfWeekNum")
    )

def serie_mensile_ritardi_cumulativi(df):
    return (
        df.filter((col("Cancelled") == 0) & (col("DepDelay") > 0))
          .withColumn("Month", month("FlightDate"))
          .groupBy("Month")
          .agg(spark_round(spark_sum("DepDelay"), 2).alias("TotalDepDelay"))
          .orderBy("Month")
    )


def serie_settimanale_cancellazioni(df):
    return (
        df.filter(col("Cancelled") == 1)
          .withColumn("DayOfWeekNum", dayofweek("FlightDate"))
          .withColumn("DayOfWeek", date_format("FlightDate", "EEEE"))
          .groupBy("DayOfWeekNum", "DayOfWeek")
          .agg(count("*").alias("Cancellazioni"))
          .orderBy("DayOfWeekNum")
    )


def serie_mensile_cancellazioni(df):
    return (
        df.filter(col("Cancelled") == 1)
          .withColumn("Month", month("FlightDate"))
          .groupBy("Month")
          .agg(count("*").alias("Cancellazioni"))
          .orderBy("Month")
    )


def ritardo_medio_per_mese(df):
    return (
        df.filter((col("Cancelled") == 0) & col("DepDelay").isNotNull())
          .withColumn("Month", month("FlightDate"))
          .groupBy("Month")
          .agg(spark_round(avg("DepDelay"), 2).alias("AvgDepDelay"))
          .orderBy("Month")
    )


def ritardo_medio_per_giorno_settimana(df):
    return (
        df.filter((col("Cancelled") == 0) & col("DepDelay").isNotNull())
          .withColumn("DayOfWeekNum", dayofweek("FlightDate"))
          .withColumn("DayOfWeek", date_format("FlightDate", "EEEE"))
          .groupBy("DayOfWeekNum", "DayOfWeek")
          .agg(spark_round(avg("DepDelay"), 2).alias("AvgDepDelay"))
          .orderBy("DayOfWeekNum")
    )

def classifica_compagnie(df):
    voli_totali = count("*")
    df_base = df.groupBy("IATA_CODE_Reporting_Airline").agg(
        spark_round(avg(col("DepDelay")), 2).alias("AvgDepDelay"),
        spark_round(avg(col("ArrDelay")), 2).alias("AvgArrDelay"),
        spark_round(spark_sum((col("DepDelay") > 0).cast("int")) / voli_totali * 100, 2).alias("PercDelayed"),
        spark_round(spark_sum(col("Cancelled")) / voli_totali * 100, 2).alias("PercCancelled")
    )
    return df_base.orderBy(col("AvgDepDelay").desc())

def classifica_aeroporti_ritardi(df):
    return (
        df.filter(col("Cancelled") == 0)
          .groupBy("Origin")
          .agg(
              spark_round(avg(col("DepDelay")), 2).alias("AvgDepDelay"),
              count("*").alias("NumFlights")
          )
          .orderBy(col("AvgDepDelay").desc())
    )


def classifica_aeroporti_cancellazioni(df):
    return (
        df.filter(col("Cancelled") == 1)
          .groupBy("Origin")
          .agg(count("*").alias("NumCancelled"))
          .orderBy(col("NumCancelled").desc())
    )

def percentuale_cause(df):
    somma = df.filter(col("Cancelled") == 0).agg(
        spark_sum("CarrierDelay").alias("Carrier"),
        spark_sum("WeatherDelay").alias("Weather"),
        spark_sum("NASDelay").alias("NAS"),
        spark_sum("SecurityDelay").alias("Security"),
        spark_sum("LateAircraftDelay").alias("LateAircraft")
    ).first().asDict()
    totale = sum(somma.values()) or 1
    df_pd = pd.DataFrame([
        {"Cause": k, "Percent": round(v / totale * 100, 2)}
        for k, v in somma.items()
    ])
    return df_pd


def trend_cause_mensile(df):
    df = (
        df.filter(col("Cancelled") == 0)
          .withColumn("Month", month("FlightDate"))
          .groupBy("Month").agg(
              spark_round(spark_sum("CarrierDelay"), 2).alias("Carrier"),
              spark_round(spark_sum("WeatherDelay"), 2).alias("Weather"),
              spark_round(spark_sum("NASDelay"), 2).alias("NAS"),
              spark_round(spark_sum("SecurityDelay"), 2).alias("Security"),
              spark_round(spark_sum("LateAircraftDelay"), 2).alias("LateAircraft")
          )
          .orderBy("Month")
    )
    return df.toPandas()

