from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg, count, dayofweek, date_format, month, round as spark_round, sum as spark_sum

def perc_voli_in_orario(df):
    """
    Restituisce la percentuale di voli in orario (DepDelay <= 0, non cancellati).
    """
    total = df.count()
    on_time = df.filter((col("Cancelled") == 0) & (col("DepDelay") <= 0)).count()
    return round(on_time / total * 100, 2) if total != 0 else 0.0


def perc_voli_in_ritardo(df):
    """
    Restituisce la percentuale di voli in ritardo (DepDelay > 0, non cancellati).
    """
    total = df.count()
    delayed = df.filter((col("Cancelled") == 0) & (col("DepDelay") > 0)).count()
    return round(delayed / total * 100, 2) if total != 0 else 0.0


def perc_voli_cancellati(df):
    """
    Restituisce la percentuale di voli cancellati.
    """
    total = df.count()
    cancelled = df.filter(col("Cancelled") == 1).count()
    return round(cancelled / total * 100, 2) if total != 0 else 0.0


def ritardo_medio_partenza(df):
    """
    Restituisce il ritardo medio di partenza (DepDelay) sui voli non cancellati.
    """
    row = df.filter(col("Cancelled") == 0).agg(avg("DepDelay")).first()
    return round(row[0], 2) if row and row[0] is not None else 0.0


def ritardo_medio_arrivo(df):
    """
    Restituisce il ritardo medio di arrivo (ArrDelay) sui voli non cancellati.
    """
    row = df.filter(col("Cancelled") == 0).agg(avg("ArrDelay")).first()
    return round(row[0], 2) if row and row[0] is not None else 0.0



def serie_settimanale_ritardi_cumulativi(df: DataFrame) -> DataFrame:
    """
    Ritardi cumulativi per giorno della settimana (tutti i lunedì, tutti i martedì, ecc.).
    Restituisce colonne DayOfWeekNum, DayOfWeek, TotalDepDelay.
    """
    return (
        df.filter((col("Cancelled") == 0) & (col("DepDelay") > 0))
          .withColumn("DayOfWeekNum", dayofweek("FlightDate"))
          .withColumn("DayOfWeek", date_format("FlightDate", "EEEE"))
          .groupBy("DayOfWeekNum", "DayOfWeek")
          .agg(spark_round(spark_sum("DepDelay"), 2).alias("TotalDepDelay"))
          .orderBy("DayOfWeekNum")
    )


def serie_mensile_ritardi_cumulativi(df: DataFrame) -> DataFrame:
    """
    Ritardi cumulativi per mese (1-12).
    Restituisce colonne Month, TotalDepDelay.
    """
    return (
        df.filter((col("Cancelled") == 0) & (col("DepDelay") > 0))
          .withColumn("Month", month("FlightDate"))
          .groupBy("Month")
          .agg(spark_round(spark_sum("DepDelay"), 2).alias("TotalDepDelay"))
          .orderBy("Month")
    )


def serie_settimanale_cancellazioni(df: DataFrame) -> DataFrame:
    """
    Numero di cancellazioni per giorno della settimana (tutti i lunedì, tutti i martedì, ecc.).
    Restituisce colonne DayOfWeekNum, DayOfWeek, Cancellazioni.
    """
    return (
        df.filter(col("Cancelled") == 1)
          .withColumn("DayOfWeekNum", dayofweek("FlightDate"))
          .withColumn("DayOfWeek", date_format("FlightDate", "EEEE"))
          .groupBy("DayOfWeekNum", "DayOfWeek")
          .agg(count("*").alias("Cancellazioni"))
          .orderBy("DayOfWeekNum")
    )


def serie_mensile_cancellazioni(df: DataFrame) -> DataFrame:
    """
    Numero di cancellazioni per mese (1-12).
    Restituisce colonne Month, Cancellazioni.
    """
    return (
        df.filter(col("Cancelled") == 1)
          .withColumn("Month", month("FlightDate"))
          .groupBy("Month")
          .agg(count("*").alias("Cancellazioni"))
          .orderBy("Month")
    )


def ritardo_medio_per_mese(df: DataFrame) -> DataFrame:
    """
    Ritardo medio di partenza per mese (1-12).
    Restituisce colonne Month, AvgDepDelay.
    """
    return (
        df.filter((col("Cancelled") == 0) & col("DepDelay").isNotNull())
          .withColumn("Month", month("FlightDate"))
          .groupBy("Month")
          .agg(spark_round(avg("DepDelay"), 2).alias("AvgDepDelay"))
          .orderBy("Month")
    )


def ritardo_medio_per_giorno_settimana(df: DataFrame) -> DataFrame:
    """
    Ritardo medio di partenza per giorno della settimana (tutti i lunedì, ecc.).
    Restituisce colonne DayOfWeekNum, DayOfWeek, AvgDepDelay.
    """
    return (
        df.filter((col("Cancelled") == 0) & col("DepDelay").isNotNull())
          .withColumn("DayOfWeekNum", dayofweek("FlightDate"))
          .withColumn("DayOfWeek", date_format("FlightDate", "EEEE"))
          .groupBy("DayOfWeekNum", "DayOfWeek")
          .agg(spark_round(avg("DepDelay"), 2).alias("AvgDepDelay"))
          .orderBy("DayOfWeekNum")
    )



def classifica_compagnie(df: DataFrame) -> DataFrame:
    """
    Restituisce per compagnia:
      - Ritardo medio partenza
      - Ritardo medio arrivo
      - Percentuale voli in ritardo
      - Percentuale voli cancellati
    """
    total_flights = count("*")
    df_base = df.groupBy("IATA_CODE_Reporting_Airline").agg(
        spark_round(avg(col("DepDelay")), 2).alias("AvgDepDelay"),
        spark_round(avg(col("ArrDelay")), 2).alias("AvgArrDelay"),
        spark_round(spark_sum((col("DepDelay") > 0).cast("int")) / total_flights * 100, 2).alias("PercDelayed"),
        spark_round(spark_sum(col("Cancelled")) / total_flights * 100, 2).alias("PercCancelled")
    )
    return df_base.orderBy(col("AvgDepDelay").desc())


def classifica_aeroporti_ritardi(df: DataFrame) -> DataFrame:
    """
    Restituisce aeroporti origin con ritardo medio di partenza e numero voli.
    """
    return (
        df.filter(col("Cancelled") == 0)
          .groupBy("Origin")
          .agg(
              spark_round(avg(col("DepDelay")), 2).alias("AvgDepDelay"),
              count("*").alias("NumFlights")
          )
          .orderBy(col("AvgDepDelay").desc())
    )


def classifica_aeroporti_cancellazioni(df: DataFrame) -> DataFrame:
    """
    Restituisce aeroporti origin ordinati per numero di voli cancellati.
    """
    return (
        df.filter(col("Cancelled") == 1)
          .groupBy("Origin")
          .agg(count("*").alias("NumCancelled"))
          .orderBy(col("NumCancelled").desc())
    )

# --- Cause dei Ritardi ---

def percentuale_cause(df: DataFrame) -> DataFrame:
    """
    Percentuale di ritardo per causa sul totale dei ritardi.
    Cause: CarrierDelay, WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay
    """
    # calcolo totale minuti di ritardo
    sums = df.filter(col("Cancelled") == 0).agg(
        spark_sum("CarrierDelay").alias("Carrier"),
        spark_sum("WeatherDelay").alias("Weather"),
        spark_sum("NASDelay").alias("NAS"),
        spark_sum("SecurityDelay").alias("Security"),
        spark_sum("LateAircraftDelay").alias("LateAircraft")
    ).first().asDict()
    total = sum(sums.values()) or 1
    # costruisco DataFrame pandas
    import pandas as pd
    pd = __import__('pandas')
    df_pd = pd.DataFrame([
        {"Cause": k, "Percent": round(v / total * 100, 2)}
        for k, v in sums.items()
    ])
    return df_pd


def trend_cause_mensile(df: DataFrame) -> DataFrame:
    """
    Andamento mensile dei minuti di ritardo per ciascuna causa.
    Restituisce DataFrame pandas con colonna Month e colonne per ogni causa.
    """
    # aggrego per mese le somme di ciascuna causa
    spark_df = (
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
    return spark_df.toPandas()

