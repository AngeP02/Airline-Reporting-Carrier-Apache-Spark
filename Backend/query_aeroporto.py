from pyspark.sql import functions as F

def lista_aeroporti_unici(df):
    aeroporti_origine = df.select("Origin").distinct().withColumnRenamed("Origin", "IATA")
    aeroporti_destinazione = df.select("Dest").distinct().withColumnRenamed("Dest", "IATA")
    tutti = aeroporti_origine.union(aeroporti_destinazione).distinct()
    return [row["IATA"] for row in tutti.collect()]


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

def mappa_tratte_aeroporto(df, pd_aeroporti, codice_aeroporto, tipo="partenza"):
    spark = df.sql_ctx.sparkSession
    df_aeroporti = spark.createDataFrame(pd_aeroporti)
    if tipo == "partenza":
        rotte_filtrate = df.filter(F.col("Origin") == codice_aeroporto) \
            .groupBy("Origin", "Dest") \
            .agg(
                F.count("*").alias("flight_count"),
                F.collect_set("Reporting_Airline").alias("airlines")
            )
    else:
        rotte_filtrate = df.filter(F.col("Dest") == codice_aeroporto) \
            .groupBy("Origin", "Dest") \
            .agg(
                F.count("*").alias("flight_count"),
                F.collect_set("Reporting_Airline").alias("airlines")
            )
    origine_info = df_aeroporti.select(
        F.col("iata").alias("Origin"),
        F.col("latitude").alias("origin_lat"),
        F.col("longitude").alias("origin_lon")
    )
    dest_info = df_aeroporti.select(
        F.col("iata").alias("Dest"),
        F.col("latitude").alias("dest_lat"),
        F.col("longitude").alias("dest_lon")
    )
    rotte = rotte_filtrate \
        .join(origine_info, on="Origin", how="left") \
        .join(dest_info, on="Dest", how="left") \
        .dropna(subset=["origin_lat", "origin_lon", "dest_lat", "dest_lon"])
    return rotte


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
    df_filtrato = df.filter(F.col("Origin") == aeroporto)
    return df_filtrato.groupBy("IATA_CODE_Reporting_Airline").agg(
        F.count("*").alias("NumVoli"),
        F.avg("DepDelayMinutes").alias("RitardoMedio"),
        (F.sum(F.col("Cancelled")) / F.count("*") * 100).alias("PercCancellazioni")
    )
