import pyspark.sql.functions as F
from pyspark.sql.functions import avg, col, count, sum as spark_sum

def calcola_percentuale_ritardi_15(df):
    totale = df.filter(col("DepDelay").isNotNull()).count()
    rit15  = df.filter(col("DepDelay") > 15).count()
    return (rit15 / totale) * 100 if totale > 0 else 0.0

def calcola_numero_aeroporti(df):
    return df.select("Origin") \
             .union(df.select("Dest")) \
             .distinct() \
             .count()

def calcola_numero_citta(df, aeroporti_info):
    aeroporti_unici = [row["Origin"]
    for row in df.select("Origin")
                .union(df.select("Dest"))
                .distinct()
                .collect()]
    df_info = aeroporti_info[aeroporti_info["iata"].isin(aeroporti_unici)]
    return df_info["city"].nunique()

def calcola_top5_compagnie_puntuali(df, n=5):
    df_stats = df.groupBy("Reporting_Airline") \
                 .agg(avg("DepDelayMinutes").alias("Ritardo_Medio_Partenza")) \
                 .orderBy("Ritardo_Medio_Partenza")
    return df_stats.limit(n).toPandas() \
                   .rename(columns={"Reporting_Airline": "Compagnia",
                                    "Ritardo_Medio_Partenza": "Ritardo Medio (min)"})

def calcola_top5_aeroporti_piu_ritardi(df, n=5):
    df_ritardi = df.filter(col("DepDelayMinutes") > 0)
    df_stats  = df_ritardi.groupBy("Origin") \
                         .agg(count("*").alias("Numero_Ritardi")) \
                         .orderBy(col("Numero_Ritardi").desc())
    return df_stats.limit(n).toPandas() \
                   .rename(columns={"Origin": "Aeroporto",
                                    "Numero_Ritardi": "Voli in Ritardo"})

def calcola_top5_tratte_trafficate(df, n=5):
    df_stats = df.groupBy("Origin", "Dest") \
                 .agg(count("*").alias("Numero_Voli")) \
                 .orderBy(col("Numero_Voli").desc())
    return df_stats.limit(n).toPandas() \
                   .rename(columns={"Origin": "Origine",
                                    "Dest": "Destinazione",
                                    "Numero_Voli": "Voli"})

def calcola_top5_compagnie_piu_voli(df, n=5):
    df_stats = df.groupBy("Reporting_Airline") \
                 .agg(count("*").alias("Totale_Voli")) \
                 .orderBy(col("Totale_Voli").desc())
    return df_stats.limit(n).toPandas() \
                   .rename(columns={"Reporting_Airline": "Compagnia",
                                    "Totale_Voli": "Voli"})

def calcola_top5_compagnie_meno_cancellazioni(df, n=5):
    stats = df.groupBy("Reporting_Airline") \
              .agg(
                  count("*").alias("Totale"),
                  spark_sum("Cancelled").alias("Voli_Cancellati")
              ) \
              .withColumn("Pct_Cancellati",
                          col("Voli_Cancellati")/col("Totale")*100) \
              .orderBy(col("Pct_Cancellati").asc())
    return stats.limit(n).toPandas() \
                .rename(columns={
                    "Reporting_Airline": "Compagnia",
                    "Pct_Cancellati": "% Cancellazioni"
                })


def calcola_mappa_aeroporti(df, aeroporti_info):
    orig = df.select(col("Origin").alias("iata"),
                     col("DepDelayMinutes").alias("DepDelay"))
    dest = df.select(col("Dest").alias("iata"),
                     col("DepDelayMinutes").alias("DepDelay"))
    tutti_voli = orig.union(dest)
    stats = tutti_voli.groupBy("iata") \
        .agg(
            count("*").alias("flight_count"),
            avg("DepDelay").alias("avg_delay")
        )
    pdf = stats.toPandas()
    merged = pdf.merge(
        aeroporti_info[["iata",  "name", "latitude", "longitude"]],
        on="iata",
        how="left"
    ).dropna(subset=["latitude", "longitude"]).rename(
        columns={"name": "airport_name"}
    )
    max_count = merged["flight_count"].max()
    merged["radius"] = merged["flight_count"] / max_count * 100_000
    max_ritardo = merged["avg_delay"].max() or 1.0

    def delay_to_color(d):
        norm = d / max_ritardo
        r = int(norm * 255)
        g = int((1 - norm) * 255)
        return [r, g, 50, 180]

    merged["color"] = merged["avg_delay"].apply(delay_to_color)
    return merged[["iata", "airport_name", "latitude", "longitude", "flight_count", "avg_delay", "radius", "color"]]


def calcola_puntualita(df):
    df = df.withColumn(
        "Status",
        F.when((df["ArrDelay"] <= 0) & (df["Cancelled"] == 0), "In Orario")
        .when((df["ArrDelay"] > 0) & (df["Cancelled"] == 0), "In Ritardo")
        .otherwise("Deviato")
    )
    status_count = df.groupBy("Status").agg(F.count("*").alias("Count"))
    voli_totali = df.count()
    status_percentuale = status_count.withColumn(
        "Percentage", (status_count["Count"] / voli_totali) * 100
    )
    return status_percentuale

def aggregazione_voli_per_mese(df):
    agg_df = df.groupBy("Month").agg(F.count("*").alias("NumeroVoli")).orderBy("Month")
    return agg_df

def calcola_kpi(df):
    totale_voli = df.count()
    voli_cancellati = df.filter(col("Cancelled") == 1).count()
    voli_deviati = df.filter(col("Diverted") == 1).count()
    ritardo_medio_partenza = df.filter(col("DepDelay").isNotNull()) \
        .agg(avg("DepDelay").alias("avgDepDelay")).collect()[0]["avgDepDelay"]
    ritardo_medio_arrivo = df.filter(col("ArrDelay").isNotNull()) \
        .agg(avg("ArrDelay").alias("avgArrDelay")).collect()[0]["avgArrDelay"]
    distanza_media = df.filter(col("Distance").isNotNull()) \
        .agg(avg("Distance").alias("avgDistance")).collect()[0]["avgDistance"]
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