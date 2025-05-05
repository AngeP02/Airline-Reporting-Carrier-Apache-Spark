import pandas as pd
from plotly.subplots import make_subplots
import pyspark.sql.functions as F
from pyspark.sql.functions import to_date, count
import calendar
import plotly.graph_objects as go

from pyspark.sql.functions import col, avg
def calcola_percentuale_ritardi_15(df):
    """
    Restituisce la percentuale di voli con ritardo in partenza > 15 minuti.
    """
    totale = df.filter(col("DepDelay").isNotNull()).count()
    rit15  = df.filter(col("DepDelay") > 15).count()
    return (rit15 / totale) * 100 if totale > 0 else 0.0

def calcola_numero_aeroporti(df):
    """
    Conta tutti gli aeroporti distinti (origine o destinazione) nel dataset.
    """
    return df.select("Origin") \
             .union(df.select("Dest")) \
             .distinct() \
             .count()

def calcola_numero_citta(df, aeroporti_info):
    """
    Conta il numero di città coinvolte dalle rotte del dataset.
    Usa il DataFrame pandas `aeroporti_info` che deve contenere
    le colonne 'iata' e 'city'.
    """
    # Prendi tutti i codici IATA usati
    unique_airports = [row["Origin"]
                       for row in df.select("Origin")
                                    .union(df.select("Dest"))
                                    .distinct()
                                    .collect()]
    # Filtra il DataFrame pandas degli aeroporti
    df_info = aeroporti_info[aeroporti_info["iata"].isin(unique_airports)]
    return df_info["city"].nunique()

def calcola_top5_compagnie_puntuali(df, n=5):
    """
    Restituisce le n compagnie con il minore ritardo medio di partenza.
    """
    df_stats = df.groupBy("Reporting_Airline") \
                 .agg(avg("DepDelayMinutes").alias("Ritardo_Medio_Partenza")) \
                 .orderBy("Ritardo_Medio_Partenza")
    return df_stats.limit(n).toPandas() \
                   .rename(columns={"Reporting_Airline": "Compagnia",
                                    "Ritardo_Medio_Partenza": "Ritardo Medio (min)"})

def calcola_top5_aeroporti_piu_ritardi(df, n=5):
    """
    Restituisce i n aeroporti con il maggior numero di voli in ritardo (>0 min partenza).
    """
    df_delayed = df.filter(col("DepDelayMinutes") > 0)
    df_stats  = df_delayed.groupBy("Origin") \
                         .agg(count("*").alias("Numero_Ritardi")) \
                         .orderBy(col("Numero_Ritardi").desc())
    return df_stats.limit(n).toPandas() \
                   .rename(columns={"Origin": "Aeroporto",
                                    "Numero_Ritardi": "Voli in Ritardo"})

def calcola_top5_tratte_trafficate(df, n=5):
    """
    Restituisce le n coppie (Origine, Destinazione) con più voli totali.
    """
    df_stats = df.groupBy("Origin", "Dest") \
                 .agg(count("*").alias("Numero_Voli")) \
                 .orderBy(col("Numero_Voli").desc())
    return df_stats.limit(n).toPandas() \
                   .rename(columns={"Origin": "Origine",
                                    "Dest": "Destinazione",
                                    "Numero_Voli": "Voli"})

def calcola_top5_compagnie_piu_voli(df, n=5):
    """
    Restituisce le n compagnie con il maggior numero di voli totali.
    """
    df_stats = df.groupBy("Reporting_Airline") \
                 .agg(count("*").alias("Totale_Voli")) \
                 .orderBy(col("Totale_Voli").desc())
    return df_stats.limit(n).toPandas() \
                   .rename(columns={"Reporting_Airline": "Compagnia",
                                    "Totale_Voli": "Voli"})

from pyspark.sql.functions import col, count, sum as spark_sum

def calcola_top5_compagnie_meno_cancellazioni(df, n=5):
    """
    Restituisce le n compagnie con la percentuale più bassa di cancellazioni.
    """
    stats = df.groupBy("Reporting_Airline") \
              .agg(
                  count("*").alias("Totale"),                  # tutti i voli
                  spark_sum("Cancelled").alias("Voli_Cancellati")  # somma dei cancel==1
              ) \
              .withColumn("Pct_Cancellati",
                          col("Voli_Cancellati")/col("Totale")*100) \
              .orderBy(col("Pct_Cancellati").asc())

    # Converto in pandas e rinomino per il grafico
    return stats.limit(n).toPandas() \
                .rename(columns={
                    "Reporting_Airline": "Compagnia",
                    "Pct_Cancellati": "% Cancellazioni"
                })


def calcola_mappa_aeroporti(df, aeroporti_info):
    """
    Restituisce un DataFrame pandas con, per ogni aeroporto:
      - iata: codice aeroporto
      - latitude, longitude
      - flight_count: numero totale di voli (arrivi+partenze)
      - avg_delay: ritardo medio in partenza (minuti)
      - radius: raggio normalizzato per pydeck
      - color: lista RGBA in base al ritardo medio
    """
    # Unisco origini e destinazioni
    orig = df.select(col("Origin").alias("iata"),
                     col("DepDelayMinutes").alias("DepDelay"))
    dest = df.select(col("Dest").alias("iata"),
                     col("DepDelayMinutes").alias("DepDelay"))
    all_flights = orig.union(dest)

    # Calcolo conteggio e ritardo medio
    stats = all_flights.groupBy("iata") \
        .agg(
            count("*").alias("flight_count"),
            avg("DepDelay").alias("avg_delay")
        )
    pdf = stats.toPandas()

    # Merge con info aeroporti
    merged = pdf.merge(
        aeroporti_info[["iata",  "name", "latitude", "longitude"]],
        on="iata",
        how="left"
    ).dropna(subset=["latitude", "longitude"]).rename(
        columns={"name": "airport_name"}
    )

    # Normalizzo raggio (usiamo 100 000 come raggio max)
    max_count = merged["flight_count"].max()
    merged["radius"] = merged["flight_count"] / max_count * 100_000

    # Normalizzo ritardo e creo colore RGBA
    max_delay = merged["avg_delay"].max() or 1.0
    def delay_to_color(d):
        norm = d / max_delay
        r = int(norm * 255)
        g = int((1 - norm) * 255)
        return [r, g, 50, 180]  # blu fisso a 50, alpha 180
    merged["color"] = merged["avg_delay"].apply(delay_to_color)

    return merged[["iata", "airport_name", "latitude", "longitude", "flight_count", "avg_delay", "radius", "color"]]




def crea_heatmap_calendario(df_pandas):
    df_pandas["FlightDate"] = pd.to_datetime(df_pandas["FlightDate"])
    df_pandas["Year"] = df_pandas["FlightDate"].dt.year
    df_pandas["Month"] = df_pandas["FlightDate"].dt.month
    df_pandas["Day"] = df_pandas["FlightDate"].dt.day
    df_pandas["Weekday"] = df_pandas["FlightDate"].dt.weekday  # Monday=0
    df_pandas["Week"] = df_pandas["FlightDate"].dt.isocalendar().week

    # Per ogni mese, costruisci un subplot
    fig = make_subplots(rows=3, cols=4, subplot_titles=[calendar.month_name[i] for i in range(1, 13)],
                        vertical_spacing=0.05, horizontal_spacing=0.03)

    for month in range(1, 13):
        df_mese = df_pandas[df_pandas["Month"] == month]
        if df_mese.empty:
            continue

        # Calcola settimana del mese
        df_mese["WeekOfMonth"] = df_mese["FlightDate"].apply(lambda d: (d.day - 1) // 7)

        z = []
        x_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        y_labels = sorted(df_mese["WeekOfMonth"].unique())

        for week in y_labels:
            row_vals = []
            for wd in range(7):
                match = df_mese[(df_mese["Weekday"] == wd) & (df_mese["WeekOfMonth"] == week)]
                if not match.empty:
                    row_vals.append(match["Numero_Voli"].values[0])
                else:
                    row_vals.append(None)
            z.append(row_vals)

        r = (month - 1) // 4 + 1
        c = (month - 1) % 4 + 1
        fig.add_trace(go.Heatmap(z=z, x=x_labels, y=[f"Week {w + 1}" for w in y_labels],
                                 colorscale='YlOrRd', showscale=False), row=r, col=c)

    fig.update_layout(height=900, width=1200, title_text="Heatmap Calendario Giornaliero per Mese")
    return fig


def calcola_voli_per_giorno(df, compagnie=None):
    df_filtrato = df.select("FlightDate", "IATA_CODE_Reporting_Airline")

    if compagnie:
        df_filtrato = df_filtrato.filter(col("IATA_CODE_Reporting_Airline").isin(compagnie))

    df_filtrato = df_filtrato.withColumn("FlightDate", to_date(col("FlightDate")))

    voli_per_giorno = df_filtrato.groupBy("FlightDate").agg(count("*").alias("Numero_Voli"))

    return voli_per_giorno


def mappa_rotte(flights_df, airports_pd):
    # Carichiamo info aeroporti come DataFrame Spark
    spark = flights_df.sql_ctx.sparkSession
    airports_df = spark.createDataFrame(airports_pd)

    # Conta voli per ogni rotta
    route_counts = flights_df.groupBy('Origin', 'Dest').agg(
        F.count('*').alias('flight_count'),
        F.collect_set('Reporting_Airline').alias('airlines')
    )

    # Seleziona latitudine e longitudine origine
    origin_info = airports_df.select(
        F.col('iata').alias('Origin'),
        F.col('latitude').alias('origin_lat'),
        F.col('longitude').alias('origin_lon')
    )

    # Seleziona latitudine e longitudine destinazione
    dest_info = airports_df.select(
        F.col('iata').alias('Dest'),
        F.col('latitude').alias('dest_lat'),
        F.col('longitude').alias('dest_lon')
    )

    # Join
    enriched_routes = route_counts \
        .join(origin_info, on='Origin', how='left') \
        .join(dest_info, on='Dest', how='left') \
        .dropna(subset=['origin_lat', 'origin_lon', 'dest_lat', 'dest_lon'])

    return enriched_routes

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





def aggregazione_voli_per_mese(df):
    agg_df = df.groupBy("Month").agg(F.count("*").alias("NumeroVoli")).orderBy("Month")
    return agg_df


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
