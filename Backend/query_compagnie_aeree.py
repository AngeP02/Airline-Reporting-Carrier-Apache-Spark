from pyspark.sql import functions as F, Window
from pyspark.sql.functions import col, avg

def lista_compagnie_uniche(df):
    return [row['IATA_CODE_Reporting_Airline'] for row in df.select('IATA_CODE_Reporting_Airline').distinct().collect()]

def statistiche_generali_compagnia(df, compagnia):
    sottoinsieme_comp = df.filter(F.col('IATA_CODE_Reporting_Airline') == compagnia)
    num = sottoinsieme_comp.count()
    nome = sottoinsieme_comp.select('Reporting_Airline').first()[0] if num > 0 else None
    return {'nome_completo': nome, 'iata_code': compagnia, 'num_voli': num}


def classifiche_compagnie(df):
    stats = df.groupBy('IATA_CODE_Reporting_Airline') \
             .agg(
                 F.avg('DepDelayMinutes').alias('AvgDelay'),
                 (F.sum(F.col('Cancelled'))/F.count('*')*100).alias('PctCancelled'),
                 F.count('*').alias('NumFlights')
             )
    window_all = Window.orderBy(F.col('AvgDelay').asc())
    stats = stats.withColumn('rank_ritardi', F.row_number().over(window_all))
    window_can = Window.orderBy(F.col('PctCancelled').asc())
    stats = stats.withColumn('rank_cancellazioni', F.row_number().over(window_can))
    window_vol = Window.orderBy(F.col('NumFlights').desc())
    stats = stats.withColumn('rank_voli', F.row_number().over(window_vol))
    return stats.select(
        F.col('IATA_CODE_Reporting_Airline').alias('Carrier'),
        'rank_ritardi', 'rank_cancellazioni', 'rank_voli'
    )


def top5_aeroporti_partenza_compagnia(df, compagnia):
    return df.filter(F.col('IATA_CODE_Reporting_Airline') == compagnia) \
             .groupBy('Origin') \
             .agg(F.count('*').alias('flight_count')) \
             .orderBy(F.col('flight_count').desc()) \
             .limit(5)


def top5_citta_servite_compagnia(df, compagnia):
    return df.filter(F.col('IATA_CODE_Reporting_Airline') == compagnia) \
             .groupBy('DestCityName') \
             .agg(F.count('*').alias('flight_count')) \
             .withColumnRenamed('DestCityName', 'City') \
             .orderBy(F.col('flight_count').desc()) \
             .limit(5)


def ranking_mensile_performance_compagnia(df, compagnia):
    sottoinsieme_comp = df.filter(F.col('IATA_CODE_Reporting_Airline') == compagnia)
    mese = sottoinsieme_comp.withColumn('Month', F.month('FlightDate')) \
                    .groupBy('Month') \
                    .agg(
                        F.avg('DepDelayMinutes').alias('AvgDelay'),
                        (F.sum(F.col('Cancelled'))/F.count('*')*100).alias('PctCancelled'),
                        (F.sum((F.col('DepDelayMinutes') <= 0).cast('int'))/F.count('*')*100).alias('PctOnTime')
                    )
    window = Window.partitionBy()
    mese = mese.withColumn('norm_delay', (F.max('AvgDelay').over(window) - F.col('AvgDelay'))/(F.max('AvgDelay').over(window)-F.min('AvgDelay').over(window)))
    mese = mese.withColumn('norm_canc', (F.max('PctCancelled').over(window) - F.col('PctCancelled'))/(F.max('PctCancelled').over(window)-F.min('PctCancelled').over(window)))
    mese = mese.withColumn('norm_ontime', (F.col('PctOnTime')-F.min('PctOnTime').over(window))/(F.max('PctOnTime').over(window)-F.min('PctOnTime').over(window)))
    mese = mese.withColumn('Score', F.col('norm_delay')*0.4 + F.col('norm_canc')*0.3 + F.col('norm_ontime')*0.3)
    return mese.select('Month', 'Score').orderBy('Month')

def distribuzione_voli_per_aeroporto_compagnia(df, compagnia):
    return df.filter(F.col('IATA_CODE_Reporting_Airline') == compagnia) \
             .groupBy('Origin') \
             .agg(F.count('*').alias('flight_count')) \
             .orderBy(F.col('flight_count').desc())

def distribuzione_voli_per_citta_compagnia(df, compagnia):
    return df.filter(F.col('IATA_CODE_Reporting_Airline') == compagnia) \
             .groupBy('DestCityName') \
             .agg(F.count('*').alias('flight_count')) \
             .withColumnRenamed('DestCityName', 'City') \
             .orderBy(F.col('flight_count').desc())

def statistiche_compagnia_aerea(compagnia_aerea, df=None):
    query = df.filter(col("Reporting_Airline") == compagnia_aerea)
    totale_voli = query.count()
    res = query.agg(
        avg("DepDelayMinutes").alias("ritardo_partenza"),
        avg("ArrDelayMinutes").alias("ritardo_arrivo")
    ).collect()[0]
    ritardo_medio_partenza = res["ritardo_partenza"]
    ritardo_medio_arrivo = res["ritardo_arrivo"]
    voli_cancellati = query.filter(col("Cancelled") == 1).count()
    voli_dirottati = query.filter(col("Diverted") == 1).count()
    statistiche = {
        "Compagnia Aerea": compagnia_aerea,
        "Totale Voli": totale_voli,
        "Ritardo Medio Partenza": ritardo_medio_partenza,
        "Ritardo Medio Arrivo": ritardo_medio_arrivo,
        "Voli Cancellati": voli_cancellati,
        "Voli Dirottati": voli_dirottati
    }
    return statistiche