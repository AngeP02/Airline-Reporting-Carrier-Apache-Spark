
### Backend: query_compagnia.py
from pyspark.sql import functions as F, Window


def lista_compagnie_uniche(df):
    return [row['IATA_CODE_Reporting_Airline'] for row in df.select('IATA_CODE_Reporting_Airline').distinct().collect()]


def statistiche_generali_compagnia(df, compagnia):
    subset = df.filter(F.col('IATA_CODE_Reporting_Airline') == compagnia)
    num = subset.count()
    nome = subset.select('Reporting_Airline').first()[0] if num > 0 else None
    return {'nome_completo': nome, 'iata_code': compagnia, 'num_voli': num}


def classifiche_compagnie(df):
    # calcola ritardo medio, % cancellazioni, num voli per compagnia
    stats = df.groupBy('IATA_CODE_Reporting_Airline') \
             .agg(
                 F.avg('DepDelayMinutes').alias('AvgDelay'),
                 (F.sum(F.col('Cancelled'))/F.count('*')*100).alias('PctCancelled'),
                 F.count('*').alias('NumFlights')
             )
    # aggiungo ranking
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
    # calcola per mese uno score combinato
    subset = df.filter(F.col('IATA_CODE_Reporting_Airline') == compagnia)
    monthly = subset.withColumn('Month', F.month('FlightDate')) \
                    .groupBy('Month') \
                    .agg(
                        F.avg('DepDelayMinutes').alias('AvgDelay'),
                        (F.sum(F.col('Cancelled'))/F.count('*')*100).alias('PctCancelled'),
                        (F.sum((F.col('DepDelayMinutes') <= 0).cast('int'))/F.count('*')*100).alias('PctOnTime')
                    )
    # normalizzo e peso: ritardo(0.4), cancellazioni(0.3), puntualità(0.3)
    window = Window.partitionBy()
    monthly = monthly.withColumn('norm_delay', (F.max('AvgDelay').over(window) - F.col('AvgDelay'))/(F.max('AvgDelay').over(window)-F.min('AvgDelay').over(window)))
    monthly = monthly.withColumn('norm_canc', (F.max('PctCancelled').over(window) - F.col('PctCancelled'))/(F.max('PctCancelled').over(window)-F.min('PctCancelled').over(window)))
    monthly = monthly.withColumn('norm_ontime', (F.col('PctOnTime')-F.min('PctOnTime').over(window))/(F.max('PctOnTime').over(window)-F.min('PctOnTime').over(window)))
    monthly = monthly.withColumn('Score', F.col('norm_delay')*0.4 + F.col('norm_canc')*0.3 + F.col('norm_ontime')*0.3)
    return monthly.select('Month', 'Score').orderBy('Month')


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
