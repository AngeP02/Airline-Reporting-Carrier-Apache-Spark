from pyspark.sql import functions as F
import pydeck as pdk
import pandas as pd



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




def tratte_partenza_aeroporto(df, codice_aeroporto):
    """
    Funzione per estrarre le tratte aeree che partono dall'aeroporto specificato
    come parametro 'codice_aeroporto'.

    :param df: DataFrame con i dati dei voli
    :param codice_aeroporto: Codice IATA dell'aeroporto di partenza
    :return: DataFrame con le tratte partenti da aeroporto specificato
    """
    # Filtra i voli partenti dall'aeroporto specificato
    tratte_partenza = df.filter(F.col("Origin") == codice_aeroporto) \
        .select("Origin", "Dest", "FlightDate", "Tail_Number") \
        .distinct()

    return tratte_partenza


def crea_mappa_tratte(df, cod_aeroporto, airports_df):
    """
    Crea una mappa delle tratte aeree per un aeroporto di partenza specificato
    utilizzando pydeck per la visualizzazione.

    :param df: DataFrame con i dati dei voli
    :param cod_aeroporto: Codice IATA dell'aeroporto di partenza
    :param airports_df: DataFrame con i dati degli aeroporti (latitude, longitude)
    :return: Oggetto pydeck.Deck con le tratte disegnate
    """
    # Estrai tratte partenti dall'aeroporto
    tratte = tratte_partenza_aeroporto(df, cod_aeroporto).toPandas()

    # Estrai i dettagli dell'aeroporto di partenza
    aeroporto_partenza = airports_df[airports_df['iata'] == cod_aeroporto].iloc[0]
    lat_partenza = aeroporto_partenza['latitude']
    lon_partenza = aeroporto_partenza['longitude']

    # Lista di rotte per la visualizzazione
    route_data = []
    for index, row in tratte.iterrows():
        aeroporto_destinazione = airports_df[airports_df['iata'] == row['Dest']].iloc[0]
        lat_destinazione = aeroporto_destinazione['latitude']
        lon_destinazione = aeroporto_destinazione['longitude']

        # Aggiungi le rotte (origine → destinazione)
        route_data.append({
            "origin_lat": lat_partenza,
            "origin_lon": lon_partenza,
            "dest_lat": lat_destinazione,
            "dest_lon": lon_destinazione,
            "flight_count": 1  # Puoi anche considerare il numero di voli qui
        })

    route_df = pd.DataFrame(route_data)

    # Creazione della mappa
    routes_layer = pdk.Layer(
        "ArcLayer",
        data=route_df,
        get_source_position=["origin_lon", "origin_lat"],
        get_target_position=["dest_lon", "dest_lat"],
        get_width=5,
        get_source_color=[255, 0, 0, 200],
        get_target_color=[0, 255, 0, 200],
        pickable=True,
        auto_highlight=True,
    )

    view_state = pdk.ViewState(
        latitude=lat_partenza,
        longitude=lon_partenza,
        zoom=4,
        pitch=0
    )

    deck = pdk.Deck(
        layers=[routes_layer],
        initial_view_state=view_state,
        map_style="mapbox://styles/mapbox/light-v10",
        tooltip={
            "html": "<b>Rotta:</b> {origin_lat}, {origin_lon} → {dest_lat}, {dest_lon}<br/>"
                    "<b>Voli:</b> {flight_count}",
            "style": {
                "backgroundColor": "steelblue",
                "color": "white"
            }
        }
    )

    return deck
