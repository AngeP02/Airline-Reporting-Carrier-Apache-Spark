import streamlit as st
from graphframes import GraphFrame
import pydeck as pdk
import pandas as pd
from pyspark.sql.functions import col
from pyspark.sql import functions as F


def mostra_grafo(flights_df, aeroporti_info):
    st.title("🔗 Analisi del grafo delle tratte aeree")

    # Nodo da disconnettere
    tutti_aeroporti = flights_df.select("Origin").union(flights_df.select("Dest")).distinct().rdd.flatMap(
        lambda x: x).collect()
    aeroporto_da_escludere = st.selectbox("✈️ Simula la caduta di un aeroporto", ["Nessuno"] + sorted(tutti_aeroporti))

    # Filtraggio in caso di "caduta"
    if aeroporto_da_escludere != "Nessuno":
        filtered_df = flights_df.filter(
            (col("Origin") != aeroporto_da_escludere) & (col("Dest") != aeroporto_da_escludere))
    else:
        filtered_df = flights_df

    # Creazione grafo
    vertices = filtered_df.select(col("Origin").alias("id")).union(
        filtered_df.select(col("Dest").alias("id"))).distinct()
    edges = filtered_df.select(col("Origin").alias("src"), col("Dest").alias("dst"))
    filtered_df.cache()
    vertices.cache()
    edges.cache()

    g = GraphFrame(vertices, edges)
    g.vertices.cache()
    g.edges.cache()

    # Scelta metrica
    metrica = st.selectbox("📊 Seleziona la metrica da visualizzare",
                           ["PageRank", "Componenti Connesse", "Motif","Motif: Scalo obbligato", "In-Degree", "Out-Degree", "Triangoli"])

    df_result = None

    if metrica == "PageRank":
        result = g.pageRank(resetProbability=0.15, maxIter=10)
        df_result = result.vertices.select("id", "pagerank").toPandas()
        metrica_col = "pagerank"
        title = "PageRank"

    elif metrica == "Componenti Connesse":
        result = g.connectedComponents()
        df_result = result.select("id", "component").toPandas()
        metrica_col = "component"
        title = "Componenti connesse"

    elif metrica == "Motif":

        motif = g.find("(a)-[]->(b); (b)-[]->(c); (a)-[]->(c)") \
         \
            .filter("a.id != b.id AND b.id != c.id AND a.id != c.id")

        df_result = motif.selectExpr("a.id as A", "b.id as B", "c.id as C").toPandas()

        st.subheader("🔁 Motif (triangoli diretti trovati)")

        st.dataframe(df_result)

        # JOIN con aeroporti_info per ottenere lat/lon

        df_result = df_result \
         \
            .merge(aeroporti_info, left_on="A", right_on="iata").rename(columns={"latitude": "latA", "longitude": "lonA"}) \
         \
            .merge(aeroporti_info, left_on="B", right_on="iata").rename(columns={"latitude": "latB", "longitude": "lonB"}) \
         \
            .merge(aeroporti_info, left_on="C", right_on="iata").rename(columns={"latitude": "latC", "longitude": "lonC"})

        # Creazione linee per visualizzare triangoli orientati

        layers = []

        for _, row in df_result.iterrows():
            data_triangolo = pd.DataFrame({

                "start_lat": [row.latA, row.latB, row.latC],

                "start_lon": [row.lonA, row.lonB, row.lonC],

                "end_lat": [row.latB, row.latC, row.latA],

                "end_lon": [row.lonB, row.lonC, row.lonA],

            })

            layer = pdk.Layer(

                "LineLayer",

                data=data_triangolo,

                get_source_position='[start_lon, start_lat]',

                get_target_position='[end_lon, end_lat]',

                get_width=3,

                get_color=[255, 0, 0],

                pickable=True

            )

            layers.append(layer)

        view_state = pdk.ViewState(

            latitude=df_result[["latA", "latB", "latC"]].values.mean(),

            longitude=df_result[["lonA", "lonB", "lonC"]].values.mean(),

            zoom=3

        )

        st.subheader("🗺️ Mappa dei triangoli orientati")

        st.pydeck_chart(pdk.Deck(

            map_style="mapbox://styles/mapbox/light-v9",

            layers=layers,

            initial_view_state=view_state,

            tooltip={"text": "Triangolo orientato A→B→C→A"}

        ))

        return

    elif metrica == "Motif: Scalo obbligato":
        st.subheader("🛬 Motif: Aeroporti collegati solo tramite scalo (senza volo diretto)")

        # Trova (a) → (b) → (c)
        motif = g.find("(a)-[]->(b); (b)-[]->(c)").filter("a.id != c.id")
        motif_df = motif.selectExpr("a.id as a_id", "b.id as b_id", "c.id as c_id")

        # Tutti gli archi diretti esistenti (a → c)
        edges_ac = g.edges.selectExpr("src as a_id", "dst as c_id")

        # Filtra i casi in cui NON esiste collegamento diretto tra a e c
        scalo_obbligato = motif_df.join(edges_ac, on=["a_id", "c_id"], how="left_anti")

        # Risultato finale
        df_result = scalo_obbligato.toPandas()
        st.dataframe(df_result)

        # [Opzionale] Numero di casi
        st.markdown(f"🔎 Trovati {len(df_result)} casi di collegamento indiretto con scalo obbligato.")
        return

    elif metrica == "In-Degree":
        df_result = g.inDegrees.toPandas()
        df_result.rename(columns={"inDegree": "valore"}, inplace=True)
        metrica_col = "valore"
        title = "In-degree"

    elif metrica == "Out-Degree":
        df_result = g.outDegrees.toPandas()
        df_result.rename(columns={"outDegree": "valore"}, inplace=True)
        metrica_col = "valore"
        title = "Out-degree"

    elif metrica == "Triangoli":
        df_result = g.triangleCount().toPandas()
        df_result.rename(columns={"count": "triangoli"}, inplace=True)
        metrica_col = "triangoli"
        title = "Conteggio triangoli"

    # Join con info aeroporti per mappa
    df_result = df_result.merge(aeroporti_info, left_on="id", right_on="iata")
    df_result.rename(columns={
        "iata": "IATA",
        "latitude": "Latitude",
        "longitude": "Longitude",
        "city": "City"
    }, inplace=True)

    if metrica == "PageRank":
        df_result = df_result.sort_values(by="pagerank", ascending=False).reset_index(drop=True)
        df_result["is_hub"] = False
        df_result.loc[:2, "is_hub"] = True  # Solo i primi 3
    else:
        df_result["is_hub"] = False

    # Visualizzazione tabellare
    st.subheader(f"📋 Dati: {title}")
    st.dataframe(df_result[["IATA", "City", metrica_col]])

    # Visualizzazione geografica
    st.subheader("🗺️ Mappa interattiva")
    # Normalizzazione per il raggio visibile
    valori = df_result[metrica_col].astype(float)
    min_val, max_val = valori.min(), valori.max()
    df_result["radius_norm"] = ((valori - min_val) / (max_val - min_val + 1e-9)) * 200000 + 30000

    # Colori precalcolati
    def calcola_colore(is_hub):
        return [85,107,47] if is_hub else [143,188,143]  # Rosso se hub, blu altrimenti

    df_result["color"] = df_result["is_hub"].apply(calcola_colore)

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df_result,
        get_position='[Longitude, Latitude]',
        get_fill_color="color",
        get_radius="radius_norm",
        pickable=True,
        opacity=0.8,
    )

    view_state = pdk.ViewState(
        latitude=df_result["Latitude"].mean(),
        longitude=df_result["Longitude"].mean(),
        zoom=3,
        pitch=0
    )

    tooltip = {"text": "IATA: {IATA}\nCity: {City}\nValore: {" + metrica_col + "}"}

    st.pydeck_chart(pdk.Deck(
        map_style="mapbox://styles/mapbox/light-v9",
        layers=[layer],
        initial_view_state=view_state,
        tooltip=tooltip
    ))
