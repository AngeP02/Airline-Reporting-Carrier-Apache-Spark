import streamlit as st
from graphframes import GraphFrame
from pyspark.sql.functions import col, lit
import pandas as pd
import pydeck as pdk

def mostra_graph(flights_df, aeroporti_info):
    st.title("Analisi Grafi sulle Rotte Aeree")

    # Selezione aeroporto da "far cadere"
    all_airports = flights_df.select("Origin").distinct().union(flights_df.select("Dest").distinct()).distinct().rdd.flatMap(lambda x: x).collect()
    nodo_rimosso = st.selectbox("Simula la caduta di un aeroporto", options=["Nessuno"] + sorted(all_airports))

    # Filtra voli se viene rimosso un nodo
    if nodo_rimosso != "Nessuno":
        filtered_df = flights_df.filter((col("Origin") != nodo_rimosso) & (col("Dest") != nodo_rimosso))
    else:
        filtered_df = flights_df

    # Costruisci vertici e archi per GraphFrame
    edges = filtered_df.select(col("Origin").alias("src"), col("Dest").alias("dst")).distinct()
    vertices = edges.select("src").union(edges.select("dst")).distinct().withColumnRenamed("src", "id")

    # Crea il grafo
    g = GraphFrame(vertices, edges)

    # Selezione analisi
    analisi = st.selectbox("Seleziona analisi da eseguire", [
        "PageRank", "Componenti Connesse", "Motif Finding", "In-Degree", "Out-Degree", "Triangoli"
    ])

    if analisi == "PageRank":
        result = g.pageRank(resetProbability=0.15, maxIter=10)
        df_result = result.vertices.select("id", "pagerank").toPandas()
        df_result = df_result.merge(aeroporti_info, left_on="id", right_on="iata")

    elif analisi == "Componenti Connesse":
        result = g.connectedComponents()
        df_result = result.select("id", "component").toPandas()
        df_result = df_result.merge(aeroporti_info, left_on="id", right_on="iata")

    elif analisi == "Motif Finding":
        result = g.find("(a)-[]->(b); (b)-[]->(c); (a)-[]->(c)")
        df_result = result.toPandas()

    elif analisi == "In-Degree":
        df_result = g.inDegrees.toPandas()
        df_result = df_result.merge(aeroporti_info, left_on="id", right_on="iata")

    elif analisi == "Out-Degree":
        df_result = g.outDegrees.toPandas()
        df_result = df_result.merge(aeroporti_info, left_on="id", right_on="iata")

    elif analisi == "Triangoli":
        df_result = g.triangleCount().select("id", "count").toPandas()
        df_result = df_result.merge(aeroporti_info, left_on="id", right_on="iata")

    # Mostra tabella
    st.subheader("Risultati Tabellari")
    st.dataframe(df_result)

    # Se ha lat/lon, mostra mappa
    if "latitude" in df_result.columns and "longitude" in df_result.columns:
        st.subheader("Visualizzazione Geografica")

        layer = pdk.Layer(
            "ScatterplotLayer",
            data=df_result,
            get_position='[longitude, latitude]',
            get_color='[200, 30, 0, 160]',
            get_radius=50000,
            pickable=True,
        )

        st.pydeck_chart(pdk.Deck(
            map_style="mapbox://styles/mapbox/light-v9",
            initial_view_state=pdk.ViewState(
                latitude=39.5,
                longitude=-98.35,
                zoom=3.5,
                pitch=0,
            ),
            layers=[layer],
            tooltip={"text": "{City} - {id}" + (
                "\nValore: " + (
                    "{pagerank}" if analisi == "PageRank"
                    else "{component}" if analisi == "Componenti Connesse"
                    else "{count}" if analisi == "Triangoli"
                    else "{inDegree}" if analisi == "In-Degree"
                    else "{outDegree}" if analisi == "Out-Degree"
                    else ""
                )
            )}
        ))
    else:
        st.info("Nessuna visualizzazione geografica disponibile per questo tipo di analisi.")
