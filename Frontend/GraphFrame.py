import pandas as pd
import streamlit as st
from graphframes import GraphFrame
import pydeck as pdk
from pyspark.sql.functions import col
import distinctipy

@st.cache_resource
def lista_aeroporti(_df):
    return _df.select("Origin").union(_df.select("Dest")).distinct().rdd.flatMap(lambda x: x).collect()

@st.cache_resource
def crea_grafo_base(_df):
    vertici = _df.select(col("Origin").alias("id")).union(_df.select(col("Dest").alias("id"))).distinct()
    archi = _df.select(col("Origin").alias("src"), col("Dest").alias("dst"))
    return GraphFrame(vertici, archi)

@st.cache_resource
def filtra_grafo(_grafo, _aeroporto):
    if _aeroporto == "Nessuno":
        return _grafo
    archi_filtrati = _grafo.edges.filter((col("src") != _aeroporto) & (col("dst") != _aeroporto))
    vertici_filtrati = archi_filtrati.select("src").union(archi_filtrati.select("dst")).distinct().withColumnRenamed("src", "id")
    vertici_filtrati = vertici_filtrati.filter(col("id") != _aeroporto)
    return GraphFrame(vertici_filtrati, archi_filtrati)

def calcola_colore(is_hub):
    return [85, 107, 47] if is_hub else [143, 188, 143]

def mostra_grafo(df, aeroporti_info):
    st.title("Analisi tramite grafo delle tratte aeree")

    tutti_aeroporti = lista_aeroporti(df)
    aeroporto_da_escludere = st.selectbox("Simula la caduta di un aeroporto", ["Nessuno"] + sorted(tutti_aeroporti))

    if aeroporto_da_escludere != "Nessuno":
        filtrato = df.filter((col("Origin") != aeroporto_da_escludere) & (col("Dest") != aeroporto_da_escludere))
    else:
        filtrato = df

    filtrato.cache()
    grafo_base = crea_grafo_base(filtrato)
    g = filtra_grafo(grafo_base, aeroporto_da_escludere)
    g.vertices.cache()
    g.edges.cache()

    metrica = st.selectbox("Seleziona la metrica da visualizzare", [
        "PageRank",
        "Componenti Connesse",
        "In-Degree",
        "Out-Degree",
        "Triangoli",
        "Componenti Fortemente Connesse",
        "Label Propagation"
    ])

    df_risultante = None
    metrica_col = ""
    titolo = ""

    if metrica == "PageRank":
        risultato = g.pageRank(resetProbability=0.15, maxIter=10)
        df_risultante = risultato.vertices.select("id", "pagerank").toPandas()
        df_risultante = df_risultante.sort_values(by="pagerank", ascending=False).reset_index(drop=True)
        metrica_col = "pagerank"
        titolo = "PageRank"

    elif metrica == "Componenti Connesse":
        risultato = g.connectedComponents()
        df_risultante = risultato.select("id", "component").toPandas()
        if df_risultante["component"].nunique() == 1:
            st.success("Il grafo è completamente connesso: esiste una sola componente.")
            return

    elif metrica == "Componenti Fortemente Connesse":
        risultato = g.stronglyConnectedComponents(maxIter=10)
        df_risultante = risultato.select("id", "component").toPandas()
        titolo = "Componenti Fortemente Connesse"
        metrica_col = "component"

    elif metrica == "Label Propagation":
        risultato = g.labelPropagation(maxIter=5)
        df_risultante = risultato.select("id", "label").toPandas()
        titolo = "Comunità (Label Propagation)"
        metrica_col = "label"

    elif metrica == "In-Degree":
        df_risultante = g.inDegrees.toPandas()
        df_risultante.rename(columns={"inDegree": "valore"}, inplace=True)
        metrica_col = "valore"
        titolo = "In-degree"

    elif metrica == "Out-Degree":
        df_risultante = g.outDegrees.toPandas()
        df_risultante.rename(columns={"outDegree": "valore"}, inplace=True)
        metrica_col = "valore"
        titolo = "Out-degree"

    elif metrica == "Triangoli":
        df_risultante = g.triangleCount().toPandas()
        df_risultante.rename(columns={"count": "triangoli"}, inplace=True)
        metrica_col = "triangoli"
        titolo = "Conteggio triangoli"

    df_risultante = df_risultante.merge(aeroporti_info, left_on="id", right_on="iata")
    df_risultante.rename(columns={
        "iata": "IATA",
        "latitude": "Latitude",
        "longitude": "Longitude",
        "city": "City"
    }, inplace=True)

    if metrica == "PageRank":
        df_risultante = df_risultante.sort_values(by="pagerank", ascending=False).reset_index(drop=True)
        df_risultante["is_hub"] = False
        df_risultante.loc[:2, "is_hub"] = True
    else:
        df_risultante["is_hub"] = False

    st.subheader(f"Dati: {titolo}")
    st.dataframe(df_risultante[["IATA", "City", metrica_col]])

    if metrica in ["In-Degree", "Out-Degree"]:
        in_df = g.inDegrees.toPandas().rename(columns={"inDegree": "inDegree"})
        out_df = g.outDegrees.toPandas().rename(columns={"outDegree": "outDegree"})
        degree_df = pd.merge(in_df, out_df, on="id", how="outer").fillna(0)
        degree_df["differenza"] = (degree_df["inDegree"] - degree_df["outDegree"]).abs()
        soglia = 20
        sbilanciati = degree_df[degree_df["differenza"] >= soglia]
        if not sbilanciati.empty:
            st.warning(f"{len(sbilanciati)} aeroporti sbilanciati (differenza > 20) trovati")
            st.dataframe(sbilanciati.sort_values(by="differenza", ascending=False))
        else:
            st.success("Nessun aeroporto mostra uno sbilanciamento significativo tra arrivi e partenze.")

    st.subheader("Mappa interattiva")
    categorici = metrica in [
        "Componenti Connesse",
        "Componenti Fortemente Connesse",
        "Label Propagation"
    ]

    if categorici:
        etichette_uniche = df_risultante[metrica_col].unique()
        colori = distinctipy.get_colors(len(etichette_uniche))
        etichette_coloti = {
            label: [int(c * 255) for c in color]
            for label, color in zip(etichette_uniche, colori)
        }
        df_risultante["color"] = df_risultante[metrica_col].map(etichette_coloti)
        df_risultante["radius_norm"] = 70000
    else:
        valori = df_risultante[metrica_col].astype(float)
        min_val, max_val = valori.min(), valori.max()
        df_risultante["radius_norm"] = ((valori - min_val) / (max_val - min_val + 1e-9)) * 200000 + 30000
        df_risultante["color"] = df_risultante["is_hub"].apply(calcola_colore)

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df_risultante,
        get_position='[Longitude, Latitude]',
        get_fill_color="color",
        get_radius="radius_norm",
        pickable=True,
        opacity=0.8,
    )
    view_state = pdk.ViewState(
        latitude=df_risultante["Latitude"].mean(),
        longitude=df_risultante["Longitude"].mean(),
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