import streamlit as st
import pandas as pd
import pydeck as pdk
from graphframes import GraphFrame
from matplotlib import pyplot as plt
from pyspark.sql.functions import col, avg, desc, month
import networkx as nx
from node2vec import Node2Vec
from pyvis.network import Network
import random
import matplotlib.cm as cm
import matplotlib.colors as mcolors

@st.cache_resource
def build_graph(_flights_df):
    clean_df = _flights_df.filter("Cancelled == 0 AND Diverted == 0")
    vertices = clean_df.select(col("Origin").alias("id")).union(
        clean_df.select(col("Dest").alias("id"))
    ).distinct()
    edges = clean_df.groupBy("Origin", "Dest").agg(avg("ArrDelay").alias("weight")) \
        .select(col("Origin").alias("src"), col("Dest").alias("dst"), "weight")
    return GraphFrame(vertices, edges)

@st.cache_resource
def compute_pagerank(_g):
    return _g.pageRank(resetProbability=0.15, maxIter=10)

@st.cache_resource
def compute_components(_g):
    return _g.connectedComponents()

@st.cache_resource
def compute_motifs(_g):
    return _g.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[ca]->(a)")


@st.cache_resource
def build_monthly_graph(flights_df, selected_month):
    snapshot_df = flights_df.filter(month("FlightDate") == selected_month).filter("Cancelled == 0 AND Diverted == 0")
    return build_graph(snapshot_df)


def visualizza_mappa_dinamica(opzione, aeroporti_info, top_hubs_df=None, motifs_clean=None, components=None):
    st.write(f"Visualizzazione scelta: '{opzione}'")

    if opzione == "PageRank":
        if top_hubs_df is not None:
            dati = top_hubs_df.merge(aeroporti_info, left_on="id", right_on="iata", how="left").dropna()
            if dati.empty:
                st.warning("Nessun dato geolocalizzato dopo il merge.")
                return

            # Scala raggio per evidenziare PageRank (esempio: moltiplica pagerank per 5000)
            dati['radius'] = dati['pagerank'] * 25000

            # Colore rosso, ma con trasparenza modulata dal pagerank (più alto = più intenso)
            max_pr = dati['pagerank'].max()
            dati['color_intensity'] = (dati['pagerank'] / max_pr * 255).astype(int)
            dati['color'] = dati['color_intensity'].apply(lambda c: [200, 30, 0, c])

            # Usare una colonna di colore e radius
            st.pydeck_chart(pdk.Deck(
                map_style='mapbox://styles/mapbox/light-v9',
                initial_view_state=pdk.ViewState(latitude=37.7749, longitude=-95.0, zoom=3),
                layers=[
                    pdk.Layer(
                        'ScatterplotLayer',
                        data=dati,
                        get_position='[longitude, latitude]',
                        get_radius='radius',
                        get_fill_color='color',
                        pickable=True,
                        auto_highlight=True,
                    ),
                    # Rimosso TextLayer
                ],
                tooltip={
                    "html": "<b>Aeroporto:</b> {iata}<br><b>PageRank:</b> {pagerank}",
                    "style": {
                        "backgroundColor": "steelblue",
                        "color": "white",
                        "fontWeight": "bold",
                        "padding": "5px",
                        "borderRadius": "5px",
                    }
                }
            ))
        else:
            st.warning("Nessun dato PageRank disponibile.")



    elif opzione.lower() == "motif":
        if motifs_clean is not None and not motifs_clean.empty:
            motifs_clean['total_delay'] = (
                    motifs_clean['Delay_AB'] + motifs_clean['Delay_BC'] + motifs_clean['Delay_CA']
            )
            top_motifs = motifs_clean.sort_values('total_delay', ascending=False).head(50)

            def build_triangles(row):
                try:
                    A = aeroporti_info.loc[aeroporti_info['iata'] == row['Airport_A']]
                    B = aeroporti_info.loc[aeroporti_info['iata'] == row['Airport_B']]
                    C = aeroporti_info.loc[aeroporti_info['iata'] == row['Airport_C']]
                    if A.empty or B.empty or C.empty:
                        return None
                    return [
                        [A['longitude'].values[0], A['latitude'].values[0]],
                        [B['longitude'].values[0], B['latitude'].values[0]],
                        [C['longitude'].values[0], C['latitude'].values[0]],
                        [A['longitude'].values[0], A['latitude'].values[0]],
                    ]
                except Exception as e:
                    print(f"Errore triangolo: {e}")
                    return None

            top_motifs['triangle'] = top_motifs.apply(build_triangles, axis=1)
            colormap = cm.get_cmap('tab20', len(top_motifs))  # 20+ colori ben distinti

            def get_unique_color(idx):
                rgba = colormap(idx)
                return [int(rgba[0] * 255), int(rgba[1] * 255), int(rgba[2] * 255), int(180)]  # opacità fissa

            top_motifs['fill_color'] = [[0, 0, 0, 0] for _ in range(len(top_motifs))]
            top_motifs['line_color'] = [get_unique_color(i) for i in range(len(top_motifs))]

            data_for_layer = top_motifs.apply(lambda r: {
                'polygon': r['triangle'],
                'fill_color': r['fill_color'],
                'line_color': r['line_color'],
                'total_delay': r['total_delay'],
                'Airport_A': r['Airport_A'],
                'Airport_B': r['Airport_B'],
                'Airport_C': r['Airport_C'],
            }, axis=1).tolist()

            st.pydeck_chart(pdk.Deck(
                map_style='mapbox://styles/mapbox/light-v9',
                initial_view_state=pdk.ViewState(latitude=37.7749, longitude=-95.0, zoom=3),
                layers=[
                    pdk.Layer(
                        "PolygonLayer",
                        data=data_for_layer,
                        get_polygon="polygon",
                        get_fill_color="fill_color",
                        get_line_color="line_color",
                        line_width_min_pixels=5,
                        pickable=True,
                        auto_highlight=True,
                    )
                ],

                tooltip={
                    "html": "<b>Triangolo Motif</b><br>Aeroporti: {Airport_A}, {Airport_B}, {Airport_C}<br>Ritardo totale: {total_delay}",
                    "style": {
                        "backgroundColor": "steelblue",
                        "color": "white",
                        "fontWeight": "bold",
                        "padding": "5px",
                        "borderRadius": "5px",
                    }
                }
            ))
        else:
            st.warning("Nessun dato Motif disponibile o motifs_clean è vuoto.")
    else:
        st.warning("Seleziona una visualizzazione valida.")
        return


def mostra_graph(flights_df, aeroporti_info):
    st.title("Analisi di rete del traffico aereo")

    with st.spinner("Costruzione del grafo..."):
        g = build_graph(flights_df)

    edges_pd = g.edges.select("src", "dst").toPandas()
    Gnx = nx.from_pandas_edgelist(edges_pd, source="src", target="dst", create_using=nx.DiGraph())
    with st.expander("Visualizza rete completa"):
        plt.figure(figsize=(12, 8))
        pos = nx.spring_layout(Gnx, k=0.2, iterations=20)
        nx.draw(Gnx, pos, with_labels=True, node_size=80, font_size=7, arrows=True)
        st.pyplot(plt.gcf())


    col_analisi, col_mappa = st.columns([1,2])
    pagerank = compute_pagerank(g)
    components = compute_components(g)
    motifs = compute_motifs(g)

    top_hubs_df = pagerank.vertices.orderBy(desc("pagerank")).toPandas()
    motifs_clean = motifs.select(
        col("a.id").alias("Airport_A"),
        col("b.id").alias("Airport_B"),
        col("c.id").alias("Airport_C"),
        col("ab.weight").alias("Delay_AB"),
        col("bc.weight").alias("Delay_BC"),
        col("ca.weight").alias("Delay_CA")
    ).toPandas()

    with col_mappa:
        try:
            st.subheader("Rete aeroporti")
            options = ["PageRank", "Motif"]
            scelta = st.pills("Directions", options, selection_mode="single")
            if scelta is None:
                scelta = "PageRank"
            visualizza_mappa_dinamica(scelta, aeroporti_info,
                                      top_hubs_df=top_hubs_df,
                                      motifs_clean=motifs_clean,
                                      components=components)
        except Exception as e:
            st.error(f"Errore nella visualizzazione mappa: {e}")

    with col_analisi:
        st.subheader("Risultati analisi (1)")

        with st.spinner("Calcolo PageRank e Componenti connesse"):
            pagerank = compute_pagerank(g)
            top_hubs_df = pagerank.vertices.orderBy(desc("pagerank")).toPandas()
            components = compute_components(g)
            component_df = components.groupBy("component").count().orderBy(desc("count")).toPandas()
        st.write("Top aeroporti per PageRank:")
        st.dataframe(top_hubs_df)
        st.write("Componenti connesse:")
        st.dataframe(component_df)

        with st.spinner("Calcolo Motif Finding"):
            motifs = compute_motifs(g)
            motifs_clean = motifs.select(
                col("a.id").alias("Airport_A"),
                col("b.id").alias("Airport_B"),
                col("c.id").alias("Airport_C"),
                col("ab.weight").alias("Delay_AB"),
                col("bc.weight").alias("Delay_BC"),
                col("ca.weight").alias("Delay_CA")
            ).toPandas()
        st.write("Motifs:")
        st.dataframe(motifs_clean)

    with st.spinner("Calcolo Gradi e Triangoli"):
        indegree = g.inDegrees.orderBy(desc("inDegree")).toPandas()
        outdegree = g.outDegrees.orderBy(desc("outDegree")).toPandas()
        triangles = g.triangleCount().orderBy(desc("count")).toPandas()
    col1,col2, col3 = st.columns(3)
    with col1:
        st.write("In-degree:")
        st.dataframe(indegree)
    with col2:
        st.write("Out-degree:")
        st.dataframe(outdegree)
    with col3:
        st.write("Conteggio triangoli:")
        st.dataframe(triangles)

    with st.spinner("💥 Simulazione guasto aeroporto"):
        airport_to_remove = st.selectbox("Aeroporto da simulare come guasto", top_hubs_df["id"].tolist())
        reduced_edges = g.edges.filter((col("src") != airport_to_remove) & (col("dst") != airport_to_remove))
        reduced_vertices = g.vertices.filter(col("id") != airport_to_remove)
        g_removed = GraphFrame(reduced_vertices, reduced_edges)
        comp_removed = g_removed.connectedComponents()
    st.write(f"Connettività dopo guasto a {airport_to_remove}:")
    st.dataframe(comp_removed.groupBy("component").count().orderBy(desc("count")).toPandas())

    with st.spinner("🧠 Betweenness Centrality"):
        centrality = nx.betweenness_centrality(Gnx, weight='weight')
        cent_df = pd.DataFrame(centrality.items(), columns=["Airport", "Centrality"]).sort_values("Centrality",                                                                                      ascending=False)
    st.dataframe(cent_df.head(10))

    with st.spinner("📆 Snapshot mensile"):
        selected_month = st.slider("Seleziona mese", 1, 12, 1)
        g_month = build_monthly_graph(flights_df, selected_month)
        pagerank_month_df = g_month.pageRank(resetProbability=0.15, maxIter=5).vertices \
            .orderBy(desc("pagerank")).limit(10).toPandas()
    st.write(f"Top aeroporti per PageRank nel mese {selected_month}:")
    st.dataframe(pagerank_month_df)

    with st.spinner("🤖 Node2Vec - Nuove tratte"):
        G_undir = nx.from_pandas_edgelist(edges_pd, source='src', target='dst', edge_attr='weight')
        n2v = Node2Vec(G_undir, dimensions=32, walk_length=20, num_walks=200, workers=1)
        model = n2v.fit(window=5, min_count=1)
        aeroporto = st.selectbox("Seleziona aeroporto per suggerimenti", list(G_undir.nodes))
        similar = model.wv.most_similar(aeroporto, topn=5)
    st.write("Suggerimenti di nuove tratte:", similar)