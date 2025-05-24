from pyspark.sql.functions import col
from graphframes import GraphFrame
import pandas as pd
from pyvis.network import Network
import streamlit.components.v1 as components
import streamlit as st
import matplotlib.pyplot as plt
import plotly.express as px

def mostra_graph(flights_df):
    st.title("Analisi del Grafo delle Rotte Aeree")

    # Selezione modalità di costruzione
    st.markdown("### Seleziona una modalità per costruire il grafo")
    filtro = st.selectbox("Metodo di costruzione", [
        "Top 500 rotte più frequenti",
        "Solo voli da un aeroporto specifico",
        "Campione casuale del 2%",
    ])

    if filtro == "Top 500 rotte più frequenti":
        rotte = flights_df.groupBy("Origin", "Dest").count().orderBy("count", ascending=False).limit(500)
    elif filtro == "Solo voli da un aeroporto specifico":
        aeroporto = st.text_input("Inserisci codice aeroporto (es. JFK)", value="JFK")
        rotte = flights_df.filter(col("Origin") == aeroporto).select("Origin", "Dest").distinct()
    else:
        rotte = flights_df.sample(fraction=0.02, seed=42).select("Origin", "Dest").distinct()

    aeroporti = rotte.selectExpr("Origin as id").union(rotte.selectExpr("Dest as id")).distinct()
    archi = rotte.selectExpr("Origin as src", "Dest as dst")
    g = GraphFrame(aeroporti, archi)

    # PageRank
    with st.spinner("Calcolo PageRank..."):
        pagerank = g.pageRank(resetProbability=0.15, maxIter=10).vertices.toPandas()
    st.subheader("Aeroporti più centrali (PageRank)")
    top_pr = pagerank.sort_values("pagerank", ascending=False).head(20)
    fig = px.bar(top_pr, x="id", y="pagerank", title="Top 20 Aeroporti per PageRank", labels={"id": "Aeroporto"})
    st.plotly_chart(fig)

    # Degree
    with st.spinner("Calcolo Degree..."):
        degrees = g.degrees.toPandas()
    st.subheader("Grado (Numero di Connessioni) per Aeroporto")
    top_deg = degrees.sort_values("degree", ascending=False).head(20)
    fig2 = px.bar(top_deg, x="id", y="degree", title="Top 20 Aeroporti per Connessioni", labels={"id": "Aeroporto"})
    st.plotly_chart(fig2)

    # Connected components (comunità)
    with st.spinner("Calcolo componenti connesse..."):
        connected_components = g.connectedComponents().select("id", "component").toPandas()
    st.subheader("Comunità (Gruppi di Aeroporti Connessi)")
    n_components = connected_components["component"].nunique()
    st.markdown(f"**Numero di componenti connesse trovate:** {n_components}")
    st.markdown(f"Visualizzazione delle componenti connesse nel grafo.")

    # Motif finding
    with st.spinner("Calcolo motif A→B→C"):
        motifs = g.find("(a)-[]->(b); (b)-[]->(c)")\
            .selectExpr("a.id as from", "b.id as via", "c.id as to")\
            .toPandas()

    st.subheader("Motif Finding (A → B → C)")
    via_nodes = motifs['via'].unique()
    if len(via_nodes) == 0:
        st.info("Nessun pattern A→B→C trovato.")
    else:
        selezionato = st.selectbox("Seleziona un aeroporto hub (via):", sorted(via_nodes))

        filtrati = motifs[motifs['via'] == selezionato]

        st.markdown(f"### Motif con nodo centrale **{selezionato}**")

        for idx, row in filtrati.iterrows():
            mini_net = Network(height="300px", width="100%", bgcolor="#111", font_color="white")
            mini_net.add_node(row['from'], label=row['from'], color="lightblue")
            mini_net.add_node(row['via'], label=row['via'], color="orange")
            mini_net.add_node(row['to'], label=row['to'], color="lightgreen")
            mini_net.add_edge(row['from'], row['via'])
            mini_net.add_edge(row['via'], row['to'])

            file_name = f"motif_{idx}.html"
            mini_net.save_graph(file_name)
            with open(file_name, 'r', encoding='utf-8') as f:
                components.html(f.read(), height=300)

    # Analisi della resilienza della rete
    st.subheader("Analisi della resilienza della rete")
    top_degree_node = degrees.sort_values("degree", ascending=False).iloc[0]['id']
    st.markdown(f"**Aeroporto con il massimo grado (più connesso): {top_degree_node}**")

    # Rimuovere l'aeroporto più connesso e ricalcolare la rete
    nodes_pdf = aeroporti.toPandas().merge(connected_components, on="id", how="left")
    g_resilienza = g.subgraph([row['id'] for idx, row in nodes_pdf.iterrows() if row['id'] != top_degree_node])

    st.markdown(f"**Componente connessa dopo la rimozione di {top_degree_node}:**")
    connected_components_resilienza = g_resilienza.connectedComponents().select("id", "component").toPandas()
    st.write(connected_components_resilienza)

    # Percorsi più brevi
    st.subheader("Percorsi più brevi tra aeroporti")
    aeroporto_iniziale = st.text_input("Aeroporto di partenza (es. JFK):", value="JFK")
    aeroporto_destinazione = st.text_input("Aeroporto di arrivo (es. LAX):", value="LAX")

    shortest_paths = g.shortestPaths(landmarks=[aeroporto_iniziale, aeroporto_destinazione])\
        .select("id", "distances").toPandas()

    st.write(shortest_paths)

    # Visualizzazione Pyvis con componenti
    st.subheader("Visualizzazione Interattiva del Grafo")
    edges_pdf = archi.toPandas()

    net = Network(height="600px", width="100%", bgcolor="#222222", font_color="white")
    color_map = px.colors.qualitative.Set3
    comp_colors = {comp: color_map[i % len(color_map)] for i, comp in enumerate(nodes_pdf['component'].unique())}

    for _, row in nodes_pdf.iterrows():
        color = comp_colors.get(row['component'], "#cccccc")
        net.add_node(row['id'], label=row['id'], color=color)

    for _, row in edges_pdf.iterrows():
        net.add_edge(row['src'], row['dst'])

    net.save_graph("graph.html")
    with open("graph.html", 'r', encoding='utf-8') as HtmlFile:
        components.html(HtmlFile.read(), height=600)

