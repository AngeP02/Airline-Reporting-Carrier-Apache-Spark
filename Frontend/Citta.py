import streamlit as st
from Backend.query_citta import (
    num_voli_partiti_citta, num_voli_arrivati_citta, num_voli_cancellati_citta,
    metriche_compagnie_citta,
    compagnie_attive_in_citta, voli_partiti_mensili_citta,
    voli_arrivati_mensili_citta, voli_totali_mensili_citta,
    cancellazioni_mensili_citta, lista_citta_uniche, num_aeroporti_citta
)
import pydeck as pdk
import plotly.express as px

from pyspark.sql.functions import col, count, collect_set
def mostra_citta(df, aeroporti_info):
    st.title("Analisi per Città")

    lista_citta = lista_citta_uniche(df)
    st.markdown(
        "<p style='font-size:24px; font-weight:bold; margin-bottom:0;'> Seleziona una città</p>",
        unsafe_allow_html=True
    )
    citta = st.selectbox(
        "",
        options=sorted(lista_citta)
    )
    if not citta:
        st.warning("Seleziona un aeroporto per iniziare.")
        return
    n_ae = num_aeroporti_citta(df, citta).collect()[0]["NumAeroportiCitta"]

    with st.container(border=True):
        st.subheader("Statistiche Generali")
        n_p = num_voli_partiti_citta(df, citta).collect()[0]["NumVoliPartiti"]
        n_a = num_voli_arrivati_citta(df, citta).collect()[0]["NumVoliArrivati"]
        n_c = num_voli_cancellati_citta(df, citta).collect()[0]["NumVoliCancellati"]

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Voli Partiti", f"{n_p:,}")
        col2.metric("Voli Arrivati", f"{n_a:,}")
        col3.metric("Voli Cancellati", f"{n_c:,}")
        col4.metric("Aeroporti in città", f"{n_ae:,}")


    # dentro al container principale per “Analisi per Città”:
    with st.container(border=True):
        st.subheader("Mappa aeroporti e rotte da città")

        # 1) selezione della città (assumo tu l'abbia già in `citta_selezionata`)
        # st.selectbox già presente sopra

        # 2) pesca gli aeroporti di partenza nella città
        aeroporti_citta = (
            df.filter(col("OriginCityName") == citta)
            .select("Origin").distinct()
            .rdd.flatMap(lambda x: x).collect()
        )

        if not aeroporti_citta:
            st.warning("Nessun aeroporto trovato per questa città.")
        else:
            # 3) se ne trovi più di uno, permetti il filtro
            if len(aeroporti_citta) > 1:
                aeroporti_selezionati = st.multiselect(
                    "Seleziona gli aeroporti di partenza:",
                    options=sorted(aeroporti_citta),
                    default=sorted(aeroporti_citta)
                )
            else:
                aeroporti_selezionati = aeroporti_citta

            # 4) prepara i dati geografici degli aeroporti
            aeroporti_info_citta = aeroporti_info[
                aeroporti_info["iata"].isin(aeroporti_selezionati)
            ]
            # scatter layer: punti aeroporti
            scatter_layer = pdk.Layer(
                "ScatterplotLayer",
                data=aeroporti_info_citta,
                get_position=["longitude", "latitude"],
                get_radius=5000,
                pickable=True,
                auto_highlight=True,
                get_fill_color=[0, 128, 255, 160]
            )

            # 5) aggrega tutte le tratte in partenza dagli aeroporti selezionati
            routes_spark = (
                df.filter(col("Origin").isin(aeroporti_selezionati))
                .groupBy("Origin", "Dest")
                .agg(
                    count("*").alias("flight_count"),
                    collect_set("Reporting_Airline").alias("airlines")
                )
            )

            # 6) arricchisci con lat/lon (usa il tuo DataFrame Pandas airports_pd → Spark)
            spark = df.sql_ctx.sparkSession
            airports_df = spark.createDataFrame(aeroporti_info)

            origin_info = airports_df.select(
                col("iata").alias("Origin"),
                col("longitude").alias("origin_lon"),
                col("latitude").alias("origin_lat")
            )
            dest_info = airports_df.select(
                col("iata").alias("Dest"),
                col("longitude").alias("dest_lon"),
                col("latitude").alias("dest_lat")
            )

            enriched = (
                routes_spark
                .join(origin_info, on="Origin", how="left")
                .join(dest_info, on="Dest", how="left")
                .dropna(subset=["origin_lat", "origin_lon", "dest_lat", "dest_lon"])
            )

            # 7) converti in Pandas e calcola la “width” per spessore archi
            routes_pd = enriched.toPandas()
            if not routes_pd.empty:
                routes_pd["width"] = routes_pd["flight_count"] / routes_pd["flight_count"].max() * 5

                arc_layer = pdk.Layer(
                    "ArcLayer",
                    data=routes_pd,
                    get_source_position=["origin_lon", "origin_lat"],
                    get_target_position=["dest_lon", "dest_lat"],
                    get_width="width",
                    get_source_color=[0, 128, 255, 80],
                    get_target_color=[255, 0, 0, 80],
                    pickable=True,
                    auto_highlight=True
                )

                # 8) view state calcolato sui punti di origine
                view_state = pdk.ViewState(
                    latitude=routes_pd["origin_lat"].mean(),
                    longitude=routes_pd["origin_lon"].mean(),
                    zoom=4,
                    pitch=0
                )

                r = pdk.Deck(
                    layers=[scatter_layer, arc_layer],
                    initial_view_state=view_state,
                    map_style="mapbox://styles/mapbox/light-v10",
                    tooltip={
                        "html": "<b>Aeroporto Origine:</b> {Origin}<br/>"
                                "<b>Destinazione:</b> {Dest}<br/>"
                                "<b>Voli:</b> {flight_count}<br/>"
                                "<b>Compagnie:</b> {airlines}",
                        "style": {"backgroundColor": "steelblue", "color": "white"}
                    }
                )
                st.pydeck_chart(r)
            else:
                st.info("Nessuna rotta in partenza dagli aeroporti selezionati.")

    with st.container(border=True):

        # Aeroporti: distribuzioni
        st.subheader("Distribuzione Partenze per Aeroporto")

        if n_ae > 1:
            col11, col12 = st.columns(2)
            aeroporti_selezionati = st.multiselect(
                "Seleziona aeroporti:",
                options=sorted(aeroporti_citta),
                default=sorted(aeroporti_citta)
            )
            with col11:
                # distribuzione partenze
                dep_df = (
                    df.filter(col("Origin").isin(aeroporti_selezionati))
                    .groupBy("Origin")
                    .agg(count("*").alias("num_flights"))
                    .toPandas()
                )
                fig_dep = px.bar(dep_df, x="Origin", y="num_flights",
                                 title="Voli Partiti per Aeroporto",
                                 text_auto=True)
                st.plotly_chart(fig_dep, use_container_width=True)
            with col12:
                # distribuzione arrivi
                arr_df = (
                    df.filter(col("Dest").isin(aeroporti_selezionati))
                    .groupBy("Dest")
                    .agg(count("*").alias("num_flights"))
                    .toPandas()
                )
                fig_arr = px.bar(arr_df, x="Dest", y="num_flights",
                                 title="Voli Arrivati per Aeroporto",
                                 text_auto=True)
                st.plotly_chart(fig_arr, use_container_width=True)

            # se c'è un solo aeroporto → metriche semplici
        elif n_ae == 1:
            a = aeroporti_citta[0]
            n_dep = df.filter(col("Origin") == a).count()
            n_arr = df.filter(col("Dest") == a).count()
            st.metric("Voli Partiti", f"{n_dep:,}")
            st.metric("Voli Arrivati", f"{n_arr:,}")

        else:
            st.info("Nessun aeroporto trovato per questa città.")

    with st.container(border=True):
        st.subheader("Compagnie Aeree")

        # Compagnie attive nella città
        active = compagnie_attive_in_citta(df, citta)

        if not active:
            st.info("Nessuna compagnia attiva trovata per questa città.")

        elif len(active) == 1:
            st.markdown(f"**Unica compagnia attiva in città:** `{active[0]}`")

            m = metriche_compagnie_citta(df, citta).toPandas()
            if not m.empty:
                row = m.iloc[0]
                st.metric("Voli totali", f"{row['NumVoli']:,}")
                st.metric("Ritardo medio", f"{row['RitardoMedio']:.1f} minuti")
                st.metric("Cancellazioni", f"{row['PercCancellazioni']:.1f}%")

        else:
            metrics = metriche_compagnie_citta(df, citta).toPandas()
            if not metrics.empty:
                m = metrics.copy()
                m['Norm_Ritardo'] = (m['RitardoMedio'].max() - m['RitardoMedio']) / \
                                    (m['RitardoMedio'].max() - m['RitardoMedio'].min())
                m['Norm_Cancellazioni'] = (m['PercCancellazioni'].max() - m['PercCancellazioni']) / \
                                          (m['PercCancellazioni'].max() - m['PercCancellazioni'].min())
                m['Norm_Voli'] = (m['NumVoli'] - m['NumVoli'].min()) / (m['NumVoli'].max() - m['NumVoli'].min())
                m['Score'] = m['Norm_Ritardo'] * 0.4 + m['Norm_Cancellazioni'] * 0.3 + m['Norm_Voli'] * 0.3

                if len(active) < 10:
                    # Grafico per tutte le compagnie
                    df_all = m.sort_values("Score", ascending=False)
                    fig_all = px.bar(
                        df_all,
                        x="IATA_CODE_Reporting_Airline",
                        y="Score",
                        text="Score",
                        labels={"IATA_CODE_Reporting_Airline": "Compagnia", "Score": "Punteggio"},
                        title=f"Tutte le compagnie ({len(active)}) ordinate per punteggio"
                    )
                    fig_all.update_traces(texttemplate="%{text:.2f}", textposition="outside")
                    fig_all.update_layout(xaxis_title="Compagnia", yaxis_title="Punteggio")
                    st.plotly_chart(fig_all, use_container_width=True)
                else:
                    # Mostra Top 5 migliori e peggiori
                    top5 = m.nlargest(5, 'Score')
                    worst5 = m.nsmallest(5, 'Score')

                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown("#### Top 5 Migliori")
                        fig1 = px.bar(top5, x='IATA_CODE_Reporting_Airline', y='Score', text='Score')
                        fig1.update_traces(texttemplate="%{text:.2f}", textposition="outside")
                        fig1.update_layout(xaxis_title="Compagnia", yaxis_title="Punteggio")
                        st.plotly_chart(fig1, use_container_width=True)
                    with col2:
                        st.markdown("#### Top 5 Peggiori")
                        fig2 = px.bar(worst5, x='IATA_CODE_Reporting_Airline', y='Score', text='Score')
                        fig2.update_traces(texttemplate="%{text:.2f}", textposition="outside")
                        fig2.update_layout(xaxis_title="Compagnia", yaxis_title="Punteggio")
                        st.plotly_chart(fig2, use_container_width=True)

            st.markdown("**Compagnie attive in città:**")
            st.markdown(
                "<div style='display: flex; flex-wrap: wrap; gap: 8px;'>"
                + "".join(
                    f"<span style='background-color: #e0f0ff; padding: 5px 10px; border-radius: 15px;"
                    f" border: 1px solid #ccc; font-size: 14px;'>{iata}</span>"
                    for iata in sorted(active)
                )
                + "\n</div>",
                unsafe_allow_html=True
            )

    with st.container(border=True):
        st.subheader("Analisi Mensile")

        tab1, tab2, tab3, tab4 = st.tabs(["Voli Partiti", "Voli Arrivati", "Voli Totali", "Cancellazioni"])

        # Voli partiti
        with tab1:
            vp = voli_partiti_mensili_citta(df, citta).toPandas()
            if not vp.empty:
                fig = px.line(
                    vp, x='Month', y='VoliPartiti', markers=True,
                    title="Andamento mensile dei voli partiti",
                    labels={'Month': 'Mese', 'VoliPartiti': 'Numero di voli'},
                    line_shape="spline",
                    color_discrete_sequence=["#1f77b4"]
                )
                fig.update_traces(mode="lines+markers", hovertemplate="Mese: %{x}<br>Voli: %{y}")
                st.plotly_chart(fig, use_container_width=True)

        # Voli arrivati
        with tab2:
            va = voli_arrivati_mensili_citta(df, citta).toPandas()
            if not va.empty:
                fig = px.line(
                    va, x='Month', y='VoliArrivati', markers=True,
                    title="Andamento mensile dei voli arrivati",
                    labels={'Month': 'Mese', 'VoliArrivati': 'Numero di voli'},
                    line_shape="spline",
                    color_discrete_sequence=["#2ca02c"]
                )
                fig.update_traces(mode="lines+markers", hovertemplate="Mese: %{x}<br>Voli: %{y}")
                st.plotly_chart(fig, use_container_width=True)

        # Voli totali
        with tab3:
            vt = voli_totali_mensili_citta(df, citta).toPandas()
            if not vt.empty:
                fig = px.line(
                    vt, x='Month', y='VoliTotali', markers=True,
                    title="Andamento mensile dei voli totali",
                    labels={'Month': 'Mese', 'VoliTotali': 'Numero di voli'},
                    line_shape="spline",
                    color_discrete_sequence=["#9467bd"]
                )
                fig.update_traces(mode="lines+markers", hovertemplate="Mese: %{x}<br>Voli: %{y}")
                st.plotly_chart(fig, use_container_width=True)

        # Cancellazioni
        with tab4:
            vc = cancellazioni_mensili_citta(df, citta).toPandas()
            if not vc.empty:
                fig = px.line(
                    vc, x='Month', y='VoliCancellati', markers=True,
                    title="Andamento mensile delle cancellazioni",
                    labels={'Month': 'Mese', 'VoliCancellati': 'Numero di cancellazioni'},
                    line_shape="spline",
                    color_discrete_sequence=["#d62728"]
                )
                fig.update_traces(mode="lines+markers", hovertemplate="Mese: %{x}<br>Cancellazioni: %{y}")
                st.plotly_chart(fig, use_container_width=True)
