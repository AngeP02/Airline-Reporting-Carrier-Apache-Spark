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
        "<p style='font-size:20px; margin-bottom:0;'> Seleziona una città</p>",
        unsafe_allow_html=True
    )
    citta = st.selectbox(
        "",
        options=sorted(lista_citta)
    )
    if not citta:
        st.warning("Seleziona un aeroporto per iniziare.")
        return
    num_aer_citta = num_aeroporti_citta(df, citta).collect()[0]["NumAeroportiCitta"]

    with st.container(border=True):
        st.subheader("Statistiche Generali")
        num_voli_partiti = num_voli_partiti_citta(df, citta).collect()[0]["NumVoliPartiti"]
        num_voli_arrivati = num_voli_arrivati_citta(df, citta).collect()[0]["NumVoliArrivati"]
        num_voli_cancellati = num_voli_cancellati_citta(df, citta).collect()[0]["NumVoliCancellati"]

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Voli Partiti", f"{num_voli_partiti:,}")
        col2.metric("Voli Arrivati", f"{num_voli_arrivati:,}")
        col3.metric("Voli Cancellati", f"{num_voli_cancellati:,}")
        col4.metric("Aeroporti in città", f"{num_aer_citta:,}")

    with st.container(border=True):
        st.subheader("Mappa aeroporti e rotte da città")
        aeroporti_citta = (
            df.filter(col("OriginCityName") == citta)
            .select("Origin").distinct()
            .rdd.flatMap(lambda x: x).collect()
        )
        if not aeroporti_citta:
            st.warning("Nessun aeroporto trovato per questa città.")
        else:
            if len(aeroporti_citta) > 1:
                aeroporti_selezionati = st.multiselect(
                    "Seleziona gli aeroporti di partenza:",
                    options=sorted(aeroporti_citta),
                    default=sorted(aeroporti_citta)
                )
            else:
                aeroporti_selezionati = aeroporti_citta
            aeroporti_info_citta = aeroporti_info[
                aeroporti_info["iata"].isin(aeroporti_selezionati)
            ]
            scatter_layer = pdk.Layer(
                "ScatterplotLayer",
                data=aeroporti_info_citta,
                get_position=["longitude", "latitude"],
                get_radius=5000,
                pickable=True,
                auto_highlight=True,
                get_fill_color=[0, 128, 255, 160]
            )
            routes_spark = (
                df.filter(col("Origin").isin(aeroporti_selezionati))
                .groupBy("Origin", "Dest")
                .agg(
                    count("*").alias("flight_count"),
                    collect_set("Reporting_Airline").alias("airlines")
                )
            )
            spark = df.sql_ctx.sparkSession
            aeroporti_df = spark.createDataFrame(aeroporti_info)
            origine_info = aeroporti_df.select(
                col("iata").alias("Origin"),
                col("longitude").alias("origin_lon"),
                col("latitude").alias("origin_lat")
            )
            dest_info = aeroporti_df.select(
                col("iata").alias("Dest"),
                col("longitude").alias("dest_lon"),
                col("latitude").alias("dest_lat")
            )
            aggiunta = (
                routes_spark
                .join(origine_info, on="Origin", how="left")
                .join(dest_info, on="Dest", how="left")
                .dropna(subset=["origin_lat", "origin_lon", "dest_lat", "dest_lon"])
            )
            rotte_pd = aggiunta.toPandas()
            if not rotte_pd.empty:
                rotte_pd["width"] = rotte_pd["flight_count"] / rotte_pd["flight_count"].max() * 5
                arc_layer = pdk.Layer(
                    "ArcLayer",
                    data=rotte_pd,
                    get_source_position=["origin_lon", "origin_lat"],
                    get_target_position=["dest_lon", "dest_lat"],
                    get_width="width",
                    get_source_color=[0, 128, 255, 80],
                    get_target_color=[255, 0, 0, 80],
                    pickable=True,
                    auto_highlight=True
                )
                view_state = pdk.ViewState(
                    latitude=rotte_pd["origin_lat"].mean(),
                    longitude=rotte_pd["origin_lon"].mean(),
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
        st.subheader("Distribuzione Partenze per Aeroporto")
        if num_aer_citta > 1:
            col11, col12 = st.columns(2)
            with col11:
                part_df = (
                    df.filter(col("Origin").isin(aeroporti_selezionati))
                    .groupBy("Origin")
                    .agg(count("*").alias("Numero_voli"))
                    .toPandas()
                )
                fig_dep = px.bar(part_df, x="Origin", y="Numero_voli",
                                 title="Voli Partiti per Aeroporto",
                                 text_auto=True)
                fig_dep.update_layout(xaxis_title="Aeroporto di Partenza", yaxis_title="Numero di Voli")
                st.plotly_chart(fig_dep, use_container_width=True)
            with col12:
                arr_df = (
                    df.filter(col("Dest").isin(aeroporti_selezionati))
                    .groupBy("Dest")
                    .agg(count("*").alias("Numero_voli"))
                    .toPandas()
                )
                fig_arr = px.bar(arr_df, x="Dest", y="Numero_voli",
                                 title="Voli Arrivati per Aeroporto",
                                 text_auto=True)
                fig_arr.update_layout(xaxis_title="Aeroporto di Destinazione", yaxis_title="Numero di Voli")
                st.plotly_chart(fig_arr, use_container_width=True)

        elif num_aer_citta == 1:
            st.markdown("E' presente un solo aeroporto in città:")
            a = aeroporti_citta[0]
            num_part = df.filter(col("Origin") == a).count()
            n_arr = df.filter(col("Dest") == a).count()
            col1, col2 = st.columns(2)
            col1.metric("Voli Partiti", f"{num_part:,}")
            col2.metric("Voli Arrivati", f"{n_arr:,}")
        else:
            st.info("Nessun aeroporto trovato per questa città.")

    with st.container(border=True):
        st.subheader("Compagnie Aeree")
        attive = compagnie_attive_in_citta(df, citta)
        if not attive:
            st.info("Nessuna compagnia attiva trovata per questa città.")
        elif len(attive) == 1:
            st.markdown(f"**Unica compagnia attiva in città:** `{attive[0]}`")
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

                if len(attive) < 10:
                    df_all = m.sort_values("Score", ascending=False)
                    fig_all = px.bar(
                        df_all,
                        x="IATA_CODE_Reporting_Airline",
                        y="Score",
                        text="Score",
                        labels={"IATA_CODE_Reporting_Airline": "Compagnia", "Score": "Punteggio"},
                        title=f"Tutte le compagnie ({len(attive)}) ordinate per punteggio"
                    )
                    fig_all.update_traces(texttemplate="%{text:.2f}", textposition="outside")
                    fig_all.update_layout(xaxis_title="Compagnia", yaxis_title="Punteggio")
                    st.plotly_chart(fig_all, use_container_width=True)
                else:
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
            attive_sorted = sorted(attive)
            num_colonne = 15
            cols = st.columns(num_colonne)
            for i, iata in enumerate(attive_sorted):
                with cols[i % num_colonne]:
                    st.info(iata)

    with st.container(border=True):
        st.subheader("Analisi Mensile")
        tab1, tab2, tab3, tab4 = st.tabs(["Voli Partiti", "Voli Arrivati", "Voli Totali", "Cancellazioni"])
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
                fig.update_layout(xaxis=dict(tickmode='linear'))
                st.plotly_chart(fig, use_container_width=True)
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
                fig.update_layout(xaxis=dict(tickmode='linear'))
                st.plotly_chart(fig, use_container_width=True)
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
                fig.update_layout(xaxis=dict(tickmode='linear'))
                st.plotly_chart(fig, use_container_width=True)
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
                fig.update_layout(xaxis=dict(tickmode='linear'))
                st.plotly_chart(fig, use_container_width=True)