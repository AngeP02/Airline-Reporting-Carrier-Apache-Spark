import streamlit as st
from Backend.query_compagnie_aeree import (
    lista_compagnie_uniche,
    statistiche_generali_compagnia,
    classifiche_compagnie,
    top5_aeroporti_partenza_compagnia,
    top5_citta_servite_compagnia,
    ranking_mensile_performance_compagnia,
    distribuzione_voli_per_aeroporto_compagnia,
    distribuzione_voli_per_citta_compagnia
)
import plotly.express as px
import pandas as pd

def mostra_compagnia(df, aeroporti_info):
    st.title("Analisi per Compagnia Aerea")

    # Selezione compagnia
    compagnie = lista_compagnie_uniche(df)
    st.markdown(
        "<p style='font-size:24px; font-weight:bold; margin-bottom:0;'> Seleziona una compagnia aerea</p>",
        unsafe_allow_html=True
    )
    compagnia_selezionata = st.selectbox("", options=sorted(compagnie))
    if not compagnia_selezionata:
        st.warning("Seleziona una compagnia per iniziare.")
        return

    # Statistiche generali
        # Statistiche generali e classifica
    with st.container(border=True):
        col1, col2 = st.columns(2)
        # Statistiche Generali
        with col1:
            st.subheader("Statistiche Generali")
            stats = statistiche_generali_compagnia(df, compagnia_selezionata)
            col11, col12 = st.columns(2)
            col11.metric("Codice IATA", f"{stats['iata_code']}")
            col12.metric("Numero di Voli Totali", f"{stats['num_voli']:,}")
        # Classifica altre compagnie
        with col2:
            st.subheader("Classifica tra tutte le compagnie")
            classifica_df = classifiche_compagnie(df).toPandas()
            total_compagnie = len(classifica_df)
            selected = classifica_df[classifica_df['Carrier'] == compagnia_selezionata].iloc[0]
            col21,col22, col23 = st.columns(3)
            col21.metric("Posizione per Ritardi",f"{int(selected['rank_ritardi'])} su {total_compagnie}")
            col22.metric("Posizione per Cancellazioni",f"{int(selected['rank_cancellazioni'])} su {total_compagnie}")
            col23.metric("Posizione per Voli Effettuati", f"{int(selected['rank_voli'])} su {total_compagnie}")

    # Top 5 Aeroporti di Partenza
    with st.container(border=True):
        st.subheader("Top 5 Aeroporti di Partenza")
        top_airports = top5_aeroporti_partenza_compagnia(df, compagnia_selezionata).toPandas()
        if not top_airports.empty:
            fig_air = px.bar(
                top_airports, x='Origin', y='flight_count', text='flight_count',
                labels={'flight_count': 'Numero di Voli', 'Origin': 'Aeroporto'}, title=""
            )
            fig_air.update_traces(texttemplate='%{text}', textposition='outside')
            st.plotly_chart(fig_air, use_container_width=True)
        else:
            st.info("Nessun dato aeroporti di partenza per questa compagnia.")

    # Top 5 Città Servite
    with st.container(border=True):
        st.subheader("Top 5 Città Servite")
        top_cities = top5_citta_servite_compagnia(df, compagnia_selezionata).toPandas()
        if not top_cities.empty:
            fig_city = px.bar(
                top_cities, x='City', y='flight_count', text='flight_count',
                labels={'flight_count': 'Numero di Voli', 'City': 'Città'}, title=""
            )
            fig_city.update_traces(texttemplate='%{text}', textposition='outside')
            st.plotly_chart(fig_city, use_container_width=True)
        else:
            st.info("Nessun dato città servite per questa compagnia.")

    # Statistiche avanzate
    with st.container(border=True):
        st.subheader("Ranking Mensile delle performance")
        st.markdown(
            "<p style='font-size:20px; margin-bottom:0;'> Valutato sulla base dei ritardi, delle cancellazioni e della puntualità</p>",
            unsafe_allow_html=True
        )
        # Ranking mensile basato su ritardo, cancellazioni e puntualità
        monthly_rank = ranking_mensile_performance_compagnia(df, compagnia_selezionata).toPandas()
        if not monthly_rank.empty:
            monthly_rank['Month'] = monthly_rank['Month'].astype(int)
            fig_rank = px.line(
                monthly_rank, x='Month', y='Score', markers=True, text='Score',
                labels={'Month': 'Mese', 'Score': 'Performance Score'}, title=""
            )
            fig_rank.update_traces(texttemplate='%{text:.2f}', textposition='top center')
            st.plotly_chart(fig_rank, use_container_width=True)
        else:
            st.info("Nessun dato performance mensili disponibili.")

        # Statistiche avanzate: distribuzioni
    with st.container(border=True):
        st.subheader("Distribuzioni Voli")
        col1, col2 = st.columns(2)

        # Distribuzione voli per Aeroporto (con grouping <1%)
        with col1:
            st.markdown("#### Per Aeroporto")
            dist_air_df = distribuzione_voli_per_aeroporto_compagnia(df, compagnia_selezionata).toPandas()
            if not dist_air_df.empty:
                total = dist_air_df['flight_count'].sum()
                dist_air_df['pct'] = dist_air_df['flight_count'] / total * 100
                large = dist_air_df[dist_air_df['pct'] >= 1].copy()
                small = dist_air_df[dist_air_df['pct'] < 1]
                if not small.empty:
                    other_row = pd.DataFrame([{
                        'Origin': 'Altri (<1%)',
                        'flight_count': small['flight_count'].sum(),
                        'pct': small['pct'].sum()
                    }])
                    large = pd.concat([large, other_row], ignore_index=True)
                fig_dist_air = px.pie(
                    large, names='Origin', values='flight_count',
                    title="Volumi di Traffico per Aeroporto"
                )
                st.plotly_chart(fig_dist_air, use_container_width=True)
            else:
                st.info("Nessun dato per distribuzione aeroporti.")

        # Distribuzione voli per Città (con grouping <1%)
        with col2:
            st.markdown("#### Per Città")
            dist_city_df = distribuzione_voli_per_citta_compagnia(df, compagnia_selezionata).toPandas()
            if not dist_city_df.empty:
                total_c = dist_city_df['flight_count'].sum()
                dist_city_df['pct'] = dist_city_df['flight_count'] / total_c * 100
                large_c = dist_city_df[dist_city_df['pct'] >= 1].copy()
                small_c = dist_city_df[dist_city_df['pct'] < 1]
                if not small_c.empty:
                    other_row_c = pd.DataFrame([{
                        'City': 'Altre (<1%)',
                        'flight_count': small_c['flight_count'].sum(),
                        'pct': small_c['pct'].sum()
                    }])
                    large_c = pd.concat([large_c, other_row_c], ignore_index=True)
                fig_dist_city = px.pie(
                    large_c, names='City', values='flight_count',
                    title="Volumi di Traffico per Città"
                )
                st.plotly_chart(fig_dist_city, use_container_width=True)
            else:
                st.info("Nessun dato per distribuzione città.")
