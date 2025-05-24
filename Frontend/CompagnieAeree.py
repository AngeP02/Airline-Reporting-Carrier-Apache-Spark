import streamlit as st
from Backend.query_compagnie_aeree import (
    lista_compagnie_uniche,
    statistiche_generali_compagnia,
    classifiche_compagnie,
    top5_aeroporti_partenza_compagnia,
    top5_citta_servite_compagnia,
    ranking_mensile_performance_compagnia,
    distribuzione_voli_per_aeroporto_compagnia,
    distribuzione_voli_per_citta_compagnia, statistiche_compagnia_aerea
)
import plotly.express as px
import pandas as pd
from pyspark.sql import Row

def mostra_compagnia(df, aeroporti_info):
    st.title("Analisi per Compagnia Aerea")

    # Selezione compagnia
    compagnie = lista_compagnie_uniche(df)
    st.markdown(
        "<p style='font-size:20px; margin-bottom:0;'> Seleziona una compagnia aerea</p>",
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
            col11, col12, col13 = st.columns(3)
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
            "<p style='font-size:16px; margin-bottom:0;'> Valutato sulla base dei ritardi, delle cancellazioni e della puntualità</p>",
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
            fig_rank.update_layout(xaxis=dict(tickmode='linear'))
            st.plotly_chart(fig_rank, use_container_width=True)
        else:
            st.info("Nessun dato performance mensili disponibili.")

        # Statistiche avanzate: distribuzioni
    with st.container(border=True):
        st.subheader("Distribuzioni Voli")
        col1, col2 = st.columns(2)

        # Distribuzione voli per Aeroporto (con grouping <1%)
        with col1:
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
    with st.container(border=True):

        st.subheader("Confronto compagnie aeree")
        compagnie_df = df.select("IATA_CODE_Reporting_Airline").distinct()
        compagnie_list = sorted(
            [compagnia['IATA_CODE_Reporting_Airline'] for compagnia in compagnie_df.collect()])
        compagnie_selezionate = st.multiselect("Seleziona una o più compagnie aeree", compagnie_list)

        if compagnie_selezionate:
            if len(compagnie_selezionate) < 2:
                modalita = "Media"  # Se una sola compagnia è selezionata, seleziona automaticamente "Media"
            else:
                options = ["Media", "Confronto"]
                modalita = st.pills("Seleziona modalità", options)

            # Calcolare le statistiche delle compagnie aeree selezionate
            stats_list = [statistiche_compagnia_aerea(compagnia, df) for compagnia in compagnie_selezionate]

            # Creazione del DataFrame PySpark per il confronto
            rows = []
            for stat in stats_list:
                rows.append(Row(Compagnia_Aerea=stat["Compagnia Aerea"],
                                Totale_Voli=stat["Totale Voli"],
                                Ritardo_Medio_Partenza=stat["Ritardo Medio Partenza"],
                                Ritardo_Medio_Arrivo=stat["Ritardo Medio Arrivo"],
                                Voli_Cancellati=stat["Voli Cancellati"],
                                Voli_Dirottati=stat["Voli Dirottati"]))

            stats_spark_df = df.sparkSession.createDataFrame(rows)

            if modalita == "Media":
                # Calcolo delle statistiche medie
                media_stats = stats_spark_df.agg(
                    {'Totale_Voli': 'avg', 'Ritardo_Medio_Partenza': 'avg', 'Ritardo_Medio_Arrivo': 'avg',
                     'Voli_Cancellati': 'avg', 'Voli_Dirottati': 'avg'}).collect()[0]

                col1,col2,col3 = st.columns(3)

                with col1:
                    fig_voli_medio = px.bar(
                        x=['Media'],
                        y=[media_stats['avg(Totale_Voli)']],
                        title="Media Totale Voli",
                        text=[f"{media_stats['avg(Totale_Voli)']:.1f}"]
                    )
                    fig_voli_medio.update_traces(textposition='outside')
                    st.plotly_chart(fig_voli_medio)

                with col2:
                    fig_ritardi_medi = px.bar(
                        x=['Ritardo Medio Partenza', 'Ritardo Medio Arrivo'],
                        y=[media_stats['avg(Ritardo_Medio_Partenza)'], media_stats['avg(Ritardo_Medio_Arrivo)']],
                        title="Media Ritardi (Partenza e Arrivo)",
                        text=[f"{media_stats['avg(Ritardo_Medio_Partenza)']:.1f}",
                              f"{media_stats['avg(Ritardo_Medio_Arrivo)']:.1f}"]
                    )
                    fig_ritardi_medi.update_traces(textposition='outside')
                    st.plotly_chart(fig_ritardi_medi)

                with col3:
                    fig_altre_medie = px.bar(
                        x=['Media Voli Cancellati', 'Media Voli Dirottati'],
                        y=[media_stats['avg(Voli_Cancellati)'], media_stats['avg(Voli_Dirottati)']],
                        title="Media Voli Cancellati e Dirottati",
                        text=[f"{media_stats['avg(Voli_Cancellati)']:.1f}", f"{media_stats['avg(Voli_Dirottati)']:.1f}"]
                    )
                    fig_altre_medie.update_traces(textposition='outside')
                    st.plotly_chart(fig_altre_medie)
            else:
                # Conversione del DataFrame Spark in Pandas
                stats_pandas_df = stats_spark_df.toPandas()

                # Raggruppiamo le metriche che vuoi visualizzare insieme
                voli_metrics = ["Totale_Voli"]
                ritardi_metrics = ["Ritardo_Medio_Partenza", "Ritardo_Medio_Arrivo"]
                altre_metrics = ["Voli_Cancellati", "Voli_Dirottati"]

                col1,col2,col3 = st.columns(3)
                with col1:
                    voli_long_df = pd.melt(stats_pandas_df, id_vars=["Compagnia_Aerea"], value_vars=voli_metrics,
                                           var_name="Metrica", value_name="Valore")
                    fig_voli = px.bar(voli_long_df, x="Metrica", y="Valore", color="Compagnia_Aerea",
                                      barmode="group", title="Totale Voli per Compagnia", text="Valore")
                    fig_voli.update_traces(textposition='outside')
                    fig_voli.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
                    st.plotly_chart(fig_voli)

                with col2:
                    ritardi_long_df = pd.melt(stats_pandas_df, id_vars=["Compagnia_Aerea"], value_vars=ritardi_metrics,
                                              var_name="Metrica", value_name="Valore")
                    fig_ritardi = px.bar(ritardi_long_df, x="Metrica", y="Valore", color="Compagnia_Aerea",
                                         barmode="group", title="Ritardo Medio (Partenza e Arrivo) per Compagnia",
                                         text="Valore")
                    fig_ritardi.update_traces(textposition='outside')
                    fig_ritardi.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
                    st.plotly_chart(fig_ritardi)

                with col3:
                    altre_long_df = pd.melt(stats_pandas_df, id_vars=["Compagnia_Aerea"], value_vars=altre_metrics,
                                            var_name="Metrica", value_name="Valore")
                    fig_altre = px.bar(altre_long_df, x="Metrica", y="Valore", color="Compagnia_Aerea",
                                       barmode="group", title="Voli Cancellati e Dirottati per Compagnia", text="Valore")
                    fig_altre.update_traces(textposition='outside')
                    fig_altre.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
                    st.plotly_chart(fig_altre)