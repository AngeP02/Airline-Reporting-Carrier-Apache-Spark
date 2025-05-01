import streamlit as st
from streamlit_option_menu import option_menu

import sys
import os
import plotly.express as px
from pyspark import StorageLevel
from pyspark.sql import Row, SparkSession
import plotly.graph_objects as go
import pandas as pd
import pydeck as pdk

import altair as alt
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType



os.environ["PYSPARK_PYTHON"] = "python"
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Backend.query import statistiche_aeroporto, \
    calcola_kpi, elenco_tutti_aeroporti, aggregazione_voli_per_mese, \
    calcola_puntualita, display_routes_map, analisi_completa_ritardi, indicatori_di_performance, mappa_rotte, \
    get_lat_lon, elenco_tutte_compagnie, statistiche_compagnia_aerea, calcola_voli_per_giorno, crea_heatmap_calendario, \
    percentuali_ritardi, gruppi_ritardo, confronto_ritardi, distribuzione_ritardi, heatmap_ritardi, \
    ritardi_medi_per_mese, ritardi_medi_per_compagnia
from Frontend.Aeroporto import mostra_aeroporto
from Frontend.CompagnieAeree import mostra_compagnia
file_paths = [
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Preprocessing/Puliti/On_Time_Reporting_Carrier_1_clean.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Preprocessing/Puliti/On_Time_Reporting_Carrier_2_clean.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Preprocessing/Puliti/On_Time_Reporting_Carrier_3_clean.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Preprocessing/Puliti/On_Time_Reporting_Carrier_4_clean.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Preprocessing/Puliti/On_Time_Reporting_Carrier_5_clean.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Preprocessing/Puliti/On_Time_Reporting_Carrier_6_clean.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Preprocessing/Puliti/On_Time_Reporting_Carrier_7_clean.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Preprocessing/Puliti/On_Time_Reporting_Carrier_8_clean.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Preprocessing/Puliti/On_Time_Reporting_Carrier_9_clean.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Preprocessing/Puliti/On_Time_Reporting_Carrier_10_clean.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Preprocessing/Puliti/On_Time_Reporting_Carrier_11_clean.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Preprocessing/Puliti/On_Time_Reporting_Carrier_12_clean.csv",

]
aeroporti_info = pd.read_csv("C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/distutils/usa-airports.csv")

def get_spark_session(app_name="ProgettoVoli"):
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "16g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

spark = get_spark_session()


df = None
for file in file_paths:
    temp_df = spark.read.csv(file, header=True, inferSchema=True)
    df = temp_df if df is None else df.union(temp_df)
df.persist(StorageLevel.MEMORY_AND_DISK)


def main():
    st.set_page_config(page_title="Analisi Voli", layout="wide")

    # Definizione del menu orizzontale full-width
    menu_items = [
        "Home",
        "Ritardi e puntualità",
        "Aeroporto",
        "Compagnie aeree",
        "Condizioni operative",
        "Informazioni tecniche"
    ]
    menu_icons = ["house", "clock-history", "x-circle", "airplane", "cloud-sun", "gear"]
    scelta = option_menu(
        menu_title=None,
        options=menu_items,
        icons=menu_icons,
        default_index=0,
        orientation="horizontal",
        styles={
            "container": {"padding": "0px", "background-color": "#f0f2f6"},
            "nav-link": {
                "font-size": "16px",
                "text-align": "center",
                "margin": "0px",
                "padding": "10px 0px",
                "flex": "1"
            },
            "nav-link-selected": {
                "background-color": "#0275d8",
                "color": "white",
                "font-weight": "bold"
            }
        }
    )

    if scelta == "Home":
        st.title("Dashboard Analisi Voli")
        st.write("Statistiche generali dei dati")

        with st.container(border=True):
            kpi = calcola_kpi(df)
            st.subheader("Indicatori chiave")
            col1, col2, col3, col4, col5, col6, col7 = st.columns(7)
            col1.metric("Totale Voli", kpi["totale_voli"])
            col2.metric("Voli Cancellati", kpi["voli_cancellati"])
            col3.metric("Voli Deviati", kpi["voli_deviati"])
            col4.metric("Ritardo Medio Partenza", f"{kpi['ritardo_medio_partenza']:.2f} min")
            col5.metric("Ritardo Medio Arrivo", f"{kpi['ritardo_medio_arrivo']:.2f} min")
            col6.metric("Distanza Media", f"{kpi['distanza_media']:.2f} miglia")
            col7.metric("Percentuale voli cancellati", f"{kpi['percentuale_cancellati']:.2f} %")

        with st.container(border=True):
            st.subheader("Calendario Heatmap - Voli per Giorno")

            compagnie_df = df.select("IATA_CODE_Reporting_Airline").distinct()
            compagnie_list = sorted([row["IATA_CODE_Reporting_Airline"] for row in compagnie_df.collect()])
            compagnie_selezionate = st.multiselect("Filtra per compagnia aerea", compagnie_list)

            voli_per_giorno_df = calcola_voli_per_giorno(df, compagnie_selezionate)
            voli_pd = voli_per_giorno_df.toPandas()

            if not voli_pd.empty:
                fig = crea_heatmap_calendario(voli_pd)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Nessun dato disponibile per le compagnie selezionate.")

        with st.container(border=True):
            col1, col2 = st.columns(2)  # 2 colonne affiancate
            with col1:
                st.subheader("Andamento dei Voli per Mese")
                agg_df = aggregazione_voli_per_mese(df)
                agg_df_pandas = agg_df.toPandas()
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=agg_df_pandas['Month'],
                    y=agg_df_pandas['NumeroVoli'],
                    name='Numero di voli per mese',
                    marker=dict(color='lightblue')
                ))
                fig.update_layout(
                    title='Numero di voli per mese',
                    xaxis_title='Mese',
                    yaxis_title='Numero di voli',
                    template='plotly_dark',
                    showlegend=False
                )
                st.plotly_chart(fig)

            with col2:
                st.subheader("Puntualità vs. Ritardi dei Voli")
                puntualita_df = calcola_puntualita(df)
                puntualita_pd = puntualita_df.toPandas()
                fig = go.Figure(data=[go.Pie(
                    labels=puntualita_pd["Status"],
                    values=puntualita_pd["Percentage"],
                    hole=0.3,
                    marker=dict(colors=["#00cc96", "#f44336", "#ff9800"])  # Colori personalizzati
                )])
                fig.update_layout(
                    title="Puntualità vs. Ritardi dei Voli",
                    showlegend=True
                )
                st.plotly_chart(fig)

        with st.container(border=True):

            st.subheader("Seleziona Compagnia/e Aeree")
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

                    st.subheader("Statistiche Medie Compagnie Aeree")

                    # Visualizzazione grafica con Plotly
                    fig = px.bar(
                        x=['Totale_Voli', 'Ritardo_Medio_Partenza', 'Ritardo_Medio_Arrivo', 'Voli_Cancellati',
                           'Voli_Dirottati'],
                        y=[media_stats['avg(Totale_Voli)'], media_stats['avg(Ritardo_Medio_Partenza)'],
                           media_stats['avg(Ritardo_Medio_Arrivo)'], media_stats['avg(Voli_Cancellati)'],
                           media_stats['avg(Voli_Dirottati)']],
                        title="Statistiche Medie Compagnie Aeree",
                        text=[media_stats['avg(Totale_Voli)'], media_stats['avg(Ritardo_Medio_Partenza)'],
                              media_stats['avg(Ritardo_Medio_Arrivo)'], media_stats['avg(Voli_Cancellati)'],
                              media_stats['avg(Voli_Dirottati)']]
                    )
                    fig.update_traces(textposition='outside')
                    fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')

                    st.plotly_chart(fig)

                else:
                    st.subheader("Confronto Compagnie Aeree")
                    # Conversione del DataFrame Spark in Pandas
                    stats_pandas_df = stats_spark_df.toPandas()

                    # Creazione di un DataFrame "lungo" per Plotly
                    metrics = ["Totale_Voli", "Ritardo_Medio_Partenza", "Ritardo_Medio_Arrivo", "Voli_Cancellati",
                               "Voli_Dirottati"]

                    stats_long_df = pd.melt(stats_pandas_df, id_vars=["Compagnia_Aerea"], value_vars=metrics,
                                            var_name="Metrica", value_name="Valore")

                    # Creazione del grafico a barre raggruppate per ogni metrica
                    fig = px.bar(stats_long_df, x="Metrica", y="Valore", color="Compagnia_Aerea",
                                 barmode="group", title="Confronto Statistiche Compagnie Aeree",
                                 text="Valore")
                    fig.update_traces(textposition='outside')
                    fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')

                    # Mostra il grafico
                    st.plotly_chart(fig)


        with st.container(border=True):
            st.subheader("Seleziona Aeroporto/i")
            compagnie_df = elenco_tutti_aeroporti(df)
            compagnie_list = [aeroporto['Origin'] for aeroporto in compagnie_df.collect()]
            compagnie_list = sorted(compagnie_list)
            compagnie_selezionate = st.multiselect("Seleziona uno o più aeroporti", compagnie_list)

            if compagnie_selezionate :
                if len(compagnie_selezionate) < 2:
                    modalita = "Media"  # Se un solo aeroporto è selezionato, seleziona automaticamente "Media"
                else:
                    options = ["Media", "Confronto"]
                    modalita = st.pills("Seleziona modalità", options)

                # Calcolare le statistiche degli aeroporti selezionati
                stats_list = [statistiche_aeroporto(aeroporto, df) for aeroporto in compagnie_selezionate]

                # Creazione del DataFrame PySpark per il confronto
                rows = []
                for stat in stats_list:
                    rows.append(Row(Aeroporto=stat["Aeroporto"],
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

                    st.subheader("Statistiche Medie")

                    # Visualizzazione grafica con Plotly
                    fig = px.bar(
                        x=['Totale_Voli', 'Ritardo_Medio_Partenza', 'Ritardo_Medio_Arrivo', 'Voli_Cancellati',
                           'Voli_Dirottati'],
                        y=[media_stats['avg(Totale_Voli)'], media_stats['avg(Ritardo_Medio_Partenza)'],
                           media_stats['avg(Ritardo_Medio_Arrivo)'], media_stats['avg(Voli_Cancellati)'],
                           media_stats['avg(Voli_Dirottati)']],
                        title="Statistiche Medie Aeroporti"
                    )
                    st.plotly_chart(fig)

                else:
                    st.subheader("Confronto Aeroporti")
                    # Conversione del DataFrame Spark in Pandas
                    stats_spark_df = stats_spark_df.toPandas()

                    # Creazione di un DataFrame che raggruppa le statistiche per ogni metrica
                    metrics = ["Totale_Voli", "Ritardo_Medio_Partenza", "Ritardo_Medio_Arrivo", "Voli_Cancellati",
                               "Voli_Dirottati"]

                    # Creazione di un DataFrame "lungo" per Plotly
                    stats_long_df = pd.melt(stats_spark_df, id_vars=["Aeroporto"], value_vars=metrics,
                                            var_name="Metrica", value_name="Valore")

                    # Creazione del grafico a barre raggruppate per ogni metrica
                    fig = px.bar(stats_long_df, x="Metrica", y="Valore", color="Aeroporto",
                                 barmode="group", title="Confronto Statistiche Aeroporti")

                    # Mostra il grafico
                    st.plotly_chart(fig)

        with st.container(border=True):

            with st.container(border=True):
                st.subheader("Mappa interattiva delle rotte")

                # Calcolo dei dati delle rotte
                route_data = mappa_rotte(df, aeroporti_info)  # Questa funzione te la creo qui sotto
                route_pd = route_data.toPandas()
                if not route_pd.empty:

                    # Estrai l'elenco di tutti gli aeroporti
                    all_airports = sorted(set(route_pd['Origin']).union(set(route_pd['Dest'])))

                    # Filtro aeroporto
                    selected_airport = st.selectbox(
                        "Seleziona un aeroporto",
                        options=["Tutti"] + all_airports,
                        index=0
                    )

                    # Filtro minimo voli
                    min_flights = st.slider(
                        "Filtra per numero minimo di voli",
                        min_value=1,
                        max_value=int(route_pd['flight_count'].max()),
                        value=10
                    )

                    if selected_airport != "Tutti":
                        route_pd = route_pd[
                            (route_pd['Origin'] == selected_airport) |
                            (route_pd['Dest'] == selected_airport)
                            ]

                    route_pd = route_pd[route_pd['flight_count'] >= min_flights]

                    if route_pd.empty:
                        st.warning("Nessuna rotta soddisfa i criteri selezionati.")
                    else:
                        # Normalizzazione larghezza linee
                        route_pd['width'] = route_pd['flight_count'] / route_pd['flight_count'].max() * 10

                        # Creazione mappa pydeck
                        routes_layer = pdk.Layer(
                            "ArcLayer",
                            data=route_pd,
                            get_source_position=["origin_lon", "origin_lat"],
                            get_target_position=["dest_lon", "dest_lat"],
                            get_width="width",
                            get_source_color=[0, 128, 255, 80],
                            get_target_color=[0, 128, 255, 80],
                            pickable=True,
                            auto_highlight=True,
                        )

                        view_state = pdk.ViewState(
                            latitude=route_pd['origin_lat'].mean(),
                            longitude=route_pd['origin_lon'].mean(),
                            zoom=3,
                            pitch=0
                        )

                        r = pdk.Deck(
                            layers=[routes_layer],
                            initial_view_state=view_state,
                            map_style="mapbox://styles/mapbox/light-v10",
                            tooltip={
                                "html": "<b>Rotta:</b> {Origin} → {Dest}<br/>"
                                        "<b>Voli:</b> {flight_count}<br/>"
                                        "<b>Compagnie:</b> {airlines}",
                                "style": {
                                    "backgroundColor": "steelblue",
                                    "color": "white"
                                }
                            }
                        )

                        st.pydeck_chart(r)

                else:
                    st.warning("Non sono disponibili dati di rotta per la visualizzazione.")


        with st.container(border=True):
            st.title("Tratte aeree")
            st.write("Visualizzazione della mappa delle tratte aeree estratte dal dataset.")
            # Richiama la funzione per visualizzare la mappa, usando i dati dal DataFrame Spark
            #display_routes_map(df)


    elif scelta == "Ritardi e puntualità":
        st.title("Ritardi")
        st.write("Analisi dei ritardi e della puntualità dei voli.")
        with st.container(border=True):
            st.subheader("Ritardi Medi per Compagnia Aerea")
            df_stats = ritardi_medi_per_compagnia(df).toPandas()
            fig = px.bar(df_stats, x="IATA_CODE_Reporting_Airline",
                         y=["Ritardo_Medio_Partenza", "Ritardo_Medio_Arrivo"],
                         barmode="group", title="Ritardi Medi per Compagnia")
            st.plotly_chart(fig, use_container_width=True)

        with st.container(border=True):
            st.subheader("Ritardi Medi per Mese")
            df_mese = ritardi_medi_per_mese(df).toPandas()
            fig = px.line(df_mese, x="Mese", y=["Ritardo_Medio_Partenza", "Ritardo_Medio_Arrivo"], markers=True,
                          title="Ritardi Medi Mensili")
            st.plotly_chart(fig, use_container_width=True)


        with st.container(border=True):
            st.subheader("Distribuzione dei Ritardi in Partenza")
            df_hist = distribuzione_ritardi(df).toPandas()
            fig = px.histogram(df_hist, x="DepDelayMinutes", nbins=50, title="Distribuzione Ritardi")
            st.plotly_chart(fig, use_container_width=True)

        with st.container(border=True):
            st.subheader("Confronto Ritardi Partenza vs Arrivo")
            df_comp = confronto_ritardi(df).toPandas()
            fig = px.scatter(df_comp, x="DepDelayMinutes", y="ArrDelayMinutes",
                             title="Confronto Ritardi Partenza vs Arrivo", trendline="ols")
            st.plotly_chart(fig, use_container_width=True)

        with st.container(border=True):
            st.subheader("Analisi Gruppi Ritardo Partenza e Arrivo")
            df_gruppi = gruppi_ritardo(df).toPandas()
            fig = px.density_heatmap(df_gruppi, x="DepartureDelayGroups", y="ArrivalDelayGroups", z="count",
                                     color_continuous_scale="Viridis", title="Distribuzione Gruppi di Ritardo")
            st.plotly_chart(fig, use_container_width=True)

        with st.container(border=True):
            st.subheader("Percentuale Ritardi oltre 15 Minuti")
            percentuali = percentuali_ritardi(df)
            st.metric(label="Partenze con Ritardo > 15 min", value=f"{percentuali['DepDel15']}%")
            st.metric(label="Arrivi con Ritardo > 15 min", value=f"{percentuali['ArrDel15']}%")



    elif scelta == "Aeroporto":
        mostra_aeroporto(df, aeroporti_info)


    elif scelta == "Compagnie aeree":
        mostra_compagnia(df, aeroporti_info)

    elif scelta == "Condizioni operative":
        st.title("Condizioni operative")
        st.write("Dati su meteo e congestioni.")
        # Aggiungi codice per questa sezione

    elif scelta == "Informazioni tecniche":
        st.title("Informazioni tecniche")
        st.write("Inserire informazioni tecniche sui dati, come tipo di dataset, numero di righe, numero di colonne, etc.."
                 ""
                 "Inserire ad esempio una sezione dove l'utente seleziona la tratta e sulla base dei dati fa dellle previsioni utilizzando l'ia, in qualche modo")
        # Aggiungi codice per questa sezione

if __name__ == "__main__":
    main()
