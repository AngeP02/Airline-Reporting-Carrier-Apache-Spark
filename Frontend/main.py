import streamlit as st
from datetime import datetime
import sys
import os
import plotly.express as px
from pyspark import StorageLevel
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import plotly.graph_objects as go

import pandas as pd
from streamlit_plotly_events import plotly_events

os.environ["PYSPARK_PYTHON"] = "python"


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from Backend.query import carica_dati, elenco_aeroporti_NY, statistiche_aeroporto, mappa_aeroporti, \
    disegna_collegamenti, calcola_kpi, elenco_tutti_aeroporti, aggregazione_voli_per_mese, aggregazione_voli_per_giorno, \
    calcola_puntualita, aggiungi_rotte

# Carica il dataset
df = carica_dati("C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Dati/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_1.csv")
df = df.persist(StorageLevel.MEMORY_ONLY)

# Interfaccia Streamlit
def main():
    # Layout principale
    st.set_page_config(page_title="Analisi Voli", layout="wide")

    #st.title("Dashboard Analisi Voli")

    menu = ["Home", "Ritardi e puntualità", "Cancellazioni e deviazioni", "Tratte aeree", "Condizioni operative", "Informazioni tecniche"]
    scelta = st.tabs( menu)

    with scelta[0]:
        st.title("Dashboard Analisi Voli")
        st.write("Statistiche generali dei dati")

        with st.container(border=True):
            kpi = calcola_kpi(df)
            st.subheader("Indicatori chiave")
            col1, col2, col3, col4, col5, col6 = st.columns(6)
            col1.metric("Totale Voli", kpi["totale_voli"])
            col2.metric("Voli Cancellati", kpi["voli_cancellati"])
            col3.metric("Voli Deviati", kpi["voli_deviati"])
            col4.metric("Ritardo Medio Partenza", f"{kpi['ritardo_medio_partenza']:.2f} min")
            col5.metric("Ritardo Medio Arrivo", f"{kpi['ritardo_medio_arrivo']:.2f} min")
            col6.metric("Distanza Media", f"{kpi['distanza_media']:.2f} miglia")


        #with st.container(border=True):
            #fig = mappa_aeroporti(df)

            # Aggiungi le rotte principali sulla mappa
            #fig = aggiungi_rotte(df, fig)

            # Mostra la mappa in Streamlit
           # st.plotly_chart(fig)

        with st.container(border=True):
            # Crea due colonne
            col1, col2 = st.columns(2)  # 2 colonne affiancate
            # Aggiungi bordi alle colonne utilizzando il CSS

            with col1:
                st.subheader("Andamento dei Voli per Mese")
                # Esegui l'aggregazione dei voli per mese
                agg_df = aggregazione_voli_per_mese(df)
                # Converte il DataFrame PySpark in Pandas
                agg_df_pandas = agg_df.toPandas()
                # Crea il grafico a barre con Plotly
                fig = go.Figure()
                # Aggiungi una traccia per il grafico a barre
                fig.add_trace(go.Bar(
                    x=agg_df_pandas['Month'],  # Mesi sull'asse X
                    y=agg_df_pandas['NumeroVoli'],  # Numero di voli sull'asse Y
                    name='Numero di voli per mese',  # Nome della traccia
                    marker=dict(color='blue')  # Colore delle barre
                ))
                # Aggiungi titolo e etichette agli assi
                fig.update_layout(
                    title='Numero di voli per mese',
                    xaxis_title='Mese',
                    yaxis_title='Numero di voli',
                    template='plotly_dark',  # Tema oscuro per il grafico
                    showlegend=False  # Nascondi la legenda
                )
                # Visualizza il grafico interattivo
                st.plotly_chart(fig)

            with col2:
                st.subheader("Puntualità vs. Ritardi dei Voli")
                # Calcola la puntualità
                puntualita_df = calcola_puntualita(df)

                # Converti il DataFrame PySpark in Pandas per la visualizzazione
                puntualita_pd = puntualita_df.toPandas()

                # Crea il grafico a torta (donut) con Plotly
                fig = go.Figure(data=[go.Pie(
                    labels=puntualita_pd["Status"],
                    values=puntualita_pd["Percentage"],
                    hole=0.3,  # Crea un grafico a donut
                    marker=dict(colors=["#00cc96", "#f44336", "#ff9800"])  # Colori personalizzati
                )])

                # Aggiungi titolo al grafico
                fig.update_layout(
                    title="Puntualità vs. Ritardi dei Voli",
                    showlegend=True
                )

                # Visualizza il grafico a torta (donut) in Streamlit
                st.plotly_chart(fig)

        #with st.container(border=True):
         #   st.subheader("Mappa Collegamenti Aeroporti")
          #  fig, airports_locations = mappa_aeroporti(df)

           # aeroporto_selezionato = st.selectbox("Seleziona un aeroporto", [loc[0] for loc in airports_locations])

            # Mostra i collegamenti se un aeroporto è selezionato
            #if aeroporto_selezionato:
             #   linee_collegamenti = disegna_collegamenti(aeroporto_selezionato, df, airports_locations)

              #  if linee_collegamenti:
               #     for line in linee_collegamenti:
                #        fig.add_trace(line)
                 #   st.plotly_chart(fig)
               # else:
                #    st.write(f"Nessun collegamento trovato per l'aeroporto {aeroporto_selezionato}.")

        with st.container(border=True):
            st.subheader("Seleziona Aeroporto/i")
            aeroporti_df = elenco_tutti_aeroporti(df)
            aeroporti_list = [aeroporto['Origin'] for aeroporto in aeroporti_df.collect()]
            aeroporti_list = sorted(aeroporti_list)
            aeroporti_selezionati = st.multiselect("Seleziona uno o più aeroporti", aeroporti_list)

            if aeroporti_selezionati :
                if len(aeroporti_selezionati) < 2:
                    modalita = "Media"  # Se un solo aeroporto è selezionato, seleziona automaticamente "Media"
                else:
                    options = ["Media", "Confronto"]
                    modalita = st.pills("Seleziona modalità", options)

                # Calcolare le statistiche degli aeroporti selezionati
                stats_list = [statistiche_aeroporto(aeroporto, df) for aeroporto in aeroporti_selezionati]

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


            else:
                # Se nessun aeroporto è selezionato, calcoliamo le statistiche medie per tutti gli aeroporti
                st.subheader("Statistiche Medie di Tutti gli Aeroporti")

                # Calcolare le statistiche di tutti gli aeroporti
                stats_list = [statistiche_aeroporto(aeroporto, df) for aeroporto in aeroporti_list]

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

                # Calcolare le statistiche medie per tutti gli aeroporti
                media_stats = stats_spark_df.agg(
                    {'Totale_Voli': 'avg', 'Ritardo_Medio_Partenza': 'avg', 'Ritardo_Medio_Arrivo': 'avg',
                     'Voli_Cancellati': 'avg', 'Voli_Dirottati': 'avg'}).collect()[0]

                st.write(media_stats)

                # Visualizzazione grafica con Plotly
                fig = px.bar(
                    x=['Totale_Voli', 'Ritardo_Medio_Partenza', 'Ritardo_Medio_Arrivo', 'Voli_Cancellati',
                       'Voli_Dirottati'],
                    y=[media_stats['avg(Totale_Voli)'], media_stats['avg(Ritardo_Medio_Partenza)'],
                       media_stats['avg(Ritardo_Medio_Arrivo)'], media_stats['avg(Voli_Cancellati)'],
                       media_stats['avg(Voli_Dirottati)']],
                    title="Statistiche Medie di Tutti gli Aeroporti"
                )
                st.plotly_chart(fig)


    with scelta[1]:
        st.title("Ritardi")
        st.write("Analisi dei ritardi e della puntualità dei voli.")
        # Aggiungi codice per questa sezione


    with scelta[1]:
        st.title("Cancellazioni e deviazioni")
        st.write("Statistiche sulla cancellazione e deviazione.")
        # Aggiungi codice per questa sezione

    with scelta[2]:
        st.title("Tratte aeree")
        st.write("Informazioni sulle tratte aeree.")
        # Aggiungi codice per questa sezione

    with scelta[3]:
        st.title("Condizioni operative")
        st.write("Dati su meteo e congestioni.")
        # Aggiungi codice per questa sezione

    with scelta[4]:
        st.title("Informazioni tecniche")
        st.write("Inserire informazioni tecniche sui dati, come tipo di dataset, numero di righe, numero di colonne, etc.."
                 ""
                 "Inserire ad esempio una sezione dove l'utente seleziona la tratta e sulla base dei dati fa dellle previsioni utilizzando l'ia, in qualche modo")
        # Aggiungi codice per questa sezione

if __name__ == "__main__":
    main()
