import streamlit as st
from Backend.query_home import (calcola_kpi, aggregazione_voli_per_mese, \

                                calcola_voli_per_giorno, crea_heatmap_calendario, calcola_puntualita,
                                calcola_percentuale_ritardi_15, calcola_numero_aeroporti, calcola_numero_citta,
                                calcola_top5_compagnie_puntuali, calcola_top5_aeroporti_piu_ritardi,
                                calcola_top5_tratte_trafficate, calcola_top5_compagnie_piu_voli,
                                calcola_top5_compagnie_meno_cancellazioni, calcola_mappa_aeroporti)

import plotly.express as px

import plotly.graph_objects as go
import pandas as pd
import pydeck as pdk


def mostra_home(df, aeroporti_info):
    st.title("Dashboard Analisi Voli")
    st.write("Statistiche generali dei dati")

    with st.container(border=True):
        kpi = calcola_kpi(df)
        perc_ritardi_15 = calcola_percentuale_ritardi_15(df)
        num_aeroporti = calcola_numero_aeroporti(df)
        num_citta = calcola_numero_citta(df, aeroporti_info)
        st.subheader("Indicatori chiave")
        cols = st.columns(5)
        cols[0].metric("Totale Voli", kpi["totale_voli"])
        cols[1].metric("Voli Cancellati", kpi["voli_cancellati"])
        cols[2].metric("Voli Deviati", kpi["voli_deviati"])
        cols[3].metric("Ritardo Medio Partenza", f"{kpi['ritardo_medio_partenza']:.2f} min")
        cols[4].metric("Ritardo Medio Arrivo", f"{kpi['ritardo_medio_arrivo']:.2f} min")

        cols = st.columns(5)
        cols[0].metric("Distanza Media", f"{kpi['distanza_media']:.2f} miglia")
        cols[1].metric("% voli cancellati", f"{kpi['percentuale_cancellati']:.2f} %")
        cols[2].metric("% Ritardi > 15 min", f"{perc_ritardi_15:.2f} %")
        cols[3].metric("Numero Aeroporti", num_aeroporti)
        cols[4].metric("Numero Città Coinvolte", num_citta)

    with st.container(border=True):
        st.subheader("Top 5 Analisi")

        # Calcola tutti i Top 5
        df1 = calcola_top5_compagnie_puntuali(df)           # Compagnie più puntuali
        df2 = calcola_top5_aeroporti_piu_ritardi(df)       # Aeroporti con più ritardi
        df3 = calcola_top5_tratte_trafficate(df)           # Tratte più trafficate
        df4 = calcola_top5_compagnie_piu_voli(df)          # Compagnie con più voli
        df5 = calcola_top5_compagnie_meno_cancellazioni(df) # Compagnie meno cancellazioni

        # Prima riga: 3 grafici
        row1_cols = st.columns(3)
        # 1. Compagnie più puntuali (ritardo medio minore)
        fig1 = px.bar(df1,
                      x="Compagnia",
                      y="Ritardo Medio (min)",
                      title="Compagnie più puntuali",
                      text_auto='.2f')
        fig1.update_layout(margin=dict(t=40, b=20))
        row1_cols[0].plotly_chart(fig1, use_container_width=True)

        # 2. Aeroporti con più ritardi
        fig2 = px.bar(df2,
                      x="Aeroporto",
                      y="Voli in Ritardo",
                      title="Aeroporti con più ritardi",
                      text_auto=True)
        fig2.update_layout(margin=dict(t=40, b=20))
        row1_cols[1].plotly_chart(fig2, use_container_width=True)

        # Trasforma il DataFrame di output calcola_top5_tratte_trafficate
        df3["Rotta"] = df3["Origine"] + " → " + df3["Destinazione"]

        fig3 = px.bar(
            df3.sort_values("Voli"),  # ordina per “Voli” crescente
            x="Voli",
            y="Rotta",
            orientation="h",  # barre orizzontali
            text="Voli",
            title="Top 5 Rotte più trafficate",
        )

        fig3.update_layout(
            yaxis=dict(title="Rotta",  # titolo asse Y
                       automargin=True),  # evita tagli sulle etichette lunghe
            xaxis=dict(title="Numero di Voli"),
            margin=dict(l=150, t=40, b=20)  # lascia spazio alle etichette
        )

        row1_cols[2].plotly_chart(fig3, use_container_width=True)

        # Seconda riga: 2 grafici
        row2_cols = st.columns(2)
        # 4. Compagnie con più voli
        fig4 = px.bar(df4,
                      x="Compagnia",
                      y="Voli",
                      title="Compagnie con più voli",
                      text_auto=True)
        fig4.update_layout(margin=dict(t=40, b=20))
        row2_cols[0].plotly_chart(fig4, use_container_width=True)

        # 5. Compagnie con meno cancellazioni
        fig5 = px.bar(df5,
                      x="Compagnia",
                      y="% Cancellazioni",
                      title="Compagnie con meno cancellazioni",
                      text_auto='.2f')
        fig5.update_layout(margin=dict(t=40, b=20))
        row2_cols[1].plotly_chart(fig5, use_container_width=True)

    with st.container(border=True):
        st.subheader("Mappa Aeroporti")
        mappa_df = calcola_mappa_aeroporti(df, aeroporti_info)

        if not mappa_df.empty:
            # 1) ScatterplotLayer per cerchi
            scatter = pdk.Layer(
                "ScatterplotLayer",
                data=mappa_df,
                pickable=True,
                stroked=True,
                filled=True,
                radius_scale=1,
                radius_min_pixels=10,
                radius_max_pixels=200,
                get_position=["longitude", "latitude"],
                get_radius="radius",
                get_fill_color="color",
                tooltip={
                    "html": (
                        "<b>Aeroporto:</b> {airport_name} ({iata})<br/>"
                        "<b>Voli:</b> {flight_count}<br/>"
                        "<b>Ritardo Medio:</b> {avg_delay:.2f} min"
                    ),
                    "style": {"backgroundColor": "steelblue", "color": "white"}
                }
            )



            view_state = pdk.ViewState(
                latitude=mappa_df["latitude"].mean(),
                longitude=mappa_df["longitude"].mean(),
                zoom=3,
                pitch=0
            )

            deck = pdk.Deck(
                layers=[scatter],
                initial_view_state=view_state,
                map_style="mapbox://styles/mapbox/light-v10",
                tooltip={
                    "html": (
                        "<b>Aeroporto:</b> {airport_name} ({iata})<br/>"
                        "<b>Voli:</b> {flight_count}<br/>"
                        "<b>Ritardo Medio:</b> {avg_delay} min"
                    ),
                    "style": {"backgroundColor": "steelblue", "color": "white"}
                }
            )
            st.pydeck_chart(deck)

            # Legenda...
            min_delay = mappa_df["avg_delay"].min()
            max_delay = mappa_df["avg_delay"].max()
            min_flights = mappa_df["flight_count"].min()
            max_flights = mappa_df["flight_count"].max()

            st.markdown("**Legenda Colore (ritardo medio)**")
            st.markdown(f"- 🟩 Verde = {min_delay:.1f} min di ritardo medio")
            st.markdown(f"- 🟥 Rosso = {max_delay:.1f} min di ritardo medio")

            st.markdown("**Legenda Dimensione (numero voli)**")
            st.markdown(f"- 🔵 Cerchio piccolo = {min_flights} voli")
            st.markdown(f"- 🔴 Cerchio grande = {max_flights} voli")
        else:
            st.warning("Nessun dato disponibile per la mappa aeroporti.")

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


