import streamlit as st
from Backend.query_home import (calcola_kpi, aggregazione_voli_per_mese, calcola_puntualita,
                                calcola_percentuale_ritardi_15, calcola_numero_aeroporti, calcola_numero_citta,
                                calcola_top5_compagnie_puntuali, calcola_top5_aeroporti_piu_ritardi,
                                calcola_top5_tratte_trafficate, calcola_top5_compagnie_piu_voli,
                                calcola_top5_compagnie_meno_cancellazioni, calcola_mappa_aeroporti)

import plotly.express as px
import plotly.graph_objects as go
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

        df1 = calcola_top5_compagnie_puntuali(df)
        df2 = calcola_top5_aeroporti_piu_ritardi(df)
        df3 = calcola_top5_tratte_trafficate(df)
        df4 = calcola_top5_compagnie_piu_voli(df)
        df5 = calcola_top5_compagnie_meno_cancellazioni(df)

        colonne_riga1 = st.columns(3)
        colonne_riga2 = st.columns(2)

        fig1 = px.bar(df1,
                      x="Compagnia",
                      y="Ritardo Medio (min)",
                      title="Compagnie più puntuali",
                      text_auto='.2f')
        fig1.update_layout(margin=dict(t=40, b=20))
        colonne_riga1[0].plotly_chart(fig1, use_container_width=True)

        # Aeroporti con più ritardi
        fig2 = px.bar(df2,
                      x="Aeroporto",
                      y="Voli in Ritardo",
                      title="Aeroporti con più ritardi",
                      text_auto=True)
        fig2.update_layout(margin=dict(t=40, b=20))
        colonne_riga1[1].plotly_chart(fig2, use_container_width=True)

        # Tratte più trafficate
        df3["Rotta"] = df3["Origine"] + " → " + df3["Destinazione"]
        fig3 = px.bar(
            df3.sort_values("Voli"),
            x="Voli",
            y="Rotta",
            orientation="h",
            text="Voli",
            title="Top 5 Rotte più trafficate",
        )
        fig3.update_layout(
            yaxis=dict(title="Rotta",
                       automargin=True),
            xaxis=dict(title="Numero di Voli"),
            margin=dict(l=150, t=40, b=20)
        )
        colonne_riga1[2].plotly_chart(fig3, use_container_width=True)

        # Compagnie con più voli
        fig4 = px.bar(df4,
                      x="Compagnia",
                      y="Voli",
                      title="Compagnie con più voli",
                      text_auto=True)
        fig4.update_layout(margin=dict(t=40, b=20))
        colonne_riga2[0].plotly_chart(fig4, use_container_width=True)

        # Compagnie con meno cancellazioni
        fig5 = px.bar(df5,
                      x="Compagnia",
                      y="% Cancellazioni",
                      title="Compagnie con meno cancellazioni",
                      text_auto='.2f')
        fig5.update_layout(margin=dict(t=40, b=20))
        colonne_riga2[1].plotly_chart(fig5, use_container_width=True)

    with st.container(border=True):
        st.subheader("Mappa Aeroporti")
        mappa_df = calcola_mappa_aeroporti(df, aeroporti_info)

        if not mappa_df.empty:
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

            min_ritardo = mappa_df["avg_delay"].min()
            max_ritardo = mappa_df["avg_delay"].max()
            min_voli = mappa_df["flight_count"].min()
            max_voli = mappa_df["flight_count"].max()

            col1,col2 = st.columns(2)
            with col1:
                st.markdown("**Legenda Dimensione Cerchi**")
                st.markdown(f"Cerchio piccolo: circa {min_voli} voli")
                st.markdown(f"Cerchio grande: circa {max_voli} voli")

            with col2:
                st.markdown("**Legenda Colore Cerchi**")
                st.markdown(f"Colore chiaro: ritardo medio vicino a {min_ritardo:.2f} min")
                st.markdown(f"Colore scuro: ritardo medio vicino a {max_ritardo:.2f} min")
        else:
            st.warning("Nessun dato disponibile per la mappa aeroporti.")

    with st.container(border=True):
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Andamento dei Voli per Mese")
            agg_df = aggregazione_voli_per_mese(df)
            agg_df_pandas = agg_df.toPandas()
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=agg_df_pandas['Month'],
                y=agg_df_pandas['NumeroVoli'],
                name='Numero di voli per mese',
                marker=dict(color='steelblue')
            ))
            fig.update_layout(
                xaxis_title='Mese',
                yaxis_title='Numero di voli',
                template='plotly_dark',
                showlegend=False
            )
            fig.update_layout(xaxis=dict(tickmode='linear'))
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
                showlegend=True
            )
            st.plotly_chart(fig)