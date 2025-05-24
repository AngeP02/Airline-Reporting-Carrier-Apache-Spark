import pandas as pd
import streamlit as st
from Backend.query_aeroporto import (
    mappa_tratte_aeroporto, lista_aeroporti_unici, num_voli_partiti, num_voli_arrivati,
    num_voli_cancellati, nome_citta_aeroporto, cause_ritardi_per_aeroporto,
    distribuzione_ritardi_mensile, trend_cancellazioni_stagionale,
    metriche_compagnie, elenco_tutti_aeroporti, statistiche_aeroporto
)
import pydeck as pdk
import plotly.express as px
from pyspark.sql import Row

def mostra_aeroporto(df, aeroporti_info):
    st.title("Analisi per Aeroporto")

    aeroporti_disponibili = lista_aeroporti_unici(df)
    st.markdown(
        "<p style='font-size:20px; margin-bottom:0;'> Seleziona un aeroporto</p>",
        unsafe_allow_html=True
    )
    aeroporto_selezionato = st.selectbox(
        "",
        options=sorted(aeroporti_disponibili)
    )
    if not aeroporto_selezionato:
        st.warning("Seleziona un aeroporto per iniziare.")
        return

    with st.container(border=True):
        st.subheader("Statistiche Generali")
        n_partiti = num_voli_partiti(df, aeroporto_selezionato).collect()[0]["NumVoliPartiti"]
        n_arrivati = num_voli_arrivati(df, aeroporto_selezionato).collect()[0]["NumVoliArrivati"]
        n_cancellati = num_voli_cancellati(df, aeroporto_selezionato).collect()[0]["NumVoliCancellati"]
        citta = nome_citta_aeroporto(df, aeroporto_selezionato)
        nome_citta = citta[0][0] if citta else "Informazione non disponibile"

        col1, col2, col3 = st.columns(3)
        col1.metric("Voli Partiti", f"{n_partiti:,}")
        col2.metric("Voli Arrivati", f"{n_arrivati:,}")
        col3.metric("Voli Cancellati", f"{n_cancellati:,}")

        st.markdown(f"Città in cui è situato l'aeroporto: {nome_citta}")

    with st.container(border=True):
        st.subheader("Mappa delle Tratte")

        tipo = st.selectbox(
            "Visualizza tratte in cui l'aeroporto è:",
            options=["Partenza", "Arrivo"]
        )

        tratte_df = mappa_tratte_aeroporto(df, aeroporti_info, aeroporto_selezionato, tipo)
        tratte_pd = tratte_df.toPandas()

        if not tratte_pd.empty:
            tratte_pd["width"] = tratte_pd["flight_count"] / tratte_pd["flight_count"].max() * 10

            routes_layer = pdk.Layer(
                "ArcLayer",
                data=tratte_pd,
                get_source_position=["origin_lon", "origin_lat"],
                get_target_position=["dest_lon", "dest_lat"],
                get_width="width",
                get_source_color=[0, 128, 255, 80],
                get_target_color=[255, 0, 0, 80],
                pickable=True,
                auto_highlight=True
            )

            view_state = pdk.ViewState(
                latitude=tratte_pd["origin_lat"].mean(),
                longitude=tratte_pd["origin_lon"].mean(),
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
                    "style": {"backgroundColor": "steelblue", "color": "white"}
                }
            )

            st.pydeck_chart(r)
        else:
            st.warning("Nessuna rotta disponibile per questo aeroporto nella modalità selezionata.")

    # ---------------- RITARDI E CANCELLAZIONI ----------------
    with st.container(border=True):
        st.subheader("Ritardi e Cancellazioni")

        # Cause ritardi
        st.markdown("Cause dei Ritardi")
        delay_causes_df = cause_ritardi_per_aeroporto(df, aeroporto_selezionato).toPandas()
        if not delay_causes_df.empty:
            delay_causes_df = delay_causes_df.T.reset_index()
            delay_causes_df.columns = ["Causa", "Minuti di Ritardo"]

            fig_delay = px.bar(
                delay_causes_df,
                x="Causa",
                y="Minuti di Ritardo",
                text="Minuti di Ritardo",
                labels={"Minuti di Ritardo": "Minuti di Ritardo"},
                title=""
            )
            fig_delay.update_traces(texttemplate='%{text:.0f}', textposition='outside')
            fig_delay.update_layout(yaxis_title="Minuti di Ritardo", xaxis_title="Causa")
            st.plotly_chart(fig_delay, use_container_width=True)
        else:
            st.info("Nessun dato di ritardo disponibile per questo aeroporto.")

        # Ritardo medio mensile
        st.markdown("Ritardo Medio Mensile")
        delay_monthly_df = distribuzione_ritardi_mensile(df, aeroporto_selezionato).toPandas()
        if not delay_monthly_df.empty:
            delay_monthly_df["Month"] = delay_monthly_df["Month"].astype(int)
            fig_monthly = px.line(
                delay_monthly_df,
                x="Month",
                y="AvgDepDelay",
                markers=True,
                text="AvgDepDelay",
                labels={"Month": "Mese", "AvgDepDelay": "Ritardo Medio (minuti)"},
                title=""
            )
            fig_monthly.update_traces(texttemplate='%{text:.1f}', textposition='top center')
            fig_monthly.update_layout(yaxis_title="Ritardo Medio (min)", xaxis_title="Mese")
            fig_monthly.update_layout(xaxis=dict(tickmode='linear'))
            st.plotly_chart(fig_monthly, use_container_width=True)
        else:
            st.info("Nessun ritardo disponibile per questo aeroporto.")

        # Cancellazioni per mese
        st.markdown("Cancellazioni per Mese")
        cancel_trend_df = trend_cancellazioni_stagionale(df, aeroporto_selezionato).toPandas()
        if not cancel_trend_df.empty:
            cancel_trend_df["Month"] = cancel_trend_df["Month"].astype(int)
            fig_cancel = px.bar(
                cancel_trend_df,
                x="Month",
                y="Cancellazioni",
                text="Cancellazioni",
                labels={"Month": "Mese", "Cancellazioni": "Numero di Cancellazioni"},
                title=""
            )
            fig_cancel.update_traces(texttemplate='%{text}', textposition='outside')
            fig_cancel.update_layout(yaxis_title="Cancellazioni", xaxis_title="Mese")
            fig_cancel.update_layout(xaxis=dict(tickmode='linear'))
            st.plotly_chart(fig_cancel, use_container_width=True)
        else:
            st.info("Nessuna cancellazione registrata per questo aeroporto.")

    with st.container(border=True):
        st.subheader("Compagnie Aeree in partenza da questo aeroporto")
        df_metrics = metriche_compagnie(df, aeroporto_selezionato).toPandas()
        df_metrics = df_metrics.rename(columns={"IATA_CODE_Reporting_Airline": "Compagnia"})

        if df_metrics.empty:
            st.info("Nessun dato compagnie disponibile per questo aeroporto.")
        else:
            # 2) Normalizzo le metriche
            df = df_metrics.copy()
            df["Norm_Ritardo"] = (df["RitardoMedio"].max() - df["RitardoMedio"]) \
                                 / (df["RitardoMedio"].max() - df["RitardoMedio"].min())
            df["Norm_Cancellazioni"] = (df["PercCancellazioni"].max() - df["PercCancellazioni"]) \
                                       / (df["PercCancellazioni"].max() - df["PercCancellazioni"].min())
            df["Norm_Voli"] = (df["NumVoli"] - df["NumVoli"].min()) \
                              / (df["NumVoli"].max() - df["NumVoli"].min())
            df["Punteggio"] = df["Norm_Ritardo"] * 0.4 + df["Norm_Cancellazioni"] * 0.3 + df["Norm_Voli"] * 0.3

            n_compagnie = len(df)

            if n_compagnie < 10:
                # Unico grafico per tutte le compagnie
                df_all = df.sort_values("Punteggio", ascending=False)
                fig_all = px.bar(
                    df_all,
                    x="Compagnia",
                    y="Punteggio",
                    text="Punteggio",
                    labels={"Punteggio": "Score"},
                    title=f"Tutte le compagnie ({n_compagnie}) ordinate per punteggio"
                )
                fig_all.update_traces(texttemplate="%{text:.2f}", textposition="outside")
                fig_all.update_layout(xaxis_title="Compagnia", yaxis_title="Punteggio")
                st.plotly_chart(fig_all, use_container_width=True)

            else:
                # Top 5 e Worst 5 affiancati
                top5 = df.sort_values("Punteggio", ascending=False).head(5)
                worst5 = df.sort_values("Punteggio", ascending=True).head(5)

                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("#### Top 5 migliori compagnie")
                    fig_top = px.bar(
                        top5,
                        x="Compagnia", y="Punteggio", text="Punteggio",
                        labels={"Punteggio": "Score"},
                        title=""
                    )
                    fig_top.update_traces(texttemplate="%{text:.2f}", textposition="outside")
                    fig_top.update_layout(xaxis_title="Compagnia", yaxis_title="Punteggio")
                    st.plotly_chart(fig_top, use_container_width=True)

                with col2:
                    st.markdown("#### Top 5 peggiori compagnie")
                    fig_worst = px.bar(
                        worst5,
                        x="Compagnia", y="Punteggio", text="Punteggio",
                        labels={"Punteggio": "Score"},
                        title=""
                    )
                    fig_worst.update_traces(texttemplate="%{text:.2f}", textposition="outside", marker_color="crimson")
                    fig_worst.update_layout(xaxis_title="Compagnia", yaxis_title="Punteggio")
                    st.plotly_chart(fig_worst, use_container_width=True)