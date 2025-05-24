import streamlit as st

from Backend.query_ritardi_cancellazioni import perc_voli_in_orario, perc_voli_in_ritardo, perc_voli_cancellati, \
    ritardo_medio_partenza, ritardo_medio_arrivo, \
    serie_settimanale_cancellazioni, serie_mensile_cancellazioni, serie_settimanale_ritardi_cumulativi, \
    serie_mensile_ritardi_cumulativi, ritardo_medio_per_giorno_settimana, ritardo_medio_per_mese, percentuale_cause, \
    trend_cause_mensile, classifica_aeroporti_cancellazioni, classifica_aeroporti_ritardi, classifica_compagnie
import plotly.express as px
def mostra_ritardi_cancellazioni(df):
    with st.container(border=True):
        st.subheader("Statistiche Generali Ritardi e Cancellazioni")

        # Calcolo metriche
        on_time = perc_voli_in_orario(df)
        delayed = perc_voli_in_ritardo(df)
        cancelled = perc_voli_cancellati(df)
        avg_dep = ritardo_medio_partenza(df)
        avg_arr = ritardo_medio_arrivo(df)

        # Visualizzazione metriche
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("% Voli in orario", f"{on_time}%")
        col2.metric("% Voli in ritardo", f"{delayed}%")
        col3.metric("% Voli cancellati", f"{cancelled}%")
        col4.metric("Ritardo medio partenza", f"{avg_dep} min")
        col5.metric("Ritardo medio arrivo", f"{avg_arr} min")

    with st.container(border=True):
        st.subheader("Serie Temporali Cumulative")
        col1, col2 = st.columns(2)
        with col1:
            # Ritardi cumulativi per giorno della settimana
            st.markdown("Ritardi cumulativi per giorno della settimana")
            df_w_delay = serie_settimanale_ritardi_cumulativi(df).toPandas()
            fig_w_delay = px.bar(
                df_w_delay,
                x="DayOfWeek",
                y="TotalDepDelay",
                text="TotalDepDelay",
                labels={"DayOfWeek": "Giorno della settimana", "TotalDepDelay": "Ritardo cumulativo (min)"},
            )
            fig_w_delay.update_traces(texttemplate="%{text:.0f}", textposition="outside")
            fig_w_delay.update_layout(xaxis=dict(tickmode='linear'))
            st.plotly_chart(fig_w_delay, use_container_width=True)
        with col2:
            # Ritardi cumulativi mensili
            st.markdown("Ritardi cumulativi per mese")
            df_m_delay = serie_mensile_ritardi_cumulativi(df).toPandas()
            fig_m_delay = px.line(
                df_m_delay,
                x="Month",
                y="TotalDepDelay",
                markers=True,
                text="TotalDepDelay",
                labels={"Month": "Mese", "TotalDepDelay": "Ritardo cumulativo (min)"},
            )
            fig_m_delay.update_traces(texttemplate="%{text:.0f}", textposition="top center")
            fig_m_delay.update_layout(xaxis=dict(tickmode='linear'))
            st.plotly_chart(fig_m_delay, use_container_width=True)
        col1, col2 = st.columns(2)
        with col1:
            # Cancellazioni per giorno della settimana
            st.markdown("Cancellazioni cumulativi per giorno della settimana")
            df_w_cancel = serie_settimanale_cancellazioni(df).toPandas()
            fig_w_cancel = px.bar(
                df_w_cancel,
                x="DayOfWeek",
                y="Cancellazioni",
                text="Cancellazioni",
                labels={"DayOfWeek": "Giorno della settimana", "Cancellazioni": "Numero di cancellazioni"},
            )
            fig_w_cancel.update_traces(texttemplate="%{text}", textposition="outside")
            fig_w_cancel.update_layout(xaxis=dict(tickmode='linear'))
            st.plotly_chart(fig_w_cancel, use_container_width=True)
        with col2:
            # Cancellazioni mensili
            st.markdown("Cancellazioni cumulativi per mese")
            df_m_cancel = serie_mensile_cancellazioni(df).toPandas()
            fig_m_cancel = px.line(
                df_m_cancel,
                x="Month",
                y="Cancellazioni",
                markers=True,
                text="Cancellazioni",
                labels={"Month": "Mese", "Cancellazioni": "Numero di cancellazioni"},
            )
            fig_m_cancel.update_traces(texttemplate="%{text}", textposition="top center")
            fig_m_cancel.update_layout(xaxis=dict(tickmode='linear'))
            st.plotly_chart(fig_m_cancel, use_container_width=True)

        # --- Analisi Temporale Ritardo Medio ---
    with st.container(border=True):
        st.subheader("Analisi Temporale Ritardo Medio")

        col1, col2 = st.columns(2)
        with col1:
            # Ritardo medio mensile
            st.markdown("Ritardo medio per mese")
            df_avg_m = ritardo_medio_per_mese(df).toPandas()
            fig_avg_m = px.bar(
                df_avg_m,
                x="Month",
                y="AvgDepDelay",
                text="AvgDepDelay",
                labels={"Month": "Mese", "AvgDepDelay": "Ritardo medio (min)"},
            )
            fig_avg_m.update_traces(texttemplate="%{text:.1f}", textposition="outside")
            fig_avg_m.update_layout(xaxis=dict(tickmode='linear'))
            st.plotly_chart(fig_avg_m, use_container_width=True)
        with col2:
            # Ritardo medio per giorno della settimana
            st.markdown("Ritardo medio per giorno della settimana")
            df_avg_w = ritardo_medio_per_giorno_settimana(df).toPandas()
            fig_avg_w = px.bar(
                df_avg_w,
                x="DayOfWeek",
                y="AvgDepDelay",
                text="AvgDepDelay",
                labels={"DayOfWeek": "Giorno della settimana", "AvgDepDelay": "Ritardo medio (min)"},
            )
            fig_avg_w.update_traces(texttemplate="%{text:.1f}", textposition="outside")
            st.plotly_chart(fig_avg_w, use_container_width=True)

        # --- Classifiche Compagnie ---
        with st.container(border=True):
            st.subheader("Classifiche Compagnie Aeree per ritardi e cancellazioni")
            df_comp = classifica_compagnie(df).toPandas()
            # rinomina le colonne in italiano
            df_comp_ita = df_comp.rename(columns={
                "IATA_CODE_Reporting_Airline": "Compagnia",
                "AvgDepDelay": "Ritardo medio partenza (min)",
                "AvgArrDelay": "Ritardo medio arrivo (min)",
                "PercDelayed": "% Voli in ritardo",
                "PercCancelled": "% Voli cancellati"
            })

            # disegno la tabella non-editabile
            st.dataframe(
                df_comp_ita,
                use_container_width=True
            )

            # grafico ritardo medio arrivo
            fig_c_arr = px.bar(df_comp, x="IATA_CODE_Reporting_Airline", y="AvgArrDelay",
                               labels={"IATA_CODE_Reporting_Airline": "Compagnia",
                                       "AvgArrDelay": "Ritardo medio arrivo (min)"})
            st.plotly_chart(fig_c_arr, use_container_width=True)

        # --- Classifiche Aeroporti ---
        with st.container(border=True):
            st.subheader("Classifiche Aeroporti (Ritardi e cancellazioni")
            st.markdown("Aeroporti con più ritardi medi")
            df_ar = classifica_aeroporti_ritardi(df).toPandas()
            fig_ar = px.bar(df_ar, x="Origin", y="AvgDepDelay",
                            labels={"Origin": "Aeroporto", "AvgDepDelay": "Ritardo medio partenza (min)"})
            st.plotly_chart(fig_ar, use_container_width=True)

            st.markdown("Aeroporti con più voli cancellati")
            df_ac = classifica_aeroporti_cancellazioni(df).toPandas()
            fig_ac = px.bar(df_ac, x="Origin", y="NumCancelled",
                            labels={"Origin": "Aeroporto", "NumCancelled": "Numero cancellazioni"})
            st.plotly_chart(fig_ac, use_container_width=True)

        # --- Cause Dei Ritardi ---
        with st.container(border=True):
            st.subheader("Analisi Cause Ritardi")
            col1, col2 = st.columns(2)
            with col1:
                df_pct = percentuale_cause(df)
                fig_pct = px.pie(df_pct, names="Cause", values="Percent",
                                 title="Percentuale di minuti di ritardo per causa")
                st.plotly_chart(fig_pct, use_container_width=True)
            with col2:
                df_trend = trend_cause_mensile(df)
                fig_trend = px.line(df_trend, x="Month", y=["Carrier", "Weather", "NAS", "Security", "LateAircraft"],
                                    markers=True, labels={"value": "Minuti di ritardo", "variable": "Causa"},
                                    title="Andamento mensile dei ritardi per causa")
                st.plotly_chart(fig_trend, use_container_width=True)