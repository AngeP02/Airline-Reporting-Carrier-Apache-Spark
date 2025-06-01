import pandas as pd
import streamlit as st
import plotly.express as px
from Backend.query_tratte_aeree import classifica_tratte_ritardo, classifica_tratte_cancellazioni, \
    variabilita_ritardi_tratte, ritardo_per_fascia_oraria, mappa_rotte_completa
import pydeck as pdk

def mostra_tratte(df, aeroporti_info):
    st.title("Analisi per Tratte Aeree")
    with st.container(border=True):
        st.subheader("Criticità Tratte Aeree")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("Top 10 Tratte per Ritardo Medio di Partenza")
            df_ritardo = classifica_tratte_ritardo(df).limit(10).toPandas()
            fig_delay = px.bar(
                df_ritardo, x=df_ritardo["Origin"] + "→" + df_ritardo["Dest"], y="AvgDepDelay",
                text="AvgDepDelay", labels={"x": "Tratta", "AvgDepDelay": "Ritardo medio (min)"}
            )
            fig_delay.update_traces(texttemplate="%{text:.1f}", textposition="outside")
            st.plotly_chart(fig_delay, use_container_width=True)
        with col2:
            st.markdown("Top 10 Tratte per Numero di Cancellazioni")
            df_cancellati = classifica_tratte_cancellazioni(df).limit(10).toPandas()
            fig_cancel = px.bar(
                df_cancellati, x=df_cancellati["Origin"] + "→" + df_cancellati["Dest"], y="NumCancelled",
                text="NumCancelled", labels={"x": "Tratta", "NumCancelled": "Cancellazioni"}
            )
            fig_cancel.update_traces(textposition="outside")
            st.plotly_chart(fig_cancel, use_container_width=True)
        st.markdown("Top 10 Tratte per Variabilità dei Ritardi")
        df_var = variabilita_ritardi_tratte(df).limit(10).toPandas()
        fig_var = px.bar(
            df_var, x=df_var["Origin"] + "→" + df_var["Dest"], y="StdDevDepDelay",
            text="StdDevDepDelay", labels={"x": "Tratta", "StdDevDepDelay": "Deviazione standard (min)"}
        )
        fig_var.update_traces(texttemplate="%{text:.1f}", textposition="outside")
        st.plotly_chart(fig_var, use_container_width=True)

        st.markdown("Ritardo Medio per Fascia Oraria")
        df_rit_fascia = ritardo_per_fascia_oraria(df).toPandas()
        fig_block = px.pie(
            df_rit_fascia,
            names="TimeBlock",
            values="AvgDepDelay",
            #title="Ritardo Medio per Fascia Oraria",
            labels={"TimeBlock": "Fascia Oraria", "AvgDepDelay": "Ritardo medio (min)"}
        )
        fig_block.update_traces(textinfo="percent+label", hovertemplate="%{label}<br>Ritardo medio: %{value:.1f} min")
        st.plotly_chart(fig_block, use_container_width=True)

    with st.container(border=True):
        st.subheader("Mappa interattiva delle rotte")
        dati_rotte = mappa_rotte_completa(df, aeroporti_info)
        rotte_pd = dati_rotte.toPandas()
        if rotte_pd.empty:
            st.warning("Non sono disponibili dati di rotta.")
            return
        tutti_aeroporti = sorted(set(rotte_pd['Origin']).union(set(rotte_pd['Dest'])))
        aeroporto_selezionato = st.selectbox(
            "Seleziona un aeroporto",
            options=["Tutti"] + tutti_aeroporti,
            index=0
        )
        df = rotte_pd.copy()
        if aeroporto_selezionato != "Tutti":
            df = df[(df['Origin'] == aeroporto_selezionato) | (df['Dest'] == aeroporto_selezionato)]
        if df.empty:
            st.warning("Nessuna rotta soddisfa i criteri selezionati.")
        else:
            df['width'] = df['flight_count'] / df['flight_count'].max() * 10
            df['color'] = df.apply(lambda _: [0, 128, 255, 80], axis=1)
            routes_layer = pdk.Layer(
                "ArcLayer",
                data=df,
                get_source_position=["origin_lon", "origin_lat"],
                get_target_position=["dest_lon", "dest_lat"],
                get_width="width",
                get_source_color="color",
                get_target_color="color",
                pickable=True,
                auto_highlight=True,
            )
            view_state = pdk.ViewState(
                latitude=df['origin_lat'].mean(),
                longitude=df['origin_lon'].mean(),
                zoom=3,
                pitch=0
            )
            deck = pdk.Deck(
                layers=[routes_layer],
                initial_view_state=view_state,
                map_style="mapbox://styles/mapbox/light-v10",
                tooltip={
                    "html": "<b>{Origin} → {Dest}</b><br/>Voli: {flight_count}<br/>Ritardo medio: {avg_delay} min<br/>Cancellazioni: {cancelled_count}",
                    "style": {"backgroundColor": "steelblue", "color": "white"}
                }
            )
            st.pydeck_chart(deck)
            st.markdown("---")
            st.subheader("Dettagli tratta selezionata")
            opzioni_rotte = df['Origin'] + " → " + df['Dest']
            sel = st.selectbox("Seleziona una tratta", options=opzioni_rotte)
            if sel:
                ori, dest = sel.split(" → ")
                row = df[(df['Origin'] == ori) & (df['Dest'] == dest)].iloc[0]
                df_highlight = pd.DataFrame([row])
                with st.expander(f"Dettagli per la tratta {sel}"):
                    st.markdown(f"""
                    - **Numero voli:** {int(row['flight_count'])}
                    - **Numero cancellazioni:** {int(row['cancelled_count'])}
                    - **Ritardo medio:** {row['avg_delay']:.2f} minuti
                    - **Città partenza:** {row['origin_city']}
                    - **Città destinazione:** {row['dest_city']}
                    - **Voli A→B:** {int(row['flight_count'])}
                    - **Voli B→A:** {int(row['count_BA'])}
                    - **Top 5 compagnie:** {', '.join(row['top_airlines'])}
                    - **Top 2 mesi con più voli:** {', '.join(str(m) for m in row['top_months'])}
                    """)
                df_highlight['color'] = df_highlight.apply(lambda _: [255, 0, 0, 180], axis=1)
                df_highlight['width'] = df_highlight['width'] * 1.5