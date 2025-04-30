import streamlit as st
from streamlit_folium import st_folium
from Backend.query_aeroporto import crea_mappa_tratte


def mostra_aeroporto(df, aeroporti_info):
    st.title("")
    st.write("")
    with st.container(border=True):
        st.subheader("")
    with st.container():
        st.header("🗺️ Mappa delle tratte aeree da un aeroporto")

        # Seleziona l'aeroporto tramite un input di testo
        codice_aeroporto = st.text_input("Inserisci codice IATA dell'aeroporto di partenza (es. JFK)", value="JFK")

        if codice_aeroporto:
            # Crea la mappa con le tratte aeree
            mappa = crea_mappa_tratte(df, codice_aeroporto, aeroporti_info)

            # Visualizza la mappa in Streamlit
            st_folium(mappa, width=700, height=500)