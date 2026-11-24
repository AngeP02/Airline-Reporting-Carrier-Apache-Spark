# Progetto Voli Big Data

Dashboard interattiva per l'analisi del traffico aereo domestico negli Stati Uniti nel 2013, sviluppata con Apache Spark e Streamlit.

Il progetto nasce come elaborato per il corso di **Modelli e Tecniche per Big Data**. L'obiettivo è analizzare il dataset **Airline Reporting Carrier On-Time Performance** del Bureau of Transportation Statistics, che contiene oltre **6 milioni di voli** effettuati nel 2013 da compagnie aeree statunitensi.

Dato il volume dei dati, le tecnologie classiche non bastano, per questo il progetto usa Apache Spark, che elabora tutto in parallelo senza campionamenti o riduzioni del dataset.

## Dataset

**Fonte:** [Bureau of Transportation Statistics](https://transtats.bts.gov/DatabaseInfo.asp?QO_VQ=EFD&DB_URL=) — anno 2013

Il dataset si compone di 12 file CSV (uno per mese), per un totale di oltre 6,3 milioni di righe. Ogni riga rappresenta un singolo volo e include:
- orari schedulati e reali di partenza/arrivo
- aeroporto di origine e destinazione
- compagnia aerea
- ritardi e relative cause (meteo, NAS, sicurezza, compagnia, aeromobile)
- cancellazioni e deviazioni

### Stack tecnologico

| Tecnologia | Uso |
|---|---|
| **PySpark** | Elaborazione distribuita del dataset |
| **MLlib** | Modelli predittivi (classificazione e regressione) |
| **GraphFrames** | Analisi della rete aeroportuale come grafo |
| **Streamlit** | Dashboard interattiva |
| **Pandas** | Conversione risultati per la visualizzazione |
| **PyDeck** | Mappe interattive geospaziali |


### Preprocessing

Prima di lanciare la dashboard è necessario eseguire il preprocessing. I due script si occupano di:

1. **`AnalisiDatasets.py`** — legge tutti e 12 i CSV in un unico DataFrame e genera un report dettagliato su valori nulli, struttura delle colonne e anomalie (es. colonna `_c109` malformata).
2. **`Preprocessing.py`** — sulla base del report, elimina le colonne interamente nulle, le righe vuote e la colonna anomala. Produce 12 CSV "puliti" pronti per l'analisi.

### Avvio della dashboard

Al primo avvio i dati vengono convertiti in formato **Parquet** e caricati in cache con `persist(MEMORY_ONLY)` — le sessioni successive partono molto più velocemente.

La sessione Spark è configurata per girare in locale con 18 GB di memoria driver, shuffle ottimizzato (8 partizioni) e join broadcast automatico fino a 100 MB.

## Sezioni della dashboard

### Home
Panoramica generale: KPI principali (totale voli, ritardo medio, % cancellazioni), top 5 compagnie per voli/puntualità/cancellazioni, top 5 aeroporti più ritardatari, top 5 rotte, e una mappa interattiva degli aeroporti dove dimensione = volume voli e colore = ritardo medio.

### Aeroporto
Analisi dettagliata per singolo aeroporto: statistiche di traffico, distribuzione dei ritardi, confronto con la media nazionale.

### Compagnie aeree
Efficienza e affidabilità per compagnia: puntualità, percentuale di cancellazioni, trend temporali.

### Città
Riepilogo delle statistiche aggregate per città di origine/destinazione.

### Ritardi e cancellazioni
Analisi delle cause di ritardo (meteo, NAS, sicurezza, compagnia, aeromobile) con trend mensili e settimanali, distribuzione per fascia oraria.

### Tratte aeree
Le rotte più trafficate e quelle con più problemi. Include una mappa interattiva dove lo spessore degli archi è proporzionale al numero di voli sulla tratta. Selezionando una tratta si ottengono le statistiche dettagliate (compagnie, mesi di punta, bidirezionalità).

### Previsioni AI
Tre modelli predittivi sviluppati con MLlib:
- **Random Forest** per la classificazione del rischio di cancellazione (AUC: 0.961). Il dataset era fortemente sbilanciato (96k cancellati su 6,3M voli), quindi è stata applicata una pesatura inversa delle classi.
- **Regressione lineare** per stimare i minuti di ritardo alla partenza, con normalizzazione via StandardScaler.
- **KMeans clustering** (4 cluster) per raggruppare voli simili: voli brevi e puntuali, voli medi con ritardi moderati, voli lunghi con ritardi significativi, casi anomali ad alta variabilità.

L'interfaccia permette di inserire i parametri di un volo (distanza, ora schedulata, mese, giorno) e ottenere in tempo reale la probabilità di cancellazione, il ritardo stimato e un livello di rischio combinato.

### Grafi
La rete aeroportuale modellata come grafo orientato (aeroporti = nodi, tratte = archi). Metriche calcolate con GraphFrames:
- **PageRank** — identifica gli hub principali (ATL, ORD e DFW i più importanti)
- **Componenti connesse e fortemente connesse** — verifica la connettività globale della rete
- **Label Propagation** — individua cluster regionali di aeroporti
- **In-Degree / Out-Degree** — aeroporti specializzati come origine o destinazione
- **Triangle Count** — misura la ridondanza e la fault tolerance della rete

È possibile simulare la rimozione di un aeroporto per vedere come cambia la struttura della rete, utile per valutare la resilienza del sistema in caso di chiusura temporanea.
