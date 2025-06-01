import streamlit as st
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.regression import LinearRegression
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import desc
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import plotly.express as px
import pandas as pd
from pyspark.sql.functions import udf
from pyspark.sql.functions import isnan, when, col
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import avg, count

@st.cache_resource
def train_modello_cancellazione(_dati_addestramento):
    from pyspark.sql.functions import udf
    from pyspark.sql.types import DoubleType
    indicizzatore_etichetta = StringIndexer(inputCol="Cancelled", outputCol="label")
    vettore_caratteristiche = VectorAssembler(inputCols=["Distance", "DepDelay", "CRSDepTime", "Month", "DayOfWeek"],
                                outputCol="features")
    conteggio_classi = _dati_addestramento.groupBy("Cancelled").count().collect()
    totale_voli = sum([r["count"] for r in conteggio_classi])
    pesi_classi = {r["Cancelled"]: totale_voli / r["count"] for r in conteggio_classi}
    calcola_peso = udf(lambda x: float(pesi_classi[x]), DoubleType())
    _dati_addestramento = _dati_addestramento.withColumn("weight", calcola_peso(col("Cancelled")))
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=20, seed=42, weightCol="weight")
    pipeline = Pipeline(stages=[indicizzatore_etichetta, vettore_caratteristiche, rf])
    return pipeline.fit(_dati_addestramento), vettore_caratteristiche

@st.cache_resource
def train_modello_ritardo(_dati_addestramento):
    assembler = VectorAssembler(inputCols=["Distance", "CRSDepTime", "Month", "DayOfWeek"], outputCol="features")
    scalare = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=False)
    lr = LinearRegression(featuresCol="scaledFeatures", labelCol="DepDelay")
    pipeline = Pipeline(stages=[assembler, scalare, lr])
    return pipeline.fit(_dati_addestramento), assembler

@st.cache_resource
def train_kmeans(_dati_scalati, features_col="scaledFeatures"):
    kmeans = KMeans(k=4, seed=42, featuresCol=features_col)
    return kmeans.fit(_dati_scalati)

@st.cache_resource
def clustered_to_pandas(_cluster_df):
    return _cluster_df.toPandas()

@st.cache_resource
def valutazione_classificatore(_modello, _dati_test):
    predizioni = _modello.transform(_dati_test)
    evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    return round(evaluator.evaluate(predizioni), 3)

def mostra_importanza(modello_pipeline, assembler):
    modello_effettivo = modello_pipeline.stages[-1]
    if hasattr(modello_effettivo, "featureImportances"):
        importanza = modello_effettivo.featureImportances.toArray()
        nomi_feature = assembler.getInputCols()
        df_importance = pd.DataFrame({"feature": nomi_feature, "importance": importanza}).sort_values("importance",ascending=False)
        st.markdown("Importanza delle feature")
        st.dataframe(df_importance)

def mostra_diagnostica_dati_cancellazione(df, dati_canc_all_diagnostics, train_dati_canc_diagnostics):
    st.subheader("Analisi preliminare valutazione voli")
    with st.expander("Dettagli analisi dati originali"):
        st.markdown("#### Analisi sulla variabile **Cancelled**")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown("DataFrame originale")
            if df is not None and "Cancelled" in df.columns:
                try:
                    conteggio_pd_originale = df.groupBy("Cancelled").count().orderBy(col("Cancelled")).toPandas()
                    st.dataframe(conteggio_pd_originale)
                    st.markdown(f"*Numero totale di righe: {df.count()}*")
                except Exception as e:
                    st.error(f"Errore nel calcolare il bilanciamento per df: {e}")
        with col2:
            st.markdown("Dataset dopo i filtri **dropna**")
            if dati_canc_all_diagnostics is not None and "Cancelled" in dati_canc_all_diagnostics.columns:
                try:
                    num_righe_dati_canc_all = dati_canc_all_diagnostics.count()
                    dati_canc_all_counts_pd = dati_canc_all_diagnostics.groupBy("Cancelled").count().orderBy(col("Cancelled")).toPandas()
                    st.dataframe(dati_canc_all_counts_pd)
                    st.markdown(f"*Numero totale di righe: {num_righe_dati_canc_all}*")
                except Exception as e:
                    st.error(f"Errore nel calcolare il bilanciamento per dati_canc_all_diagnostics: {e}")
        with col3:
            st.markdown("Dataset di addestramento effettivo")
            if train_dati_canc_diagnostics is not None and "Cancelled" in train_dati_canc_diagnostics.columns:
                try:
                    num_righe_train_dati_canc = train_dati_canc_diagnostics.count()
                    train_df_conteggio = train_dati_canc_diagnostics.groupBy("Cancelled").count().orderBy(col("Cancelled")).toPandas()
                    st.dataframe(train_df_conteggio)
                    st.markdown(f"*Numero totale di righe: {num_righe_train_dati_canc}*")
                except Exception as e:
                    st.error(f"Errore nel calcolare il bilanciamento per train_dati_canc_diagnostics: {e}")

        st.markdown("#### Statistiche Descrittive delle **feature** nel set di addestramento")
        if train_dati_canc_diagnostics is not None and train_dati_canc_diagnostics.count() > 0:
            try:
                feature_da_verificare = ["Distance", "DepDelay", "CRSDepTime", "Month", "DayOfWeek"]
                feature_attuali_da_verificare = [f for f in feature_da_verificare if f in train_dati_canc_diagnostics.columns]
                if not feature_attuali_da_verificare:
                    st.warning(f"Nessuna delle feature specificate ({', '.join(feature_da_verificare)}) trovata.")
                else:
                    summary_pd = train_dati_canc_diagnostics.select(feature_attuali_da_verificare).summary("count", "mean", "stddev", "min", "max").toPandas()
                    traduzioni_colonne = {
                        "summary": "Statistica",
                        "Distance": "Distanza",
                        "DepDelay": "RitardoPartenza",
                        "CRSDepTime": "OraPartenzaSched",
                        "Month": "Mese",
                        "DayOfWeek": "GiornoSettimana"
                    }
                    summary_pd.rename(columns=traduzioni_colonne, inplace=True)
                    traduzioni_valori = {
                        "count": "Conteggio",
                        "mean": "Media",
                        "stddev": "Dev. Standard",
                        "min": "Minimo",
                        "max": "Massimo"
                    }
                    summary_pd["Statistica"] = summary_pd["Statistica"].replace(traduzioni_valori)
                    st.dataframe(summary_pd)
            except Exception as e:
                st.error(f"Errore nel calcolare le statistiche descrittive: {e}")
        else:
            st.warning("DataFrame 'train_dati_canc_diagnostics' vuoto o non fornito per le statistiche.")

        st.markdown("#### Analisi nulli sui voli cancellati")
        df_cancellati = df.filter(col("Cancelled") == 1)
        num_cancellati = df_cancellati.count()
        if num_cancellati == 0:
            st.warning("Nessun volo cancellato per analisi NA.")
            return
        else:
            st.markdown(f"Totale voli cancellati: **{num_cancellati}**")
        schema = dict(df.dtypes)
        exprs = []
        for c in df.columns:
            dtype = schema[c]
            if dtype in ["float", "double"]:
                exprs.append(count(when(col(c).isNull() | isnan(col(c)), c)).alias(c))
            else:
                exprs.append(count(when(col(c).isNull(), c)).alias(c))
        conteggio_nulli = df_cancellati.select(exprs)
        nulli_pd = conteggio_nulli.toPandas().T
        nulli_pd.columns = ['conteggio_mancante']
        nulli_pd["percentuale_mancante"] = (nulli_pd["conteggio_mancante"] / num_cancellati * 100).round(2)
        nulli_pd["percentuale_mancante"] = nulli_pd["percentuale_mancante"].astype(str) + '%'
        nulli_pd = nulli_pd[nulli_pd["conteggio_mancante"] > 0].sort_values("percentuale_mancante", ascending=False)
        st.markdown("Colonne con nulli tra i voli cancellati:")
        st.dataframe(nulli_pd)

def mostra_ai(df):
    st.title("Analisi Previsioni AI")
    with st.container(border=True):
        colonne_cancellazione = ["Cancelled", "Distance", "DepDelay", "CRSDepTime", "Month", "DayOfWeek"]
        colonne_esistenti_canc = [c for c in colonne_cancellazione if c in df.columns]
        dati_canc_all = df.select(colonne_esistenti_canc).na.drop()
        train_dati_canc, test_dati_canc = dati_canc_all.randomSplit([0.8, 0.2], seed=42)

        mostra_diagnostica_dati_cancellazione(df, dati_canc_all, train_dati_canc)

        dati_canc_all = df.select("Cancelled", "Distance", "DepDelay", "CRSDepTime", "Month", "DayOfWeek") \
            .withColumn("DepDelay", when(col("DepDelay").isNull(), -1).otherwise(col("DepDelay"))) \
            .dropna()
        train_dati_canc, test_dati_canc = dati_canc_all.randomSplit([0.8, 0.2], seed=42)

        dati_rit_all = df.select("DepDelay", "Distance", "CRSDepTime", "Month", "DayOfWeek").na.drop()
        dati_rit_all = dati_rit_all.withColumn("DepDelay", col("DepDelay").cast("double"))
        train_dati_rit, test_dati_rit = dati_rit_all.randomSplit([0.8, 0.2], seed=42)

        train_dati_canc.cache()
        test_dati_canc.cache()
        train_dati_rit.cache()
        test_dati_rit.cache()

        clustering_df_raw = df.select("Distance", "DepDelay", "ArrDelay").na.drop()
        assembler_cluster = VectorAssembler(inputCols=["Distance", "DepDelay", "ArrDelay"], outputCol="features")
        clustering_data_non_scalati = assembler_cluster.transform(clustering_df_raw)

        scaler_cluster = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=False)
        scaler_model_cluster = scaler_cluster.fit(clustering_data_non_scalati)
        clustering_data_scaled = scaler_model_cluster.transform(clustering_data_non_scalati)

        model_canc, assembler_canc = train_modello_cancellazione(train_dati_canc)
        model_rit, assembler_rit = train_modello_ritardo(
            train_dati_rit)
        kmeans_model = train_kmeans(clustering_data_scaled, features_col="scaledFeatures")

        clustered = kmeans_model.transform(clustering_data_scaled).select("Distance", "DepDelay", "ArrDelay", "prediction")
        pd_clustered = clustered_to_pandas(clustered)
        with st.expander("Dettagli analisi dati dopo modifiche DataFrame"):
            st.markdown(f"**AUC del classificatore (su test set):** {valutazione_classificatore(model_canc, test_dati_canc)}")
            mostra_importanza(model_canc, assembler_canc)

    with st.container(border=True):
        st.subheader("Previsione cancellazione e ritardo volo")
        distanza_canc = st.slider("Distanza (miglia)", 0, 3000, 500, key="canc_distanza_v2")
        ritardo_canc = st.slider("Ritardo partenza previsto in minuti (Per previsione cancellazione)", 0, 500, 0, key="canc_dep_delay_v2")
        col1_canc, col2_canc = st.columns(2)
        with col1_canc:
            ora_canc = st.slider("Ora schedulata", 0, 23, 12, key="canc_ora_v2")
        with col2_canc:
            minuti_canc = st.slider("Minuti schedulati", 0, 59, 0, key="canc_minuti_v2")
        crs_dep_canc = ora_canc * 100 + minuti_canc
        mesi_canc = st.slider("Mese", 1, 12, 6, key="canc_month_v2")
        sett_canc = st.slider("Giorno della settimana", 1, 7, 3, key="canc_dow_v2")
        input_df_canc = df.sparkSession.createDataFrame(
            [(float(distanza_canc), float(ritardo_canc), float(crs_dep_canc), float(mesi_canc), float(sett_canc))],
            ["Distance", "DepDelay", "CRSDepTime", "Month", "DayOfWeek"]
        )
        prediction_canc_val = model_canc.transform(input_df_canc).select("prediction").first()["prediction"]
        risultato_canc = "Cancellato" if prediction_canc_val == 1.0 else "Non cancellato"
        st.markdown(f"**Risultato previsto:** {risultato_canc}")
        predizione_minuti = model_rit.transform(input_df_canc).select("prediction").first()["prediction"]
        predizione_minuti = max(0, round(predizione_minuti))  # Arrotonda e assicura non sia negativo
        input_prob_canc = df.sparkSession.createDataFrame(
            [(float(distanza_canc), float(predizione_minuti), float(crs_dep_canc), float(mesi_canc), float(sett_canc))],
            ["Distance", "DepDelay", "CRSDepTime", "Month", "DayOfWeek"]
        )
        probabilita_vector = model_canc.transform(input_prob_canc).select("probability").first()["probability"]
        probabilita_di_cancellazione = round(probabilita_vector[1] * 100, 2)
        rischio = "Basso"
        if probabilita_di_cancellazione > 70 and predizione_minuti > 60:
            rischio = "Altissimo (Ritardo >60min e cancellazione >70%)"
        elif probabilita_di_cancellazione > 70:
            rischio = "Alto (Cancellazione >70%)"
        elif predizione_minuti > 45:
            rischio = "Medio (Ritardo >45min)"
        elif probabilita_di_cancellazione > 30 or predizione_minuti > 20:
            rischio = "Medio-Basso (Cancellazione >30% o ritardo >20min)"
        st.markdown(f"**Ritardo stimato:** {predizione_minuti} minuti")
        st.markdown(f"**Probabilità di cancellazione (dato ritardo stimato):** {probabilita_di_cancellazione}%")
        st.markdown(f"**Livello di rischio combinato:** {rischio}")

    with st.container(border=True):
        st.subheader("Clustering delle tratte")
        df_campionato = pd_clustered.sample(n=min(2000, len(pd_clustered)), random_state=42)
        statistiche_cluster = pd_clustered.groupby("prediction").mean(numeric_only=True).round(2)
        traduzioni_cluster_colonne = {
            "prediction": "Cluster",
            "Distance": "Distanza",
            "DepDelay": "RitardoPartenza",
            "ArrDelay": "RitardoArrivo"
        }
        statistiche_cluster.rename(columns=traduzioni_cluster_colonne, inplace=True)
        st.markdown("Statistiche per cluster")
        st.dataframe(statistiche_cluster)
        df_campionato["ArrDelayAbs"] = df_campionato["ArrDelay"].abs()
        df_campionato["prediction"] = df_campionato["prediction"].astype(str)
        fig1 = px.scatter(
            df_campionato, x="Distance", y="DepDelay", color="prediction", size="ArrDelayAbs",
            color_discrete_sequence=px.colors.qualitative.Plotly,
            labels={"Distance": "Distanza (miglia)", "DepDelay": "Ritardo Partenza (min)",
                    "ArrDelayAbs": "Ritardo Arrivo Assoluto (min)", "prediction": "Cluster"},
            title="Cluster dei voli (campione)"
        )
        st.plotly_chart(fig1, use_container_width=True)


    with st.container(border=True):
        st.subheader("Distribuzione ritardi in partenza (sul set di test)")
        ritardi_test_df = test_dati_rit.select("DepDelay").toPandas()
        fig2 = px.histogram(ritardi_test_df, x="DepDelay", nbins=50, title="Distribuzione Ritardi Partenza (Test Set)")
        st.plotly_chart(fig2, use_container_width=True)

    with st.container(border=True):
        st.subheader("Analisi AI: Previsione rischio per tratte e compagnie")
        campione = df.select("Reporting_Airline", "Origin", "Dest", "Distance", "DepDelay", "CRSDepTime", "Month",
                           "DayOfWeek") \
            .na.drop()
        campione_rit = model_rit.transform(campione)
        campione_canc_input = campione_rit.withColumnRenamed("prediction", "RitardoPrevisto")
        campione_canc_input = campione_canc_input.withColumn("DepDelay", col("RitardoPrevisto")) \
            .drop("features")
        campione_canc = model_canc.transform(campione_canc_input)
        extra_prob_udf = udf(lambda prob_vector: float(prob_vector[1]), DoubleType())
        campione_canc = campione_canc.withColumn("CancelProb", extra_prob_udf(col("probability")))
        tratte_rischio_ml = campione_canc.groupBy("Origin", "Dest") \
            .agg(
            avg("RitardoPrevisto").alias("RitardoStimato"),
            avg("CancelProb").alias("ProbCancellazioneStimata"),
            count("*").alias("NumCampioni")
        ).orderBy(desc("RitardoStimato"))
        tratte_pd = tratte_rischio_ml.withColumn("Tratta", F.concat_ws("-", "Origin", "Dest")).toPandas().round(2)
        st.markdown("#### Tratte con rischio più alto")
        st.dataframe(tratte_pd[["Tratta", "RitardoStimato", "ProbCancellazioneStimata", "NumCampioni"]].head(10))
        compagnie_rischio_ml = campione_canc.groupBy("Reporting_Airline") \
            .agg(
            avg("RitardoPrevisto").alias("RitardoStimato"),
            avg("CancelProb").alias("ProbCancellazioneStimata"),
            count("*").alias("NumCampioni")
        ).orderBy(desc("RitardoStimato"))
        compagnie_pd = compagnie_rischio_ml.toPandas().round(2)
        st.markdown("#### Compagnie con rischio più alto")
        st.dataframe(compagnie_pd.head(10))