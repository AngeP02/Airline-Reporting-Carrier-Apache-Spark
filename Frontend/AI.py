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
from pyspark.sql.functions import isnan, when, count, col
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import avg, count, sum

@st.cache_resource
def train_modello_cancellazione(_train_data):
    from pyspark.sql.functions import udf
    from pyspark.sql.types import DoubleType
    indexer = StringIndexer(inputCol="Cancelled", outputCol="label")
    assembler = VectorAssembler(inputCols=["Distance", "DepDelay", "CRSDepTime", "Month", "DayOfWeek"],
                                outputCol="features")
    label_counts = _train_data.groupBy("Cancelled").count().collect()
    total = sum([r["count"] for r in label_counts])
    class_weights = {r["Cancelled"]: total / r["count"] for r in label_counts}
    weight_udf = udf(lambda x: float(class_weights[x]), DoubleType())
    _train_data = _train_data.withColumn("weight", weight_udf(col("Cancelled")))
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=20, seed=42, weightCol="weight")
    pipeline = Pipeline(stages=[indexer, assembler, rf])
    return pipeline.fit(_train_data), assembler

@st.cache_resource
def train_modello_ritardo(_train_data):
    assembler = VectorAssembler(inputCols=["Distance", "CRSDepTime", "Month", "DayOfWeek"], outputCol="features")
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=False)
    lr = LinearRegression(featuresCol="scaledFeatures", labelCol="DepDelay")
    pipeline = Pipeline(stages=[assembler, scaler, lr])
    return pipeline.fit(_train_data), assembler

@st.cache_resource
def train_kmeans(_data_scaled, features_col="scaledFeatures"):
    kmeans = KMeans(k=4, seed=42, featuresCol=features_col)
    return kmeans.fit(_data_scaled)

@st.cache_resource
def clustered_to_pandas(_clustered_df):
    return _clustered_df.toPandas()

def valutazione_classificatore(model, test_data):
    predictions = model.transform(test_data)
    evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    return round(evaluator.evaluate(predictions), 3)

def mostra_importanza(modello_pipeline, assembler, titolo="Importanza delle feature"):
    modello_effettivo = modello_pipeline.stages[-1]
    if hasattr(modello_effettivo, "featureImportances"):
        importanza = modello_effettivo.featureImportances.toArray()
        feature_names = assembler.getInputCols()
        importance_df = pd.DataFrame({"feature": feature_names, "importance": importanza}).sort_values("importance",ascending=False)
        st.markdown(f"**{titolo}**")
        st.dataframe(importance_df)

def mostra_diagnostica_dati_cancellazione(df, dati_canc_all_diagnostics, train_dati_canc_diagnostics):
    st.subheader("Analisi preliminare valutazione voli")
    with st.expander("Dettagli analisi dati originali"):
        st.markdown("#### Analisi sulla variabile **Cancelled**")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown("DataFrame originale")
            if df is not None and "Cancelled" in df.columns:
                try:
                    original_counts_pd = df.groupBy("Cancelled").count().orderBy(col("Cancelled")).toPandas()
                    st.dataframe(original_counts_pd)
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
                    train_counts_pd = train_dati_canc_diagnostics.groupBy("Cancelled").count().orderBy(col("Cancelled")).toPandas()
                    st.dataframe(train_counts_pd)
                    st.markdown(f"*Numero totale di righe: {num_righe_train_dati_canc}*")
                except Exception as e:
                    st.error(f"Errore nel calcolare il bilanciamento per train_dati_canc_diagnostics: {e}")

        st.markdown("#### Statistiche Descrittive delle **feature** nel set di addestramento")
        if train_dati_canc_diagnostics is not None and train_dati_canc_diagnostics.count() > 0:
            try:
                features_to_check = ["Distance", "DepDelay", "CRSDepTime", "Month", "DayOfWeek"]
                actual_features_to_check = [f for f in features_to_check if f in train_dati_canc_diagnostics.columns]
                if not actual_features_to_check:
                    st.warning(f"Nessuna delle feature specificate ({', '.join(features_to_check)}) trovata.")
                else:
                    summary_pd = train_dati_canc_diagnostics.select(actual_features_to_check).summary("count", "mean", "stddev", "min", "max").toPandas()
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
        null_counts = df_cancellati.select(exprs)
        nulls_pd = null_counts.toPandas().T
        nulls_pd.columns = ['conteggio_mancante']
        nulls_pd["percentuale_mancante"] = (nulls_pd["conteggio_mancante"] / num_cancellati * 100).round(2)
        nulls_pd["percentuale_mancante"] = nulls_pd["percentuale_mancante"].astype(str) + '%'
        nulls_pd = nulls_pd[nulls_pd["conteggio_mancante"] > 0].sort_values("percentuale_mancante", ascending=False)
        st.markdown("Colonne con nulli tra i voli cancellati:")
        st.dataframe(nulls_pd)

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
        clustering_data_unscaled = assembler_cluster.transform(clustering_df_raw)

        scaler_cluster = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=False)
        scaler_model_cluster = scaler_cluster.fit(clustering_data_unscaled)
        clustering_data_scaled = scaler_model_cluster.transform(clustering_data_unscaled)

        model_canc, assembler_canc = train_modello_cancellazione(train_dati_canc)
        model_rit, assembler_rit = train_modello_ritardo(
            train_dati_rit)
        kmeans_model = train_kmeans(clustering_data_scaled, features_col="scaledFeatures")

        clustered = kmeans_model.transform(clustering_data_scaled).select("Distance", "DepDelay", "ArrDelay", "prediction")
        pd_clustered = clustered_to_pandas(clustered)
        with st.expander("Dettagli analisi dati dopo modifiche DataFrame"):
            st.markdown(f"**AUC del classificatore (su test set):** {valutazione_classificatore(model_canc, test_dati_canc)}")
            mostra_importanza(model_canc, assembler_canc, titolo="Importanza Feature")

    with st.container(border=True):
        st.subheader("Previsione cancellazione e ritardo volo")
        distanza_canc = st.slider("Distanza (miglia)", 0, 3000, 500, key="canc_distanza_v2")
        dep_delay_canc = st.slider("Ritardo partenza previsto in minuti (Per previsione cancellazione)", 0, 500, 0, key="canc_dep_delay_v2")
        col1_canc, col2_canc = st.columns(2)
        with col1_canc:
            ora_canc = st.slider("Ora schedulata", 0, 23, 12, key="canc_ora_v2")
        with col2_canc:
            minuti_canc = st.slider("Minuti schedulati", 0, 59, 0, key="canc_minuti_v2")
        crs_dep_canc = ora_canc * 100 + minuti_canc
        month_canc = st.slider("Mese", 1, 12, 6, key="canc_month_v2")
        dow_canc = st.slider("Giorno della settimana", 1, 7, 3, key="canc_dow_v2")
        input_df_canc = df.sparkSession.createDataFrame(
            [(float(distanza_canc), float(dep_delay_canc), float(crs_dep_canc), float(month_canc), float(dow_canc))],
            ["Distance", "DepDelay", "CRSDepTime", "Month", "DayOfWeek"]
        )
        prediction_canc_val = model_canc.transform(input_df_canc).select("prediction").first()["prediction"]
        risultato_canc = "Cancellato" if prediction_canc_val == 1.0 else "Non cancellato"
        st.markdown(f"**Risultato previsto:** {risultato_canc}")
        prediction_minuti = model_rit.transform(input_df_canc).select("prediction").first()["prediction"]
        prediction_minuti = max(0, round(prediction_minuti))  # Arrotonda e assicura non sia negativo
        input_prob_canc = df.sparkSession.createDataFrame(
            [(float(distanza_canc), float(prediction_minuti), float(crs_dep_canc), float(month_canc), float(dow_canc))],
            ["Distance", "DepDelay", "CRSDepTime", "Month", "DayOfWeek"]
        )
        probabilita_vector = model_canc.transform(input_prob_canc).select("probability").first()["probability"]
        probabilita_di_cancellazione = round(probabilita_vector[1] * 100, 2)
        rischio = "Basso"
        if probabilita_di_cancellazione > 70 and prediction_minuti > 60:
            rischio = "Altissimo (Ritardo >60min e cancellazione >70%)"
        elif probabilita_di_cancellazione > 70:
            rischio = "Alto (Cancellazione >70%)"
        elif prediction_minuti > 45:
            rischio = "Medio (Ritardo >45min)"
        elif probabilita_di_cancellazione > 30 or prediction_minuti > 20:
            rischio = "Medio-Basso (Cancellazione >30% o ritardo >20min)"
        st.markdown(f"**Ritardo stimato:** {prediction_minuti} minuti")
        st.markdown(f"**Probabilità di cancellazione (dato ritardo stimato):** {probabilita_di_cancellazione}%")
        st.markdown(f"**Livello di rischio combinato:** {rischio}")

    with st.container(border=True):
        st.subheader("Clustering delle tratte")
        sample_df = pd_clustered.sample(n=min(2000, len(pd_clustered)), random_state=42)
        cluster_stats = pd_clustered.groupby("prediction").mean(numeric_only=True).round(2)
        traduzioni_cluster_colonne = {
            "prediction": "Cluster",
            "Distance": "Distanza",
            "DepDelay": "RitardoPartenza",
            "ArrDelay": "RitardoArrivo"
        }
        cluster_stats.rename(columns=traduzioni_cluster_colonne, inplace=True)
        st.markdown("Statistiche per cluster")
        st.dataframe(cluster_stats)
        sample_df["ArrDelayAbs"] = sample_df["ArrDelay"].abs()
        sample_df["prediction"] = sample_df["prediction"].astype(str)
        fig1 = px.scatter(
            sample_df, x="Distance", y="DepDelay", color="prediction", size="ArrDelayAbs",
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
        sample = df.select("Reporting_Airline", "Origin", "Dest", "Distance", "DepDelay", "CRSDepTime", "Month",
                           "DayOfWeek") \
            .na.drop()
        sample_rit = model_rit.transform(sample)
        sample_canc_input = sample_rit.withColumnRenamed("prediction", "RitardoPrevisto")
        sample_canc_input = sample_canc_input.withColumn("DepDelay", col("RitardoPrevisto")) \
            .drop("features")
        sample_canc = model_canc.transform(sample_canc_input)
        extract_prob_udf = udf(lambda prob_vector: float(prob_vector[1]), DoubleType())
        sample_canc = sample_canc.withColumn("CancelProb", extract_prob_udf(col("probability")))
        tratte_rischio_ml = sample_canc.groupBy("Origin", "Dest") \
            .agg(
            avg("RitardoPrevisto").alias("RitardoStimato"),
            avg("CancelProb").alias("ProbCancellazioneStimata"),
            count("*").alias("NumCampioni")
        ).orderBy(desc("RitardoStimato"))
        tratte_pd = tratte_rischio_ml.withColumn("Tratta", F.concat_ws("-", "Origin", "Dest")).toPandas().round(2)
        st.markdown("#### Tratte con rischio più alto")
        st.dataframe(tratte_pd[["Tratta", "RitardoStimato", "ProbCancellazioneStimata", "NumCampioni"]].head(10))
        compagnie_rischio_ml = sample_canc.groupBy("Reporting_Airline") \
            .agg(
            avg("RitardoPrevisto").alias("RitardoStimato"),
            avg("CancelProb").alias("ProbCancellazioneStimata"),
            count("*").alias("NumCampioni")
        ).orderBy(desc("RitardoStimato"))
        compagnie_pd = compagnie_rischio_ml.toPandas().round(2)
        st.markdown("#### Compagnie con rischio più alto")
        st.dataframe(compagnie_pd.head(10))