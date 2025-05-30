import streamlit as st
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.regression import LinearRegression
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
import plotly.express as px

@st.cache_resource
def train_modello_cancellazione(_dati):
    indexer = StringIndexer(inputCol="Cancelled", outputCol="label")
    assembler = VectorAssembler(inputCols=["Distance", "DepDelay", "CRSDepTime", "Month", "DayOfWeek"],
                                outputCol="features")
    lr = LogisticRegression(maxIter=10)
    pipeline = Pipeline(stages=[indexer, assembler, lr])
    return pipeline.fit(_dati)

@st.cache_resource
def train_modello_ritardo(_dati):
    assembler = VectorAssembler(inputCols=["Distance", "CRSDepTime", "Month", "DayOfWeek"], outputCol="features")
    lr = LinearRegression(featuresCol="features", labelCol="DepDelay")
    pipeline = Pipeline(stages=[assembler, lr])
    return pipeline.fit(_dati)

@st.cache_resource
def train_kmeans(_data):
    kmeans = KMeans(k=4, seed=42)
    return kmeans.fit(_data)

@st.cache_resource
def clustered_to_pandas(_clustered_df):
    return _clustered_df.toPandas()

@st.cache_resource
def anomalies_to_pandas(_df):
    return _df.limit(1000).toPandas()

def mostra_ai(df):
    st.title("Analisi Predittiva con AI")

    dati_canc = df.select("Cancelled", "Distance", "DepDelay", "CRSDepTime", "Month", "DayOfWeek").na.drop()
    dati_rit = df.select("DepDelay", "Distance", "CRSDepTime", "Month", "DayOfWeek").na.drop()
    dati_rit = dati_rit.withColumn("DepDelay", col("DepDelay").cast("double"))
    clustering_df = df.select("Distance", "DepDelay", "ArrDelay").na.drop()

    model_canc = train_modello_cancellazione(dati_canc)
    model_rit = train_modello_ritardo(dati_rit)
    assembler = VectorAssembler(inputCols=["Distance", "DepDelay", "ArrDelay"], outputCol="features")
    clustering_data = assembler.transform(clustering_df)
    kmeans_model = train_kmeans(clustering_data)
    clustered = kmeans_model.transform(clustering_data).select("Distance", "DepDelay", "ArrDelay", "prediction")
    pd_clustered = clustered_to_pandas(clustered)

    with st.container(border=True):
        st.subheader("Previsione cancellazione volo")
        st.markdown("Inserisci parametri del volo")
        distanza = st.slider("Distanza (miglia)", 0, 3000, 500, key="canc_distanza")
        dep_delay = st.slider("Ritardo partenza previsto (minuti)", 0, 500, 0, key="canc_dep_delay")
        col1, col2 = st.columns(2)
        with col1:
            ora = st.slider("Ora schedulata (HH)", 0, 23, 12, key="canc_ora")
        with col2:
            minuti = st.slider("Minuti schedulati (MM)", 0, 59, 0, key="canc_minuti")
        crs_dep = ora * 100 + minuti
        month = st.slider("Mese", 1, 12, 6, key="canc_month")
        dow = st.slider("Giorno della settimana", 1, 7, 3, key="canc_dow")

        input_df = df.sparkSession.createDataFrame(
            [(float(distanza), float(dep_delay), float(crs_dep), float(month), float(dow))],
            ["Distance", "DepDelay", "CRSDepTime", "Month", "DayOfWeek"]
        )
        prediction = model_canc.transform(input_df).select("prediction").first()["prediction"]
        risultato = "Cancellato" if prediction == 1.0 else "Non cancellato"
        st.success(f"Risultato previsto: **{risultato}**")

    with st.container(border=True):
        st.subheader("Previsione ritardo in partenza")
        distanza = st.slider("Distanza (miglia)", 0, 3000, 500, key="rit_distanza")
        col1, col2 = st.columns(2)
        with col1:
            ora = st.slider("Ora schedulata (HH)", 0, 23, 12, key="canc_ora_rit")
        with col2:
            minuti = st.slider("Minuti schedulati (MM)", 0, 59, 0, key="canc_minuti_rit")
        crs_dep = ora * 100 + minuti
        month = st.slider("Mese", 1, 12, 6, key="rit_month")
        dow = st.slider("Giorno della settimana", 1, 7, 3, key="rit_dow")

        input_df = df.sparkSession.createDataFrame(
            [(float(distanza), float(crs_dep), float(month), float(dow))],
            ["Distance", "CRSDepTime", "Month", "DayOfWeek"]
        )
        prediction = model_rit.transform(input_df).select("prediction").first()["prediction"]
        st.success(f"Ritardo previsto: **{round(prediction, 2)} minuti**")

    with st.container(border=True):
        st.subheader("Clustering delle tratte")
        sample_df = pd_clustered.sample(n=min(2000, len(pd_clustered)), random_state=42)

        cluster_stats = pd_clustered.groupby("prediction").mean(numeric_only=True).round(2)
        st.markdown("**Statistiche per cluster:**")
        st.dataframe(cluster_stats)

        # Crea una nuova colonna con i valori assoluti del ritardo all'arrivo
        sample_df["ArrDelayAbs"] = sample_df["ArrDelay"].abs()

        # Usa la nuova colonna per la dimensione dei punti
        fig1 = px.scatter(
            sample_df, x="Distance", y="DepDelay", color="prediction", size="ArrDelayAbs",
            labels={"Distance": "Distanza (miglia)", "DepDelay": "Ritardo Partenza (min)",
                    "ArrDelay": "Ritardo Arrivo (min)", "prediction": "Cluster"},
            title="Cluster dei voli"
        )

        st.plotly_chart(fig1, use_container_width=True)
