import streamlit as st
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.regression import LinearRegression
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col
from pyspark.ml import Pipeline

def mostra_ai(df):
    st.title("Analisi Predittiva con AI")


    with st.container(border=True):
        st.subheader("Previsione cancellazione volo")
        dati = df.select("Cancelled", "Distance", "DepDelay", "CRSDepTime", "Month", "DayOfWeek") \
            .na.drop()

        indexer = StringIndexer(inputCol="Cancelled", outputCol="label")
        assembler = VectorAssembler(inputCols=["Distance", "DepDelay", "CRSDepTime", "Month", "DayOfWeek"],
                                    outputCol="features")
        lr = LogisticRegression(maxIter=10)

        pipeline = Pipeline(stages=[indexer, assembler, lr])
        model = pipeline.fit(dati)

        st.markdown("Simula volo")
        distanza = st.slider("Distanza (miglia)", 0, 3000, 500, key="canc_distanza")
        dep_delay = st.slider("Ritardo partenza previsto (minuti)", 0, 500, 0, key="canc_dep_delay")
        crs_dep = st.slider("Orario schedulato (HHMM)", 0, 2400, 1200, key="canc_crs_dep")
        month = st.slider("Mese", 1, 12, 6, key="canc_month")
        dow = st.slider("Giorno della settimana", 1, 7, 3, key="canc_dow")

        input_df = df.sparkSession.createDataFrame(
            [(float(distanza), float(dep_delay), float(crs_dep), float(month), float(dow))],
            ["Distance", "DepDelay", "CRSDepTime", "Month", "DayOfWeek"]
        )
        prediction = model.transform(input_df).select("prediction").first()["prediction"]
        risultato = "Cancellato" if prediction == 1.0 else "Non cancellato"
        st.success(f"Risultato previsto: **{risultato}**")

    with st.container(border=True):
        st.subheader("Previsione ritardo in partenza")
        dati = df.select("DepDelay", "Distance", "CRSDepTime", "Month", "DayOfWeek").na.drop()
        dati = dati.withColumn("DepDelay", col("DepDelay").cast("double"))

        assembler = VectorAssembler(inputCols=["Distance", "CRSDepTime", "Month", "DayOfWeek"], outputCol="features")
        lr = LinearRegression(featuresCol="features", labelCol="DepDelay")

        pipeline = Pipeline(stages=[assembler, lr])
        model = pipeline.fit(dati)

        distanza = st.slider("Distanza (miglia)", 0, 3000, 500, key="rit_distanza")
        crs_dep = st.slider("Orario schedulato (HHMM)", 0, 2400, 1200, key="rit_crs_dep")
        month = st.slider("Mese", 1, 12, 6, key="rit_month")
        dow = st.slider("Giorno della settimana", 1, 7, 3, key="rit_dow")

        input_df = df.sparkSession.createDataFrame(
            [(float(distanza), float(crs_dep), float(month), float(dow))],
            ["Distance", "CRSDepTime", "Month", "DayOfWeek"]
        )
        prediction = model.transform(input_df).select("prediction").first()["prediction"]
        st.success(f"Ritardo previsto: **{round(prediction, 2)} minuti**")

    with st.container(border=True):
        st.subheader("Clustering delle tratte: Distanza vs Ritardo Partenza")

        tratte_df = df.select("Distance", "DepDelay", "ArrDelay").na.drop()
        assembler = VectorAssembler(inputCols=["Distance", "DepDelay", "ArrDelay"], outputCol="features")
        data = assembler.transform(tratte_df)

        kmeans = KMeans(k=4, seed=42)
        model = kmeans.fit(data)
        clustered = model.transform(data).select("Distance", "DepDelay", "ArrDelay", "prediction")
        pd_clustered = clustered.toPandas()

        import plotly.express as px
        fig = px.scatter(
            pd_clustered.sample(n=2000),
            x="Distance", y="DepDelay",
            color="prediction",
            labels={
                "Distance": "Distanza (miglia)",
                "DepDelay": "Ritardo Partenza (min)",
                "prediction": "Cluster"
            },
        )
        st.plotly_chart(fig, use_container_width=True)

    with st.container(border=True):
        st.subheader("Rilevamento anomalie (ritardi sospetti)")
        anomalies_df = df.select("DepDelay", "ArrDelay", "Distance") \
            .na.drop() \
            .filter((col("DepDelay") > 180) | (col("ArrDelay") > 180))

        anomalies_pd = anomalies_df.limit(1000).toPandas()

        import plotly.express as px
        fig = px.scatter(
            anomalies_pd, x="Distance", y="DepDelay",
            color="ArrDelay", color_continuous_scale="reds",
        )
        st.plotly_chart(fig, use_container_width=True)
