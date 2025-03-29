from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

# Configurazione ambiente
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["SPARK_HOME"] = "C:\\spark"

# Creazione della sessione Spark
spark = SparkSession.builder \
    .appName("ReportEsteso") \
    .config("spark.driver.memory", "8g") \
    .master("local[*]") \
    .getOrCreate()

# Percorso del file da analizzare
file_path = "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Dati/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_1.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)


# Funzione per rilevare se una colonna è una data
def detect_date_format(df, col):
    sample_value = df.select(col).dropna().limit(1).collect()
    if sample_value:
        sample_value = str(sample_value[0][col])
        if "-" in sample_value and len(sample_value) in [10, 19]:  # Formati YYYY-MM-DD o YYYY-MM-DD HH:MM:SS
            return "YYYY-MM-DD" if len(sample_value) == 10 else "YYYY-MM-DD HH:MM:SS"
        elif "/" in sample_value:
            return "MM/DD/YYYY"  # Possibile formato USA
    return "N/A"


# Generazione del report con informazioni estese
report_path = "report_generale.txt"
with open(report_path, "a") as f:
    f.write(f"\n=== Informazioni dettagliate per il file: {file_path} ===\n\n")
    total_count = df.count()
    f.write(f"Numero totale di righe: {total_count}\n\n")

    # Analisi delle colonne
    for col in df.columns:
        col_type = str(df.schema[col].dataType)  # Tipo di dato della colonna
        date_format = detect_date_format(df, col) if "date" in col.lower() else "N/A"
        #cond_null = F.col(col).isNull() | (F.col(col) == "")
        #null_count = df.filter(cond_null).count()
        #non_null_count = total_count - null_count
        #perc_null = (null_count / total_count * 100) if total_count > 0 else 0
        #unique_count = df.select(col).distinct().count()  # Valori unici

        f.write(
            f"Colonna '{col}': tipo={col_type}, formato data={date_format}\n")

print(f"Informazioni aggiunte al report: {report_path}")

# Chiude la sessione Spark
spark.stop()
