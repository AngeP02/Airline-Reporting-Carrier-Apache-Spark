from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# configurazione ambiente (aggiorna i percorsi se necessario)
import os

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["SPARK_HOME"] = "C:\\spark"

# Creazione della sessione Spark (sul driver)
spark = SparkSession.builder \
    .appName("AnalisiDatiVoli") \
    .config("spark.driver.memory", "24g") \
    .config("spark.sql.debug.maxToStringFields", "1000") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .master("local[*]") \
    .getOrCreate()

print("PYSPARK_PYTHON:", os.environ.get("PYSPARK_PYTHON"))
print("Spark Version:", spark.version)

# Lista dei file da processare
nomi_file = [
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Dati/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_1.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Dati/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_2.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Dati/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_3.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Dati/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_4.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Dati/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_5.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Dati/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_6.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Dati/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_7.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Dati/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_8.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Dati/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_9.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Dati/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_10.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Dati/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_11.csv",
    "C:/Users/ANGELICA/Desktop/ANGELICA/UNICAL/MAGISTRALE/I ANNO/PRIMO SEMESTRE/MODELLI E TECNICHE PER BIG DATA/PROGETTO/CODICE/Dati/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_12.csv",
]

# Legge tutti i file in un unico DataFrame
# L'opzione input_file_name() aggiunge una colonna con il nome del file da cui provengono le righe
df_all = spark.read.csv(nomi_file, header=True, inferSchema=True) \
    .withColumn("file", F.input_file_name())

# Recupera la lista dei file effettivamente letti (questo avviene sul driver)
file_list = [row.file for row in df_all.select("file").distinct().collect()]
print("File letti:", file_list)


# Funzione per generare il report per ogni file
def report_per_file(df, file_name):
    df_file = df.filter(F.col("file") == file_name)
    total_count = df_file.count()
    print(f"\n--- Report per il file: {file_name} ---")
    print(f"Numero totale di righe: {total_count}")

    # Report per ogni colonna (escludendo la colonna 'file' aggiunta)
    report = {}
    for col in [c for c in df_file.columns if c != "file"]:
        # Consideriamo anche stringhe vuote come null se necessario
        cond_null = F.col(col).isNull() | (F.col(col) == "")
        null_count = df_file.filter(cond_null).count()
        non_null_count = total_count - null_count
        perc_null = (null_count / total_count * 100) if total_count > 0 else 0
        report[col] = {"nulli": null_count, "non_nulli": non_null_count, "perc_null": round(perc_null, 2)}
        print(f"Colonna '{col}': nulli={null_count}, non nulli={non_null_count}, % nulli={perc_null:.2f}")

    # Mostra l'header e lo schema
    header_columns = [c for c in df_file.columns if c != "file"]
    print("Header:", header_columns)
    print("Schema:")
    df_file.printSchema()

    return {
        "header": header_columns,
        "null_report": report,
        "clean_df": df_file
    }


# Genera il report per ogni file e confronta gli header
reports = {}
headers = []
for file_name in file_list:
    reports[file_name] = report_per_file(df_all, file_name)
    headers.append(reports[file_name]["header"])

# Confronto degli header fra i file (sul driver)
if all(h == headers[0] for h in headers):
    print("\nTutti gli header sono uguali.")
else:
    print("\nGli header non sono tutti uguali. Dettaglio:")
    for file_name in file_list:
        print(f"{file_name} -> Header: {reports[file_name]['header']}")

# Chiusura sessione Spark
spark.stop()
