from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

# Configurazione ambiente (aggiorna i percorsi se necessario)
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
df_all = spark.read.csv(nomi_file, header=True, inferSchema=True) \
    .withColumn("file", F.input_file_name())

# Recupera la lista dei file effettivamente letti
file_list = [row.file for row in df_all.select("file").distinct().collect()]
print("File letti:", file_list)


# Funzione per generare il report generale
def genera_report(df, file_list):
    report_data = []
    total_rows_all_files = 0
    total_cols_all_files = 0
    common_columns = set(df_all.columns) - {"file"}

    with open("report_generale.txt", "w") as f:
        f.write("=== Report Generale ===\n\n")

        # Per ogni file, eseguiamo l'analisi
        for file_name in file_list:
            print(f"Elaborazione del file {file_name}...")
            df_file = df.filter(F.col("file") == file_name)
            total_count = df_file.count()
            total_rows_all_files += total_count
            total_cols_all_files = max(total_cols_all_files, len(df_file.columns) - 1)  # Senza la colonna 'file'

            f.write(f"\n--- Report per il file: {file_name} ---\n")
            f.write(f"Numero totale di righe: {total_count}\n")

            # Report per ogni colonna (escludendo la colonna 'file' aggiunta)
            for col in [c for c in df_file.columns if c != "file"]:
                cond_null = F.col(col).isNull() | (F.col(col) == "")
                null_count = df_file.filter(cond_null).count()
                non_null_count = total_count - null_count
                perc_null = (null_count / total_count * 100) if total_count > 0 else 0

                f.write(f"Colonna '{col}': nulli={null_count}, non nulli={non_null_count}, % nulli={perc_null:.2f}\n")

            # Colonne non comuni tra i file
            non_common_columns = set(df_file.columns) - common_columns
            if non_common_columns:
                f.write("\nColonne non comuni tra i file:\n")
                for col in non_common_columns:
                    f.write(f"  - {col}: {str(df_file.schema[col].dataType)}\n")

        # Dettagli sui dati aggregati
        f.write("\n--- Riepilogo Finale ---\n")
        f.write(f"Numero totale di righe per tutti i file: {total_rows_all_files}\n")
        f.write(f"Numero totale di colonne in tutti i file (comune a tutti): {total_cols_all_files}\n")

    print("Report generale generato nel file 'report_generale.txt'.")


# Eseguiamo il report in parallelo
genera_report(df_all, file_list)

# Chiusura della sessione Spark
spark.stop()
