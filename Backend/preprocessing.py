from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import shutil

import os
# os.environ["HADOOP_HOME"] = "C:\\hadoop"
# os.environ["hadoop.home.dir"] = "C:\\hadoop"
#
# #os.environ["HADOOP_OPTS"] = "-Djava.library.path="
# import os
# os.environ["PYSPARK_PYTHON"] = r"C:\Users\ANGELICA\Desktop\ANGELICA\UNICAL\MAGISTRALE\I ANNO\PRIMO SEMESTRE\MODELLI E TECNICHE PER BIG DATA\PROGETTO\CODICE\.venv\Scripts\python.exe"
# os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\ANGELICA\Desktop\ANGELICA\UNICAL\MAGISTRALE\I ANNO\PRIMO SEMESTRE\MODELLI E TECNICHE PER BIG DATA\PROGETTO\CODICE\.venv\Scripts\python.exe"
# os.environ["PYSPARK_PYTHON"] = os.path.join("C:\\Users\\ANGELICA\\Desktop\\ANGELICA\\UNICAL\\MAGISTRALE\\I ANNO\\PRIMO SEMESTRE\\MODELLI E TECNICHE PER BIG DATA\\PROGETTO\\CODICE\\.venv", "Scripts", "python.exe")
# os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ["PYSPARK_PYTHON"]
#
# # Creazione della sessione Spark
# # TODO cancellare 8g dal codice seguente
# #spark = SparkSession.builder.appName("AnalisiDatiVoli").config("spark.driver.memory", "24g").getOrCreate()
# spark = SparkSession.builder \
#     .appName("AnalisiDatiVoli") \
#     .config("spark.driver.memory", "24g") \
#     .config("spark.sql.debug.maxToStringFields", "1000") \
#     .config("spark.hadoop.fs.defaultFS", "file:///") \
#     .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
#     .config("spark.hadoop.io.nativeio.NativeIO$Windows.access0", "false") \
#     .config("spark.hadoop.home.dir", "C:\\hadoop") \
#     .master("local[*]") \
#     .getOrCreate()
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PYSPARK_PYTHON"] = r"C:\Users\ANGELICA\Desktop\ANGELICA\UNICAL\MAGISTRALE\I ANNO\PRIMO SEMESTRE\MODELLI E TECNICHE PER BIG DATA\PROGETTO\CODICE\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = os.environ["PYSPARK_PYTHON"]
os.environ["SPARK_HOME"] = "C:\\spark"
os.environ["PATH"] += os.pathsep + r"C:\Users\ANGELICA\Desktop\ANGELICA\UNICAL\MAGISTRALE\I ANNO\PRIMO SEMESTRE\MODELLI E TECNICHE PER BIG DATA\PROGETTO\CODICE\.venv\Scripts"

# Creazione della sessione Spark
spark = SparkSession.builder \
    .appName("AnalisiDatiVoli") \
    .config("spark.driver.memory", "24g") \
    .config("spark.sql.debug.maxToStringFields", "1000") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.hadoop.io.nativeio.NativeIO$Windows.access0", "false") \
    .config("spark.hadoop.home.dir", "C:\\hadoop") \
    .master("local[*]") \
    .getOrCreate()
import os
print(os.environ["PYSPARK_PYTHON"])
print(spark.version)

spark.conf.set("spark.sql.debug.maxToStringFields", "1000")  # Or any higher number depending on your needs
spark.conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
spark.conf.set("spark.hadoop.native.lib", "false")


#Caricamento dei dati
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

#Controllo degli header dei file
df_riferimento = spark.read.csv(nomi_file[0], header=True, inferSchema=True)
df_lista_file = [df_riferimento]
for file in nomi_file[1:]:
    df_altro_file = spark.read.csv(file, header=True, inferSchema=True)
    if df_altro_file.columns == df_riferimento.columns:
        df_lista_file.append(df_altro_file)
    else:
        print(f"Il file {file} ha una struttura diversa")

#Unione del contenuto dei file con header uguale
df_finale = df_lista_file[0]
for file in df_lista_file[1:]:
    df_finale = df_finale.union(file)
df_finale = df_finale.drop("_c109")
df_finale.show(5)

# PRE PROCESSING DEI DATI
#Eliminazione delle righe e delle colonne con valori nulli
n_righe_prima = df_finale.count()
print(f"Numero righe prima : {n_righe_prima}")
df_finale.dropna(how="all")
numero_righe = df_finale.count()
print(f"Numero righe dopo: {numero_righe}")
nulli_per_colonna = {
    col_n : df_finale.filter(col(col_n).isNull()).count() / numero_righe
    for col_n in df_finale.columns
}
threshold = 0.9     #soglia di valori nulli che devono esserci
colonne_con_nulli =  [col_n for col_n, pct_null in nulli_per_colonna.items() if pct_null > threshold]
print(f"Colonne con nulli: {colonne_con_nulli}")
df_finale = df_finale.drop(*colonne_con_nulli)

#Eliminazione dei duplicati
df_finale = df_finale.dropDuplicates()

project_dir = os.getcwd()  # Ottiene la directory del progetto
data_dir = os.path.join(project_dir, "Dati")  # Percorso della cartella dati

output_dir = os.path.join(data_dir, "On_Time_Performance_2013_cleaned")

# Rimuove la cartella di output se esiste già
if os.path.exists(output_dir):
    shutil.rmtree(output_dir)

df_finale.coalesce(1).write.option("header", "true").csv(output_dir)

print(f"Il file è stato salvato in: {output_dir}")

df_finale.describe().show()
df_finale.show(5)