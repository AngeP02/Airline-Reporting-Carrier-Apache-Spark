import os
from pyspark.sql import SparkSession

# Impostazione delle variabili d'ambiente per Hadoop e Spark
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
