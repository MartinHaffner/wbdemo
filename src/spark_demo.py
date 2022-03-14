from pyspark.sql import SparkSession
import pyspark.sql.functions as sqlf
from pyspark.sql.types import StringType, IntegerType, StructType, StructField



# Erzeugt eine Spark-Session: der Job verbindet sich emtweder mit einem Cluster, oder
# erzeugt eine lokale 1-Knoten-Umgebung. Dies wird über die Parameter des Programms
# oder die Config-Informationen im SPARK_HOME-Verzeichnis sichergestellt
spark = SparkSession.builder.getOrCreate()


# Das unvermeidliche Hallo Welt-Beispiel
print(f"Hallo Spark {spark.version}!")

# Liest ein oder mehrere Rohdatenfiles aus einem lokalen Verzeichnis mit:
# - Trennzeichen Semikolon
# - Header-Zeile
# - automatischer Schema-Erkennung
# Wenn kein bestimmtes File angegeben ist, werden automatisch ALLE Files aus
# dem Verzeichnis gelesen!
# Ebenso könnten die Daten aus einem verteilten Verzeichnis geladen werden:
# z.B. spark.read.csv("hdfs://pfad/file") => HDFS
#      spark.read.csv("gs://pfad/file")   => Google Cloud Storage

df_messwerte_base = spark.read.csv(
    "raw_data/messwerte",
    header=True,
    sep=";",
    inferSchema=True)

# Werte ausgeben, Schema ausgeben
df_messwerte_base.show()
df_messwerte_base.printSchema()
df_messwerte_base.count()



# Einfache Transformationen:
# Spalten umbenennen (withColumnRenamed())
# Spalten auswählen (select())
# Zeilen auswählen (filter())

# Wichtig zu wissen: Das Ergebnis jeder Operation gibt wiederum einen
# Dataframe zurück; insofern kann man beliebig viele dieser Operationen
# aneinander hängen!
df_messwerte_2 = df_messwerte_base.withColumnRenamed(
    " TNK", "min_temperature"
).withColumnRenamed(
    " TXK", "max_temperature"
).withColumnRenamed(
    " TMK", "mean_temperature"
).withColumnRenamed(
    " UPM", "mean_humidity"
).select(
    "STATIONS_ID",
    "MESS_DATUM",
    "min_temperature",
    "max_temperature",
    "mean_temperature",
    "mean_humidity"
).filter(
    "mean_temperature > 10"
)


# Das gleiche mit SQL
# Dazu muss zuerst ein SQL-View auf den vorhandenen DataFrame angelegt werden
df_messwerte_base.createOrReplaceTempView("messwerte_base")
df_messwerte_2_sql = spark.sql("""
SELECT STATIONS_ID, 
       MESS_DATUM,
      ` TNK` min_temperature, 
      ` TXK` max_temperature, 
      ` TMK` mean_temperature, 
      ` UPM` mean_humidity 
    FROM messwerte_base
    WHERE ` TMK` > 10
""")


# Werte transformieren:
# Datum: Integer-Wert in Date konvertieren
# Stations-ID: in String ohne Nachkommastellen konvertieren, vorne mit Nullen
# auffüllen
# humidity: die Datenquelle (DWD) liefert den Platzhalter-Wert -999.0 für fehlende Werte
# diesen in None-Wert konvertieren

# Import von Hilfsfunktionen:
# import pyspark.sql.functions as sqlf
# Auch möglich: import pyspark.sql.functions.<function>
# => riskant, wenn an anderer Stelle Funktionen mit gleichem Namen existieren!
# (z.B. bei min() oder max())

df_messwerte_3 = df_messwerte_2.withColumn(
    "datum", sqlf.to_date(
        sqlf.col("MESS_DATUM").cast(StringType()),
        format="yyyyMMdd"
    )
).withColumn(
    "station_id", sqlf.lpad(
        sqlf.col("STATIONS_ID").cast(IntegerType()).cast(StringType()),
        5,
        "0")
).withColumn(
    "humidity_cleaned", sqlf.when(
        sqlf.col("mean_humidity") == -999.0, None
    ).otherwise(
        sqlf.col("mean_humidity")
    )
).select(
    "station_id",
    "datum",
    "min_temperature",
    "max_temperature",
    "mean_temperature",
    "humidity_cleaned"
)



# Werte transformieren, SQL way

df_messwerte_2_sql.createOrReplaceTempView("messwerte2")
df_messwerte_3_sql = spark.sql("""
SELECT lpad(
            cast(
                cast(STATIONS_ID AS Integer) 
                AS String
            ), 5, "0"
       ) station_id,
       to_date(
          cast(MESS_DATUM AS String), 
          "yyyyMMdd"
       ) datum,
       min_temperature,
       max_temperature,
       mean_temperature,
       CASE
         WHEN mean_humidity = -999.0 
         THEN NULL 
         ELSE mean_humidity
       END humidity_cleaned
    FROM messwerte2
""")





#
# Joinen von zwei Tabellen
#

# Als erstes die zweite Tabelle einlesen. Diese hat - anders als die erste
# Tabelle - keinen eindeutigen Delimiter, stattdessen ein Fixed Width Format.
df_stationen = spark.read.csv(
    "raw_data/stationen"
).withColumn(
    "station_id", sqlf.substring(sqlf.col("_c0"), 0, 5)
).withColumn(
    "station_name", sqlf.rtrim(sqlf.substring(sqlf.col("_c0"), 62, 40))
).select(
    "station_id", "station_name"
).filter(
    "station_id not in ('-----', 'Stati')"
)


# Der eigentliche Join: Angabe des Dataframes, der Bedingung und der Join-Art
# Die gejointe Tabelle enthält danach die Summe der Spalten aus beiden
# Tabellen; doppelte Spalten müssen bei Bedarf gedroppt werden.

df_messwerte_stationen = df_messwerte_3.join(
    df_stationen,
    df_messwerte_3["station_id"] == df_stationen["station_id"],
    "inner"
).drop(
    df_stationen["station_id"]
)

# das gleiche mit SQL
df_messwerte_3_sql.createOrReplaceTempView("messwerte3")
df_stationen.createOrReplaceTempView("stationen")
df_messwerte_stationen_sql = spark.sql("""
SELECT t1.*,
       t2.station_name
    FROM messwerte3 t1
    INNER JOIN stationen t2
    ON t1.station_id = t2.station_id
""")
df_messwerte_stationen_sql.show()



#
# Aggregation
# Für eine Aggregation muss zuerst ein GroupBy nach der Kategorie erfolgen;
# dann werden die Funktionen für die Aggregations-Statistiken angegeben
df_messwerte_stationen_agg = df_messwerte_stationen.groupBy(
    "station_name"
).agg(
    sqlf.min("min_temperature").alias("total_min"),
    sqlf.max("max_temperature").alias("total_max")
)

df_messwerte_stationen.createOrReplaceTempView("messwerte_stationen")
df_messwerte_stationen_agg_sql = spark.sql("""
SELECT station_name,
       min(min_temperature) total_min,
       max(max_temperature) total_max
    FROM messwerte_stationen
    GROUP BY station_name
""")


#
# Export Parquet
#
# Diese Variante erzeugt bei lokaler Ausführung physische Files (entsprechend Anzahl Worker)
df_messwerte_stationen_agg.write.parquet("processed_data/aggregierte_daten_3")
# Diese Variante erzwingt das Schreiben eines einzelnen Files
df_messwerte_stationen_agg.coalesce(1).write.parquet("processed_data/aggregierte_daten_4")

