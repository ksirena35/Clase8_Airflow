
#!/usr/bin/env python3

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext

sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)

from pyspark.sql import functions as F
from pyspark.sql.functions import col

##leo el archivo parquet ingestado  desde HDFS y lo cargo en un dataframe.


df1 = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/results.csv")
df2 = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/drivers.csv")


###Hago un Join de los dos dataframe.

union = df1.join(df2, on="driverid", how="inner")


###cambie el schema de las columnas driverid y points, para operar funciones con enteros .

cambio = union.withColumn("driverId", col("driverId").cast("int")).withColumn("points", col("points").cast("int"))



##Rankeo por la mayor cantidad de puntos, filtre los 10 mejores en puntaje en la historia.

ranked_drivers = cambio.orderBy(F.col('points').desc()).limit(10)

##Dataframe final para insertar en  la tabla driver_result.

dff = ranked_drivers.select(
    ranked_drivers.forename.alias("driver_forname"),
    ranked_drivers.surname.alias("driver_surname"),
    ranked_drivers.nationality.alias("driver_nationality"),
        ranked_drivers.points 
)

##inserto el dff en la bases de datos f1 la tabla driver_result.

dff.write.mode("append").format("hive").saveAsTable("f1.driver_result")


####Ingest del archivo constructors.csv para preparar los datos para la tabla constructor_results.

df3 = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/constructors.csv")

##El Gran Prix en  1991 tiene el raceID 318, hago un filtro del f1.

filter_results = df1.filter(df1.raceId == 318)


##rankeo  los 3  con mejor puntaje

ranked_results = filter_results.orderBy(F.col('points').desc()).limit(3)

# hago un join con el dataframe f3 que  contienen las columnas que pide el ejercicio.

ranked_join = ranked_results.join(df3, on="constructorId", how="inner")

##luego sumo los puntos de los drivers y los agrupo por constructor.

constructor_points_sum = ranked_join.groupBy("constructorId").agg(F.sum("points").alias("points"))

##preparo la tabla que voy a insertar.

df_insert = ranked_join.select(
    ranked_join.constructorRef,
        ranked_join.name.alias("cons_name"),
    ranked_join.nationality.alias("cons_nationality"),
        ranked_join.url,
        ranked_join.points.cast("int").alias("points")
)

##Hago un append para agregar los datos en la tabla ya creada en hive constructor_result.

df_insert.write.mode("append").format("hive").saveAsTable("f1.constructor_result")
