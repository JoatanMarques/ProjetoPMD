# Comando para rodar o pyspark em local com as configurações necessárias
# pyspark --packages com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.3.0

# Configuração do cassandra
spark = (
	SparkSession.builder.appName("TripAgencyCassandra")
	.config(
		"spark.jars.packages",
		"com.datastax.spark:spark-cassandra-conncetor-assembly_2.12:3.3.0",
	)
	.config(
		"spark.sql.catalog.client", "com.datastax.spark.connector.datasource.CassandraCatalog"
	)
	.config("spark.sql.catalog.client.spark.cassandra.connection.host", "127.0.0.1")
	.config("spark.cassandra.connection.port", "9042")
	.config("spark.sql.extensions", "com.datasax.spark.connector.CassandraSparkExtensions")
	.getOrCreate()
)

# Lendo arquivo de voos
df = spark.read.option("header", True).csv("flights.csv")

# Renomeando colunas
df = (
	df.withColumnRenamed("travelCode", "travel_code")
	.withColumnRenamed("userCode", "user_code")
	.withColumnRenamed("from", "travel_from")
	.withColumnRenamed("to", "travel_to")
	.withColumnRenamed("flightType", "flight_type")
	.withColumnRenamed("date", "travel_date")
)

from datetime import datetime

from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType

# Alterando formato da data para ser compativel com o formato do cassandra
to_date = udf(lambda x: datetime.strptime(x, "%m/%d/%Y"), DateType())
df = df.withColumn("travel_date", to_date(col("travel_date")))

# Alterando tipo das colunas
df = (
	df.withColumn("travel_code", df.travel_code.cast("int"))
	.withColumn("user_code", df.user_code.cast("int"))
	.withColumn("price", df.price.cast("double"))
	.withColumn("time", df.time.cast("double"))
	.withColumn("distance", df.distance.cast("double"))
)

df.write.format("org.apache.spark.sql.cassandra").mode("append").options(
	table="flights", keyspace="tripcompany"
).save()
