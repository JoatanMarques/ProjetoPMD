# Comando para rodar o pyspark em local com as configurações necessárias
# pyspark --conf "spark.mongodb.read.connection.uri=mongodb://127.0.0.1/pmd2023joatan.fase_final?readPreference=primaryPreferred" --conf "spark.mongodb.write.connection.uri=mongodb://127.0.0.1/pmd2023joatan.fase_final" --conf "spark.mongodb.write.ignoreNullValues=true" --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.0

# Configurando spark e lendo arquivos
spark = (
	SparkSession.builder.appName("teste")
	.config(
		"spark.mongodb.read.connection.uri", "mongodb://127.0.0.1/pmd2023joatan.fase_final"
	)
	.config(
		"spark.mongodb.write.connection.uri", "mongodb://127.0.0.1/pmd2023joatan.fase_final"
	)
	.config("spark.mongodb.write.ignoreNullValues", True)
	.getOrCreate()
)

df1 = spark.read.option("header", True).csv("users.csv")
df2 = spark.read.option("header", True).csv("hotels.csv")

# Tranformando a tabela de hotéis e usuários em uma tabela que contem o usuário e uma lista de hotéis onde ele visitou

hotel_list = df2.collect()

hotel_dict = {}

# Varrendo todos os hotéis e criando uma lista em um dicionário onde a chave é o id do usuário
for hotel in hotel_list:
	if hotel.userCode not in hotel_dict:
		hotel_dict[hotel.userCode] = []
	hotel_dict[hotel.userCode].append(hotel)

user_list = df1.collect("code")
data = []

from pyspark.sql import Row

# Varrendo uma lista com o id de todos os usuários e criando sua respectiva linha (tupla em uma lista de dados)
for user in user_list:
	if user.code in hotel_dict:
		data.append((user.code, Row(hotels=hotel_dict[user.code])))
	else:
		data.append((user.code, Row(hotels=[])))

columns = ["userCode", "hotels"]

# Criando um dataframe com o id do usuário e uma lista dos hotéis de cada usuário
df3 = spark.createDataFrame(data=data, schema=columns)
df4 = df3.select("userCode", "hotels.hotels")

# Fazendo uma junção do dataframe de usuários com o dataframe que acabou de ser criado
df1 = df1.join(df4, df1.code == df4.userCode, "inner")
df1 = df1.select("userCode", "name", "gender", "age", "hotels")

# Tratando dados nulos e renomeando/alterando tipo das colunas
from pyspark.sql.functions import col, when

df1 = df1.withColumn(
	"gender", when(col("gender") == "none", None).otherwise(col("gender"))
)
df1 = df1.withColumnRenamed("userCode", "user_code")
df1 = df1.withColumn("user_code", df1.user_code.cast("int")).withColumn(
	"age", df1.age.cast("int")
)

# Escrevendo o dataframe no mongodb
df1.write.format("mongodb").mode("overwrite").save()