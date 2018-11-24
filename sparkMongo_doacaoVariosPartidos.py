
#""sparkMongo_doacoesVariosPartidos.py"""

#O resultado deste código é quais empresas dizeram doações para mais de um partido político
from pyspark.sql import SparkSession

#Inicia a SparkSession, variável utilizada para inicializar as bilbiotecas do Apache Spark
sparkDoacoes = SparkSession \
        .builder \
        .appName("doacao data") \
        .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.11:2.2.2.') \
        .config("spark.mongodb.input.uri","mongodb://10.7.40.39/doacao_eleicoes.doacao_collection") \
        .config("spark.mongodb.output.uri","mongodb://10.7.40.39/doacao_eleicoes.doacao_collection") \
        .getOrCreate()

#Cria o dataframe de doacoes
dfd = sparkDoacoes.read.format("com.mongodb.spark.sql.DefaultSource").load()

#Agrupa os dataframes de doacoes pelo nome do doador, e pelo partido político que ele doou
ag = dfd.groupBy([dfd.cat_donator_name2, dfd.cat_party])
#Ordenada as doacoes por quantidades
sort_ag = sorted(ag.agg({"num_donation_ammount": "sum"}).collect())

#Inicia a separacao por empresas e quantidades de doacoes
emp = {}

for row in sort_ag:
    #caso o nome da empresa doadora nao esteja na lista de empresas, a Tupla daquela empresa recebe o nome do Partido a que ela doou
    if row.cat_donator_name2 not in emp:
        emp[row.cat_donator_name2] = [row.cat_party]
    #Caso esteja, eu simplesmente irei adicionar o nome do partido na tupla
    else:
        emp[row.cat_donator_name2].append(row.cat_party)
    #Se a tupla tiver mais de um chave, ou seja, mais de um nome, quer dizer que doou para mais de um partido
    for key in emp:
        if len(emp[key]) > 1:
            print(key.encode("utf-8"))




