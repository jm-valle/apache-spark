
#""sparkMongo_maioresDoacoes.py"""

#Este código retorna as 30 maiores doações das eleições de 2014

from pyspark.sql import SparkSession

sparkDoacoes = SparkSession \
        .builder \
        .appName("doacao data") \
        .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.11:2.2.2.') \
        .config("spark.mongodb.input.uri","mongodb://10.7.40.39/doacao_eleicoes.doacao_collection") \
        .config("spark.mongodb.output.uri","mongodb://10.7.40.39/doacao_eleicoes.doacao_collection") \
        .getOrCreate()

#Criacao dos Dataframes
dfd = sparkDoacoes.read.format("com.mongodb.spark.sql.DefaultSource").load()
#visualizacao do Dataframe
dfd.printSchema()

#Seleciona os 30 maiores doadores do Dataframe
maiores_doa = dfd.orderBy(dfd.num_donation_ammount.desc()).take(30)
for row in maiores_doa:
    print("Nome do doador: "+ row.cat_donator_name.encode("utf8") + "\n" \
        "Quantidade doada: " + str(row.num_donation_ammount) + "\n"  \
        "Nome do Candidato " + row.cat_candidate_name.encode("utf8"))