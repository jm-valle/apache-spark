
#""sparkMongo_maisFrequentes.py"""

#Doadores mais frequentes e quanto eles doaram

from pyspark.sql import SparkSession

sparkDoacoes = SparkSession \
        .builder \
        .appName("doacao data") \
        .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.11:2.2.2.') \
        .config("spark.mongodb.input.uri","mongodb://10.7.40.39/doacao_eleicoes.doacao_collection") \
        .config("spark.mongodb.output.uri","mongodb://10.7.40.39/doacao_eleicoes.doacao_collection") \
        .getOrCreate()

#Criacao dos Dataframes de doacoes
dfd = sparkDoacoes.read.format("com.mongodb.spark.sql.DefaultSource").load()

#Extraindo do dataframe os doadores mais frequentes(5% mais Frequentes)
freq1 = dfd.freqItems(("cat_original_donator_name2",),0.05).collect()

#Filtragem dos nomes dos doadores
for empresa in freq1[0].cat_original_donator_name2_freqItems:
	f1 = dfd.filter(dfd.cat_original_donator_name2 == empresa).collect()
	sum = 0	
	for row in f1:
		sum = sum + row.num_donation_ammount
	print(empresa.encode("utf-8") + ": R$" + str(sum))