
#""sparkMongo_doacaoCandidato.py"""

#Quantidade de doações por candidato, neste caso foi utilizado o 
#nome de Dilma Rousseff

sparkDoacoes = SparkSession \
        .builder \
        .appName("doacao data") \
        .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.11:2.2.2.') \
        .config("spark.mongodb.input.uri","mongodb://10.7.40.39/doacao_eleicoes.doacao_collection") \
        .config("spark.mongodb.output.uri","mongodb://10.7.40.39/doacao_eleicoes.doacao_collection") \
        .getOrCreate()

#Criacao do dataframe de doacoes
dfd = sparkDoacoes.read.format("com.mongodb.spark.sql.DefaultSource").load()
#Criacao do dataframe de candidatos
c = dfd.groupBy([dfd.cat_candidate_name]).count().collect()
for row in c:
    if row.cat_candidate_name == u'DILMA VANA ROUSSEFF':
        print("count:"+str(row["count"]))