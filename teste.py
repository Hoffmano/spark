from pyspark import SparkConf, SQLContext, SparkContext
from pyspark.sql.functions import mean as _mean, stddev as _stddev, col
import pandas as pd


class SparkDataframe:     
#------------------------------------------------------------------- objetificação
    def __init__(self):
        self.template = '.\dados\sample\*'        
        sc = SparkContext("local[3]", "teste")
        spark = SQLContext(sc)  
        self.arquivo = spark.read.format('csv').options(header=True).load(self.template)   
        self.arquivo.createOrReplaceTempView("source")
#--------------------------------------------------------------------as funções do servidor (retornam dataframes)
    def filter(self, string):
        return self.arquivo.filter(string)
    def media(self, coluna):
        temp = self.arquivo.select(_mean(col(coluna))).alias('mean').collect()
        return temp
    def desvioPadrao(self, coluna):
        temp = self.arquivo.select(_stddev(col(coluna))).alias('std').collect()
        return temp      
    

#--------------------------------------------------------------------o cliente
quit = False

rdd = SparkDataframe()

while (not quit):
    print()
    entrada = input("> ")
    if entrada == "quit":
        quit = True
    elif entrada == "filter":
        temp = input("> Filtre:") # SINTAXE: "coluna ='string'"
        resposta = rdd.filter(temp)
        print(resposta.toPandas())
    elif entrada == "media":
        temp = input("> Diga qual coluna:")
        print(rdd.media(temp))
    elif entrada == "desvio padrao":
        temp = input("> Diga qual coluna:")
        print(rdd.desvioPadrao(temp))
    elif entrada == "query":
        temp = input("> Faça uma query SQL(a tabela é source): ")
        print(rdd.arquivo.sql(temp))
    else:
        print("Comando não reconhecido.")


'''colunas = ['STATION','DATE','LATITUDE','LONGITUDE','ELEVATION','NAME','TEMP','TEMP_ATTRIBUTES','DEWP','DEWP_ATTRIBUTES','SLP',   
    	'SLP_ATTRIBUTES','STP','STP_ATTRIBUTES','VISIB','VISIB_ATTRIBUTES','WDSP','WDSP_ATTRIBUTES','MXSPD','GUST','MAX','MAX_ATTRIBUTES',
        'MIN','MIN_ATTRIBUTES','PRCP','PRCP_ATTRIBUTES','SNDP','FRSHTT']'''
