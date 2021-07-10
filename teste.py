from pyspark import SparkConf, SQLContext, SparkContext
from pyspark.sql.functions import mean as _mean, stddev as _stddev, col
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import pandas as pd
import traceback


class SparkDataframe:     
#------------------------------------------------------------------- objetificação
    def __init__(self):
        self.template = '.\dados\sample\*'        
        sc = SparkContext("local[3]", "teste")
        spark = SQLContext(sc)  
        self.arquivo = spark.read.format('csv').options(header=True).load(self.template)   
        self.arquivo.createOrReplaceTempView("source")
        self.selecionado = self.arquivo
#--------------------------------------------------------------------as funções do servidor (retornam dataframes)
    def filter(self, string):
        self.selecionado = self.arquivo.filter(string)

    def media(self, coluna):
        temp = self.selecionado.select(_mean(col(coluna))).alias('mean').collect()
        return temp[0][0]

    def desvioPadrao(self, coluna):
        temp = self.selecionado.select(_stddev(col(coluna))).alias('std').collect()
        return temp[0][0]

    def quadradosMinimos(self, feature, label): #<----------------MUITO IMPORTANTE A SER FEITO.
        listX = []
        for col in feature:
            numero = self.desvioPadrao(col)
            listX.append(numero)
        

#--------------------------------------------------------------------o cliente
quit = False

rdd = SparkDataframe()

#Também muito importante: Discutir se e como refazer esse terminal. Porque mano, tanto else if empilhado me da gatinho.

while (not quit):  
    try: #<----------------- Usado porque isso crasha. Bastante.
        print()
        entrada = input("> ")
        if entrada == "quit": # <-------- Auto explicativo.
            quit = True
            break
        elif entrada == "filter": # <---------------- Filtre as colunas, melhor método. SINTAXE: (coluna = 'valor' AND coluna = 'valor' AND..... Aceita <, >, != e =).
            temp = input("> Filtre:") 
            rdd.filter(temp)            
            print(rdd.selecionado.toPandas())
        elif entrada == "media": # <-------------------Dado a seleção atual (feita por filter), faça a média da coluna alvo.
            temp = input("> Diga qual coluna:")
            print(rdd.media(temp))
        elif entrada == "desvio padrao": #<------------------Mesma coisa de cima só que para desvio padrão.
            temp = input("> Diga qual coluna:")
            print(rdd.desvioPadrao(temp))
        elif entrada == "quadrados minimos": #<-------------------- Não faço a menor caralha ideia de como implementar.
            temp1 = input("> Diga qual a coluna de X:")
            formatado = temp1.split()
            temp2 = input("> Diga qual a coluna de Y:")
            print(rdd.quadradosMinimos(formatado, temp2))
        elif entrada == "query": #<--------------------------------- Query SQL padrão. Mais lento que o filter. Somente para debug.
            temp = input("> Faça uma query SQL(a tabela é source): ")
            print(rdd.arquivo.sql(temp))
        elif entrada == "clear": #<---------------------------------- Limpe a seleção.
            rdd.selecionado = rdd.arquivo
            print("Seleção atual limpa. Default = o dataset inteiro.")
        else:
            print("Comando não reconhecido.")
    except:
        print("Crashou")
        traceback.print_exc()

        

'''colunas = ['STATION','DATE','LATITUDE','LONGITUDE','ELEVATION','NAME','TEMP','TEMP_ATTRIBUTES','DEWP','DEWP_ATTRIBUTES','SLP',   
    	'SLP_ATTRIBUTES','STP','STP_ATTRIBUTES','VISIB','VISIB_ATTRIBUTES','WDSP','WDSP_ATTRIBUTES','MXSPD','GUST','MAX','MAX_ATTRIBUTES',
        'MIN','MIN_ATTRIBUTES','PRCP','PRCP_ATTRIBUTES','SNDP','FRSHTT']. Isso é só para lembrar o esquema.'''
