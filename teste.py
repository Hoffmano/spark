from pyspark import SparkConf, SQLContext, SparkContext
from pyspark.sql.functions import mean as _mean, stddev as _stddev, col, sum, year, month, dayofweek
from pyspark.sql.types import DateType

import numpy as np #<--------------------------somente cliente
import matplotlib.pyplot as plt #<-------------------------somente cliente
import pandas as pd #<------------------------- acho que só o cliente?
import traceback # <---------------------somente cliente


class SparkDataframe: #--------------------------------------------------------------------o servidor   
#------------------------------------------------------------------- objetificação
    def __init__(self):
        self.template = '.\dados\sample\*'

        sc = SparkContext("local[3]", "teste")
        spark = SQLContext(sc)  
        self.arquivo = spark.read.format('csv').options(header=True, inferSchema = True).load(self.template)
        self.arquivo = self.arquivo.withColumn("DATE", col("DATE").cast(DateType()))

        self.arquivo.createOrReplaceTempView("source")
        self.selecionado = self.arquivo
        self.a_atual = 0
        self.b_atual = 0
        self.y_min = 0
        self.y_max = 0
        self.dP = 0
        self.med = 0
#--------------------------------------------------------------------as funções do servidor (retornam dataframes)
    def query(self, string):
        self.selecionado = self.arquivo.sql(string)

    def filter(self, string):
        self.selecionado = self.arquivo.filter(string)

    def agrupar(self, op, colunaLabel, colunaAlvo, tempo):
        if tempo:
            if tempo == 'year':
                self.selecionado = self.selecionado.groupBy(year(colunaLabel))
            elif tempo == 'month':
                self.selecionado = self.selecionado.groupBy(month(colunaLabel)) 
            elif tempo == 'day':
                self.selecionado = self.selecionado.groupBy(dayofweek(colunaLabel))
            else: self.selecionado = self.selecionado.groupBy(colunaLabel)

        if op == "sum":
            self.selecionado = self.selecionado.sum(colunaAlvo)
        elif op == "count":
            self.selecionado = self.selecionado.count()
        elif op == "max":
            self.selecionado = self.selecionado.max(colunaAlvo)
        elif op == "min":
            self.selecionado = self.selecionado.min(colunaAlvo)
        elif op == "avg":
            self.selecionado = self.selecionado.avg(colunaAlvo)       
        else:
            print("função não reconhecida.")
        

    def media(self, coluna):
        temp = self.selecionado.select(_mean(col(coluna))).alias('mean').collect()
        return temp[0][0]

    def desvioPadrao(self, coluna):
        temp = self.selecionado.select(_stddev(col(coluna))).alias('std').collect()
        return temp[0][0]

    def quadradosMinimos(self, feature, label):
        x_media = self.media(feature)
        y_media = self.media(label)
        x_min = self.selecionado.agg({feature: "max"}).collect()[0][0]
        x_max = self.selecionado.agg({feature: "min"}).collect()[0][0]

        sub_resposta_x = self.selecionado.withColumn("soma_x", col(feature).cast("double") * (col(feature).cast("double") - x_media)).select(col("soma_x"))
        sub_resposta_y = self.selecionado.withColumn("soma_y", col(feature).cast("double") * (col(label).cast("double") - y_media)).select(col("soma_y"))

        parte_baixo = sub_resposta_x.groupBy().sum().collect()[0][0]
        parte_cima = sub_resposta_y.groupBy().sum().collect()[0][0]

        self.b_atual = parte_cima / parte_baixo
        self.a_atual = y_media - self.b_atual * x_media

        self.y_min = self.a_atual + self.b_atual * x_min
        self.y_max = self.a_atual + self.b_atual * x_max
    
    def printSchema(self):
        self.arquivo.printSchema()
        

#-------------------------------------------------------------------- o cliente
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
            rdd.selecionado.show()

        elif entrada == "show": # <----------------------Mostra os dados selecionados.
            print(rdd.selecionado.toPandas())

        elif entrada == "media": # <-------------------Dado a seleção atual (feita por filter), faça a média da coluna alvo.
            temp = input("> Diga qual coluna:")
            valor = rdd.media(temp)
            rdd.med = valor
            print(valor)

        elif entrada == "desvio padrao": #<------------------Mesma coisa de cima só que para desvio padrão.
            temp = input("> Diga qual coluna:")
            valor = rdd.desvioPadrao(temp)
            rdd.dP = valor
            print(valor)

        elif entrada == "quadrados minimos": #<-------------------- Não faço a menor caralha ideia de como implementar.
            temp1 = input("> Diga qual a coluna de X:")
            formatado = temp1.split()
            temp2 = input("> Diga qual a coluna de Y:")
            rdd.quadradosMinimos(formatado[0], temp2)
            print("Quadrados calculados.")

        elif entrada == "agrupar": # EXEMPLO: calcular a menor temperatura de cada estação: op = min, colunaLabel = 'NAME', colunaAlvo = 'TEMP'
            op = input("> Qual função?: ") #<------ opções: count, min, max, avg, sum
            colunaLabel = input("> Qual coluna de grupo?: ")
            colunaAlvo = input("> Qual a coluna da função?: ")
            tempo = input("> Selecione o período de tempo (year,month,day ou nada): ")
            rdd.agrupar(op, colunaLabel, colunaAlvo, tempo)
            print(rdd.selecionado.toPandas())

        elif entrada == "query": #<--------------------------------- Query SQL padrão. Mais lento que o filter. Somente para debug.
            temp = input("> Faça uma query SQL(a tabela é source): ")
            rdd.query(temp)

        elif entrada == "plot": #<---------------------------------- Plot dos quadrados mínimos.
            if rdd.a_atual == 0 and rdd.b_atual == 0:
                print("Nada foi selecionado.")
            else:
                x = np.linspace(-100,100,100)
                y_media = rdd.a_atual + rdd.b_atual * x                

                plt.plot(x, y_media, '-r', label='a + bx')

                #a ser feito: plot do desvio padrão                
                
                plt.title('Resultante dos quadrados mínimos')
                plt.xlabel('x', color='#1C2833')
                plt.ylabel('y', color='#1C2833')
                plt.legend(loc='upper left')
                plt.grid()
                plt.show()

        elif entrada == "clear": #<---------------------------------- Limpe a seleção.
            rdd.selecionado = rdd.arquivo
            rdd.b_atual = 0
            rdd.a_atual = 0
            rdd.y_max = 0
            rdd.y_min = 0
            rdd.dP = 0
            rdd.med = 0
            print("Seleção atual limpa. Default = o dataset inteiro.")
        
        elif entrada == "schema":
            rdd.printSchema()
        else:
            print("Comando não reconhecido.")
    except:
        print("Crashou")
        traceback.print_exc()
