# Spark

## Linguagem utilizada
Para o desenvolvimento desta aplicação foi utilizado Python, a escolha foi feita devido a alguns atributos
- Membros ja tinham conhecimento prévio relevante
- Facilidade para execução de operações matemáticas e estatísticas
- Suporte para Apache Spark

## Bibliotecas utilizadas
- pyspark
- numpy
- matplotlib
- pandas
- traceback
- scipy

## Como funciona
Spark é um framework para processamento de dados em grande escala.

O Spark foi criado tentando resolver as limitações do MapReduce
(by doing processing in-memory, reducing the number of steps in a job, and by reusing data across multiple parallel operations) 

Em apenas um passo, os dados são lidos, as operações são feitas e os resultados reescritos.

Escrito em Java, interpretado por Python, Scala e R(parcialmente).

O driver roda um processo.

### SQL e Big Data
Em cima dos objetos de Row do RDD, há a abstração Dataframe.
Permite a integração de consultas SQL padrão.
Com otimização de custo, tornando as queries rápidas.
O núcleo do Spark torna as queries tolerantes a falhas.


Na inicialização, declaramos cores como Workers, que serão usados por completo quando chamados.

Cada Worker também é um processo.

O Standalone Cluster Manager administrará como os Workers trabalharão e dividirá a carga de trabalho.

# Como Utilizar

1. Alterar na `linha 15` do arquivo `modulo_spark.py` colocando a pasta que contenha os dados a serem analisados como seque o exemplo
    - Os dados devem estar descompactados em pastas com o nome do ano, e dentro das pastas dos anos devem contem os csvs
3. Iniciar o terminal do programa utilizando o comando padrão do python

## Spark Data Frame
A classe SparkDataFrame foi uma classe desenvolvida para encapsular todo o ferramental necessário para a analise dos dados em questão.

Nela temos algumas funções

# Funções

É possivel visualizar exemplos das funções no arquivo `examples.md`

### query
Utilizada para debug, faz queries SQL no data frame, porem de maneira muito lenta.

---

### filter
Utilizada para filtrar parte do data frame, equivalente a um where do SQL.

#### Exemplo de uso
```
> filter
> Filtre: NAME = "SOKOL, RS"
```

#### Resultado
Com isso passaremos a ter salvo no nosso SparkDataFrame o conjunto de dados resultado do filter

---

### show
Comando utilizado para mostrar no terminal o conjunto de dados selecionado.

#### Exemplo de uso
```
> show
```

#### Resultado
Imprime no terminal o conjunto de dados previamente selecionado

---

### media
Comando utilizado calcular a média de alguma coluna numérica do conjunto de dados selecionado.

#### Exemplo de uso
```
> media
> Diga qual coluna: TEMP
```

#### Resultado
Calcula a média da coluna do conjunto de dados selecionado.

---

### desvio padrao
Comando utilizado calcular o desvio padrão de alguma coluna numérica do conjunto de dados selecionado, alem de plotar o gráfico da distribuição do mesmo.
Para usar este comando adequadamente é necessário calcular a média da coluna previamente

#### Exemplo de uso
```
> media
> Diga qual coluna: TEMP

> desvio padrao
> Diga qual coluna: TEMP
```

---

### quadrados minimos
Comando utilizado calcular os quadrados mínimos entre duas colunas de dados numéricas.
Para visualizar o resultado deste comando utilize o comando `plot`

#### Exemplo de uso
```
> quadrados minimos
> Diga qual a coluna de X: TEMP
> Diga qual a coluna de Y: DEWP
```

---

### agrupar
Comando utilizado agrupar o conjunto de dados selecionado aplicando alguma determinada função.
Opções:
- avg
- min
- max
- sum
- count

#### Exemplo de uso
```
> agrupar
> Qual função?: avg
> Qual coluna de grupo?: DATE
> Qual a coluna da função?: TEMP
> Selecione o período de tempo (year,month,day ou nada): mounth
```

---

### plot
Comando utilizado exibir o grafico calculado pela função `quadrados minimos`

#### Exemplo de uso
```
> agrupar
> Qual função?: avg
> Qual coluna de grupo?: DATE
> Qual a coluna da função?: TEMP
> Selecione o período de tempo (year,month,day ou nada): mounth
```

---

### plot
Comando utilizado exibir o grafico calculado pela função `quadrados minimos`

#### Exemplo de uso
```
> plot
```

### clear
Comando utilizado limpar o conjunto de dados selecionado, sendo necessário fazer um novo filter para populá-lo novamente.

#### Exemplo de uso
```
> clear
```

### schema
Comando utilizado exibir o schema do data frame selecionado, sendo isso o nome das colunas do Dataframe e o seu tipo de variável.

#### Exemplo de uso
```
> schema
```
