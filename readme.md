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

## Spark Data Frame
A classe SparkDataFrame foi uma classe desenvolvida para encapsular todo o ferramental necessário para a analise dos dados em questão.

Nela temos algumas funções

# Funções

---

### query
Utilizada para debug, faz queries SQL no data frame, porem de maneira muito lenta

---

### filter
Utilizada para filtrar parte do data frame, equivalente a um where do SQL

#### Exemplo de uso
```
> filter
> Filtre: NAME = "SOKOL, RS"
```

#### Resultado
Com isso passaremos a ter salvo no nosso SparkDataFrame o conjunto de dados resultado do filter

---

### show
Comando utilizado para mostrar no terminal o conjunto de dados selecionado

#### Exemplo de uso
```
> show
```

#### Resultado
Imprime no terminal o conjunto de dados previamente selecionado

---

### media
Comando utilizado calcular a média de alguma coluna numérica do conjunto de dados selecionado

#### Exemplo de uso
```
> media
> Diga qual coluna: TEMP
```

#### Resultado
Calcula a média da coluna do conjunto de dados selecionado

---

### desvio padrao
Comando utilizado calcular o desvio padrão de alguma coluna numérica do conjunto de dados selecionado, alem de plotar o gráfico da distribuição do mesmo
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
Comando utilizado calcular os quadrados mínimos entre duas colunas de dados numéricas
Para visualizar o resultado deste comando utilize o comando `plot`

#### Exemplo de uso
```
> quadrados minimos
> Diga qual a coluna de X: TEMP
> Diga qual a coluna de Y: DEWP
```

---

### agrupar
Comando utilizado agrupar o conjunto de dados selecionado aplicando alguma determinada função
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
Comando utilizado limpar o conjunto de dados selecionado, sendo necessário fazer um novo filter para popula-lo novamente

#### Exemplo de uso
```
> clear
```

### schema
Comando utilizado exibir o schama do data frame selecionado

#### Exemplo de uso
```
> schema
```
