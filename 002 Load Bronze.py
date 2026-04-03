# Databricks notebook source
# MAGIC %md
# MAGIC **1. Configurações Iniciais e Importações**
# MAGIC
# MAGIC Aqui está um exemplo de um notebook em PySpark para implementar a arquitetura Medallion com as camadas Bronze, Silver e Gold, utilizando Databricks e Delta Lake. Este exemplo segue as boas práticas de desenvolvimento e performance, incluindo a criação de surrogate keys (chaves substitutas) para as dimensões e otimização da tabela de fatos na camada Gold.
# MAGIC
# MAGIC **Explicações:**
# MAGIC
# MAGIC - Importar bibliotecas e funções necessárias.
# MAGIC - Definir os caminhos de arquivo para as camadas Bronze, Silver e Gold.
# MAGIC - Configurar as definições do Spark para um desempenho ótimo, como partições de shuffle automático.

# COMMAND ----------

# Importar as bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Iniciar a SparkSession com configurações otimizadas, aqui é como se tivessemos definindo como nossa equipe de trabalho gera gerenciada
spark = (
    SparkSession.builder
        .appName("Load Data Bronze")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.files.maxPartitionBytes", "128MB")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.executor.instances", "10")
        .config("spark.executor.memory", "8g")
        .config("spark.executor.cores", "4")
        .getOrCreate()
)

## Ambiente de trabalho dos executores, abaixo são as configurações do  ambiente de trabalho dos executores ##
# spark.sql.shuffle.partitions -> O Spark divide o trabalho em partições, é como decidir quantas “caixinhas” você vai usar para organizar os dados antes de processá-los, aqui estamos criando 200 partições (caixas) para organizar os dados.

# spark.sql.files.maxPartitionBytes -> Define o tamanho máximo de dados por partição quando o Spark lê arquivos, é como dizer: “cada caixa pode ter no máximo 128 garrafas” no nosso caso 128 MB de dados.Quando você lemos arquivos grandes (CSV, JSON, Parquet), isso evita que uma única partição fique enorme e vire gargalo.

# spark.sql.parquet.compression.codec -> Define o tipo de compressão para arquivos Parquet com "snappy" (Snappy é uma compressão rápida e leve). Na camada Bronze, queremos armazenar dados de forma eficiente sem perder velocidade. Snappy é perfeito para isso

# spark.sql.adaptive.enabled -> O Spark fica “inteligente” e ajusta o plano de execução enquanto o job está rodando. O Adaptive Query Execution (AQE) é um recurso do Spark que observa o que está acontecendo durante o job e faz ajustes dinâmicos para melhorar a performance. Ele não ignora as configurações que eu criei, mas corrige ou otimiza quando percebe que elas não são ideais para aquele conjunto de dados (ele não altera PatitionBytes).

## Executores em si, aqui definimos os recursos que serão usados para processar os dados ##

# spark.executor.instances -> Define o número de executores (processos) que serão criados para processar os dados. Aqui, estamos usando 10 executores.

# spark.executor.memory -> Define a quantidade de memória RAM disponível para cada executor. Aqui, estamos usando 8 GB de memória por executor.

# spark.executor.cores -> Define o número de núcleos de CPU disponíveis para cada executor. Aqui, estamos usando 4 núcleos por executor. Cada núcleo executa uma tarefa por vez então, se você tiver 4 núcleos, pode processar 4 tarefas simultaneamente, logo, com 10 executores, pode processar 40 tarefas simultaneamente.

#Cluster
#│
#├── Nó 1 (worker)
#│     ├── Executor 1 (4 cores, 8GB)
#│     └── Executor 2 (4 cores, 8GB)
#│
#├── Nó 2 (worker)
#│     ├── Executor 3 (4 cores, 8GB)
#│     └── Executor 4 (4 cores, 8GB)
#│
#└── Nó 3 (worker)
#     ├── Executor 5 (4 cores, 8GB)
#     └── Executor 6 (4 cores, 8GB)


# Definir caminhos de armazenamento no Data Lake
lz_path_in = "/Volumes/lhdw/data/landingzone_vendas_processar/"
lz_path_out = "/Volumes/lhdw/data/landingzone_vendas_processado/"
bronze_path = "/Volumes/lhdw/data/bronze/"



# COMMAND ----------

# MAGIC %md
# MAGIC **Justificativa:**
# MAGIC
# MAGIC - **spark.sql.shuffle.partitions**: Define o número de partições para operações que envolvem shuffle (como joins e agregações). Escolher um valor fixo, como 200, garante que o cluster trabalhe de forma paralela de maneira eficiente.
# MAGIC
# MAGIC Um cálculo comum para o número de partições é o seguinte:
# MAGIC
# MAGIC _`número de partições = número de núcleos de CPU * 2 ou 3`_
# MAGIC
# MAGIC Isso ajuda a garantir que o Spark use todos os núcleos disponíveis.
# MAGIC - **spark.sql.files.maxPartitionByte**s: Definimos o tamanho máximo dos arquivos particionados para evitar a criação de muitos arquivos pequenos, o que prejudicaria a performance de leitura e escrita.
# MAGIC - **spark.sql.parquet.compression.codec**: Snappy é uma escolha comum para Parquet, pois oferece uma boa combinação de compressão rápida e descompressão eficiente.
# MAGIC - **spark.sql.adaptive.enabled**: A otimização adaptativa ajusta o plano de execução conforme o tamanho dos dados, melhorando o desempenho automaticamente.

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC **2. Camada Bronze: Ingestão de Dados Brutos**
# MAGIC
# MAGIC A camada Bronze armazena dados brutos com formato parquet, sem transformações significativas. Aqui vamos simplesmente gravar os dados brutos como parquet.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando um Schema para dados brutos

# COMMAND ----------

# Definir o esquema dos dados brutos
schema_lz = StructType([
    StructField("IDProduto", IntegerType(), True),
    StructField("Data", DateType(), True),
    StructField("IDCliente", IntegerType(), True),
    StructField("IDCampanha", IntegerType(), True),
    StructField("Unidades", IntegerType(), True),
    StructField("Produto", StringType(), True),
    StructField("Categoria", StringType(), True),
    StructField("Segmento", StringType(), True),
    StructField("IDFabricante", IntegerType(), True),
    StructField("Fabricante", StringType(), True),
    StructField("CustoUnitario", DoubleType(), True),
    StructField("PrecoUnitario", DoubleType(), True),
    StructField("CodigoPostal", StringType(), True),
    StructField("EmailNome", StringType(), True),
    StructField("Cidade", StringType(), True),
    StructField("Estado", StringType(), True),
    StructField("Regiao", StringType(), True),
    StructField("Distrito", StringType(), True),
    StructField("Pais", StringType(), True)
]) # está definindo manualmente o esquema (schema) dos dados que serão lidos

# Leitura dos dados e adição da coluna nome do arquivo durante a leitura
df_vendas = spark.read.option("header", "true").schema(schema_lz).csv(lz_path_in) \
                      .withColumn("filename", regexp_extract(col("_metadata.file_path"), "([^/]+)$", 0))
#spark.read.option("header", "true") -> Diz ao Spark que o CSV tem cabeçalho
#.schema(schema_lz) -. Define o esquema dos dados que serão lidos
#.csv(lz_path_in) -> Lê os dados do caminho especificado
#.withColumn("filename", regexp_extract(col("_metadata.file_path"), "([^/]+)$", 0)) -> Adiciona uma nova coluna chamada "filename" que contém o nome do arquivo original usando uma expressão regular para extrair o nome do arquivo do caminho completo do arquivo. O regex ([^/]+)$ → pega só o nome do arquivo (tudo depois da última / ).

distinct_filenames = df_vendas.select("filename").distinct()

# Exibindo o DataFrame para verificar a leitura correta dos dados
display(df_vendas)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apresentando os arquivos lidos

# COMMAND ----------


display(distinct_filenames)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salvar/Persistir dados na camada Bronze Bronze
# MAGIC
# MAGIC Os dados serão salvos de forma particionada **Ano e Mês**

# COMMAND ----------

# Escrever a tabela no formato Parquet, particionando por DataVenda (ano e mês)
df_vendas.withColumn("Ano", year("Data")) \
             .withColumn("Mes", month("Data")) \
             .write.mode("overwrite").partitionBy("Ano", "Mes").parquet(bronze_path)

# Apresentando o DataFrame
#display(df_vendas)

# COMMAND ----------

# MAGIC %md
# MAGIC **Justificativas:**
# MAGIC
# MAGIC - Lê os dados brutos a partir de um arquivo CSV na landing zone e escreve esses dados no formato Parquet na camada Bronze.
# MAGIC - O Parquet é escolhido pelo seu suporte a colunas e sua eficiência tanto em termos de espaço quanto em desempenho de leitura e escrita.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mover os arquivos processados para pasta processado

# COMMAND ----------

from pyspark.sql import functions as F
#distinct_filenames.unpersist() -> Isso remove o DataFrame do cache/memória caso ele tenha sido persistido antes.
# Mover os arquivos processados para o caminho lz_path_out
# Nota: A operação de mover arquivos diretamente não é suportada pelo DataFrame API do Spark.
# É necessário utilizar o dbutils.fs.mv para mover os arquivos manualmente após o processamento.

# Primeiro, verifique se há arquivos a serem movidos
if distinct_filenames.select("filename").distinct().count() > 0: #Existe pelo menos um arquivo que foi lido?
    filenames = distinct_filenames.select("filename").distinct().collect() #Isso transforma os nomes dos arquivos em uma lista Python: ["vendas_01.csv", "vendas_02.csv", ...]

    for row in filenames:
        src_path = row.filename #para cada arquivo na lista
        dbutils.fs.mv(lz_path_in + "/" + src_path, lz_path_out) #move o arquivo da pasta de entrada para a pasta de saída


# COMMAND ----------

# MAGIC %md
# MAGIC ## fluxo
# MAGIC - Arquivo chega na Landing Zone
# MAGIC - Spark lê o arquivo
# MAGIC - Aplica schema
# MAGIC - Adiciona metadados (filename)
# MAGIC - Escreve na Bronze em Parquet particionado
# MAGIC - Move o arquivo original para a pasta de saída
# MAGIC Esse é o padrão recomendado em arquiteturas Lakehouse.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Evidências

# COMMAND ----------

# MAGIC %fs ls /Volumes/lhdw/data/landingzone_vendas_processar/

# COMMAND ----------

# MAGIC %fs ls /Volumes/lhdw/data/landingzone_vendas_processado/

# COMMAND ----------

# MAGIC %fs ls /Volumes/lhdw/data/bronze/

# COMMAND ----------

# MAGIC %md
# MAGIC ### A opção de gravar dados no modo "append" 
# MAGIC
# MAGIC Permite adicionar novos dados a um arquivo existente, sem substituir ou excluir os dados já presentes. 
# MAGIC
# MAGIC No caso específico do código fornecido, a linha de código comentada `df_vendas.withColumn("Ano", year("Data")) \ .withColumn("Mes", month("Data")) \ .write.mode("append").partitionBy("Ano", "Mes").parquet(bronze_path)` indica que os dados do DataFrame `df_vendas` serão adicionados ao arquivo Parquet existente no caminho `bronze_path`, mantendo a estrutura de particionamento por ano e mês.
# MAGIC
# MAGIC Essa opção é útil quando se deseja adicionar novos dados a um conjunto de dados já existente, como por exemplo, quando novas vendas são registradas e precisam ser incorporadas ao conjunto de dados de vendas existente.

# COMMAND ----------

#df_vendas.withColumn("Ano", year("Data")) \
#         .withColumn("Mes", month("Data")) \
#         .write.mode("append").partitionBy("Ano", "Mes").parquet(bronze_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gerenciar o uso de memória 
# MAGIC Em PySpark, é importante gerenciar o uso de memória eficientemente, especialmente quando se trabalha com grandes conjuntos de dados. Para isso, você pode usar alguns comandos específicos que ajudam a liberar memória, remover objetos em cache ou persistidos e forçar a coleta de lixo.
# MAGIC
# MAGIC Cache é quando o Spark guarda um DataFrame na memória (RAM) para evitar recalcular tudo de novo.
# MAGIC
# MAGIC **1. Limpar cache:**
# MAGIC PySpark pode armazenar dados em cache para melhorar o desempenho de operações repetidas com persist(). Para liberar esses dados, você pode usar o comando unpersist().

# COMMAND ----------

#Se você quiser que um DataFrame fique armazenado na memória, você precisa pedir explicitamente usando:
df_vendas.cache() ou df_vendas.persist()

# Exemplo de como liberar o cache de um DataFrame

df_vendas.unpersist()

# O comando unpersist() remove o DataFrame do cache, liberando a memória associada. Ele é especialmente útil quando você já não precisa mais dos dados persistidos.

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Limpar todos os dados em cache:**
# MAGIC
# MAGIC Se houver vários DataFrames em cache, você pode limpá-los todos de uma vez.

# COMMAND ----------

# Limpar todos os dados em cache

spark.catalog.clearCache()

# clearCache() limpa o cache de todos os objetos em cache no SparkSession atual, liberando uma quantidade significativa de memória quando múltiplos DataFrames estão sendo reutilizados.

# COMMAND ----------

# MAGIC %md
# MAGIC **3. Forçar coleta de lixo:**
# MAGIC
# MAGIC O Python possui um coletor de lixo que remove objetos não referenciados da memória. Você pode forçar a coleta de lixo para liberar memória.

# COMMAND ----------

import gc
gc.collect()

#Comentário: Esse comando força o coletor de lixo a executar imediatamente, liberando a memória de objetos Python que não estão mais em uso.

# COMMAND ----------

# MAGIC %md
# MAGIC **4. Liberar variáveis manualmente:**
# MAGIC
# MAGIC Se você criou variáveis grandes que não são mais necessárias, você pode removê-las explicitamente.

# COMMAND ----------

del df_vendas

# O comando del remove o objeto da memória. Isso é útil quando você tem grandes DataFrames ou objetos Python que já não são necessários.

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC **Dicas adicionais:**
# MAGIC - Evite cachear DataFrames desnecessários.
# MAGIC
# MAGIC **Resumo**
# MAGIC
# MAGIC - **Para uma limpeza rápida e geral**: Use spark.catalog.clearCache().
# MAGIC - **Para liberar memória de DataFrames específicos**: Use df.unpersist().
# MAGIC - **Para remover variáveis específicas**: Use del.
# MAGIC - **Para uma solução completa**: Reinicie o cluster.