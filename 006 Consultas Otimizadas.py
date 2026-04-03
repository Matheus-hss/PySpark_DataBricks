# Databricks notebook source
# MAGIC %md
# MAGIC ### Iniciando SparkSession

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession with the required configurations for Delta Lake
spark = SparkSession.builder \
    .appName("Leitura Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Número de Núcleos

# COMMAND ----------

#Numero de núcleos rodando no cluster
num_cores = spark._jsc.sc().getExecutorMemoryStatus().keySet().size()
print(f"Número de núcleos no cluster: {num_cores}")


# COMMAND ----------

# MAGIC %md
# MAGIC ####Evidência de Fato Vendas

# COMMAND ----------

from delta.tables import DeltaTable
delta_path = "dbfs:/mnt/lhdw/gold/vendas_delta"
delta_table = DeltaTable.forPath(spark, f"{delta_path}/fato_vendas")
delta_table.toDF().show() 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Evidência de Dim Produto

# COMMAND ----------

from delta.tables import DeltaTable
delta_path = "dbfs:/mnt/lhdw/gold/vendas_delta"
dim_produto_df = DeltaTable.forPath(spark, f"{delta_path}/dim_produto")
dim_produto_df.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Evidência de Dim Geografia

# COMMAND ----------

from delta.tables import DeltaTable
delta_path = "dbfs:/mnt/lhdw/gold/vendas_delta"
dim_geografia_df = DeltaTable.forPath(spark, f"{delta_path}/dim_geografia")
dim_geografia_df.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Evidência de Dim Categoria

# COMMAND ----------

from delta.tables import DeltaTable
delta_path = "dbfs:/mnt/lhdw/gold/vendas_delta"
dim_categoria_df = DeltaTable.forPath(spark, f"{delta_path}/dim_categoria")
dim_categoria_df.toDF().show()


# COMMAND ----------

# MAGIC %md
# MAGIC ####Evidência de Dim Cliente

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import DeltaTable
delta_path = "dbfs:/mnt/lhdw/gold/vendas_delta"
#dim_cliente_df = DeltaTable.forPath(spark, f"{delta_path}/dim_cliente")
dim_cliente_df = spark.read.format("delta").load(delta_path+"/dim_cliente")
# Conte o número de linhas
#dim_cliente_df.count()
display(dim_cliente_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ####Evidência de Dim Fabricante

# COMMAND ----------

from delta.tables import DeltaTable
delta_path = "dbfs:/mnt/lhdw/gold/vendas_delta"
dim_fabricante_df = DeltaTable.forPath(spark, f"{delta_path}/dim_fabricante")
dim_fabricante_df.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Evidência de Dim Segmento

# COMMAND ----------

from delta.tables import DeltaTable
delta_path = "dbfs:/mnt/lhdw/gold/vendas_delta"
dim_segmento_df = DeltaTable.forPath(spark, f"{delta_path}/dim_segmento")
dim_segmento_df.toDF().show()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC **Dicas para Otimizar a Performance**
# MAGIC > **Particionamento**: Definimos partições adequadas para evitar leituras desnecessárias e melhorar a performance de consultas.
# MAGIC
# MAGIC > **Codec de compressão**: Usamos Snappy, pois oferece boa performance de compressão e descompressão.
# MAGIC
# MAGIC > **Shuffle partitions**: Definimos um valor fixo para spark.sql.shuffle.partitions para melhorar o paralelismo durante operações como joins e agregações.
# MAGIC > Além disso, podemos explorar técnicas como cache para tabelas pequenas (dimensões) que são frequentemente acessadas, e broadcast join para otimizar joins entre a tabela Fato e as tabelas de dimensões

# COMMAND ----------

# MAGIC %md
# MAGIC ### Otimização de Leitura com predicate pushdown:
# MAGIC - Certifique-se de que as consultas estão aproveitando o predicate pushdown, o que significa que os filtros são aplicados diretamente ao ler os dados, melhorando a eficiência.
# MAGIC

# COMMAND ----------

# Utilizando predicate pushdown para otimizar a consulta
# Caminho para o diretório dos arquivos Delta
gold_path = "dbfs:/mnt/lhdw/gold/vendas_delta/fato_vendas"
df_filtrado = spark.read.format("delta").load(gold_path).filter("Ano = 2012 AND Mes = 10")

display(df_filtrado)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Broadcast join
# MAGIC **Explicação:**
# MAGIC **1. Broadcast Join:**
# MAGIC
# MAGIC - O broadcast() é aplicado às tabelas de <b>dimensões</b> (dim_produto_df e dim_cliente_df). Isso replica as tabelas de dimensão para todos os nós, permitindo que as junções sejam realizadas localmente em cada nó, sem necessidade de comunicação entre nós, o que melhora a performance em clusters distribuídos.
# MAGIC
# MAGIC **2. Junção com Broadcast:**
# MAGIC
# MAGIC - As junções são feitas entre as colunas de chave original (IDProduto, IDCliente) e as tabelas de dimensão para obter as chaves substitutas (SK_Produto, SK_Cliente).
# MAGIC
# MAGIC **3. Particionamento:**
# MAGIC
# MAGIC - Adicionamos colunas de Ano e Mês para otimizar o armazenamento da tabela de fatos e melhorar o desempenho em consultas temporais. A tabela é particionada por essas colunas.
# MAGIC
# MAGIC **Vantagens do Broadcast Join:**
# MAGIC
# MAGIC - Reduz a movimentação de dados durante a operação de junção, pois as dimensões pequenas são replicadas para todos os nós.
# MAGIC - Aumenta a performance quando as tabelas de dimensão são significativamente menores que a tabela de fatos, o que é o caso comum em arquiteturas de data warehouse.
# MAGIC
# MAGIC **Desvantagens do Broadcast Join:**
# MAGIC - Limitação de Memória: O DataFrame menor deve caber na memória de todos os nós. Se o DataFrame for muito grande, pode causar erros de falta de memória

# COMMAND ----------

from pyspark.sql.functions import year, sum, broadcast,desc
from pyspark.sql import SparkSession

# Leitura das tabelas Delta
vendas_df = spark.read.format("delta").load("/mnt/lhdw/gold/vendas_delta/fato_vendas")
categoria_df = spark.read.format("delta").load("/mnt/lhdw/gold/vendas_delta/dim_categoria")

# Usar broadcast para a tabela categoria
 
categoria_df = broadcast(categoria_df)

# Realizar o join entre as tabelas
joined_df = vendas_df.join(categoria_df, vendas_df.sk_categoria == categoria_df.sk_categoria)

# Agrupar por categoria e ano e calcular a soma do total de vendas
resultado_df = joined_df.groupBy("Categoria", "Ano")\
        .agg(sum("TotalVendas").alias("TotalVendas"))\
        .orderBy("Ano",desc("TotalVendas"))


display(resultado_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Melhorias de Performance**
# MAGIC
# MAGIC Filtros de Partição: Se você souber quais partições específicas deseja ler, aplicar filtros nas partições pode reduzir significativamente o tempo de leitura.
# MAGIC Reparticionamento: Se os dados estiverem distribuídos de forma desigual, você pode usar repartition() para redistribuir o DataFrame com base em uma coluna-chave.

# COMMAND ----------

# MAGIC %md
# MAGIC # Dicas de Performance com PySpark
# MAGIC
# MAGIC ## 1. Use DataFrame/Dataset em vez de RDD
# MAGIC Os DataFrames e Datasets são mais eficientes que os RDDs, pois incluem otimizações automáticas e um motor de execução otimizado. Eles permitem um melhor gerenciamento de memória e execução mais rápida.
# MAGIC
# MAGIC ## 2. Evite UDFs (User Defined Functions)
# MAGIC As UDFs podem ser lentas porque não são otimizadas pelo Catalyst Optimizer do Spark. Sempre que possível, use as funções internas do Spark SQL, que são mais eficientes.
# MAGIC
# MAGIC ## 3. Use `coalesce()` em vez de `repartition()`
# MAGIC O `coalesce()` é mais eficiente que o `repartition()` para reduzir o número de partições, pois evita o shuffle de dados.
# MAGIC
# MAGIC ## 4. Cache de Dados
# MAGIC Cache os DataFrames que são reutilizados várias vezes em suas operações. Isso evita a re-leitura dos dados do disco e melhora o desempenho.
# MAGIC
# MAGIC ## 5. Reduza Operações de Shuffle
# MAGIC Operações de shuffle, como `groupByKey` e `reduceByKey`, podem ser caras. Use `mapPartitions` e `reduceByKey` sempre que possível para minimizar o shuffle.
# MAGIC
# MAGIC ## 6. Ajuste o Número de Partições
# MAGIC Ajuste o número de partições para equilibrar a carga de trabalho entre os executores. Um número inadequado de partições pode levar a um uso ineficiente dos recursos.
# MAGIC
# MAGIC ## 7. Use Formatos de Dados Serializados
# MAGIC Formatos de dados como Parquet e ORC são mais eficientes para leitura e escrita, pois são compactados e otimizados para consultas.
# MAGIC
# MAGIC ## 8. Ajuste as Configurações do Spark
# MAGIC Ajuste configurações como `spark.executor.memory`, `spark.executor.cores` e `spark.sql.shuffle.partitions` para otimizar o uso de recursos.
# MAGIC
# MAGIC ## 9. Utilize a Adaptive Query Execution (AQE)
# MAGIC A AQE permite que o Spark ajuste dinamicamente o plano de execução das consultas com base nas estatísticas de tempo de execução, melhorando o desempenho.
# MAGIC
# MAGIC Implementar essas práticas pode ajudar a melhorar significativamente o desempenho de suas aplicações PySpark. Se precisar de mais detalhes ou tiver outras perguntas, estou aqui para ajudar! 🤜🤛