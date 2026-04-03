# Databricks notebook source
# MAGIC %md
# MAGIC ### Camada Gold (Delta): Criação de Fatos e Dimensões

# COMMAND ----------

from pyspark.sql import SparkSession

# Cria a SparkSession
spark = SparkSession.builder \
    .appName("Carga Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# COMMAND ----------

# Define os caminhos de armazenamento no Data Lake
bronze_path = "/Volumes/lhdw/data/bronze/"

silver_path = "/Volumes/lhdw/data/silver/"

gold_fato_path = "/Volumes/lhdw/data/gold/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ler dados Camada Silver
# MAGIC Filtrado pela maior data na tabela fato

# COMMAND ----------

from pyspark.sql.functions import date_sub, lit

try:
    # Ler a maior data de venda da tabela fato_vendas
    max_data_venda = spark.read.format("delta").load(gold_fato_path) \
                              .selectExpr("max(DataVenda) as MaxDataVenda") \
                              .collect()[0]["MaxDataVenda"]
except:
    # Primeira execução - tabela não existe ainda
    max_data_venda = None

#.selectExpr("max(DataVenda) as MaxDataVenda" -> Selecione a maior data da coluna DataVenda e chame essa coluna de MaxDataVenda.
#.collect()[0]["MaxDataVenda"] -> Coletar o resultado da consulta em formato de lista

display(max_data_venda)

# Carregar dados da Silver filtrando pela DataVenda maior que a obtida acima
if max_data_venda:
    df_silver = spark.read.format("parquet").load(silver_path) \
                              .filter(f"Data > '{max_data_venda}'")
else:
    # Primeira execução - carregar todos os dados
    df_silver = spark.read.format("parquet").load(silver_path)

df_silver.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação da Dimensão Produto

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, current_timestamp
#Nome tabela destino

tb_destino = "dim_produto"

# Extrair produtos únicos para a dimensão Produto
dim_produto_df = df_silver.select(
    "IDProduto", "Produto", "Categoria").dropDuplicates()

# Adicionar chave substituta (surrogate keys)
dim_produto_df = dim_produto_df.withColumn("sk_produto", monotonically_increasing_id()+1) \
                               .withColumn("data_atualizacao", current_timestamp())


# Escrever DimProduto no formato Delta
dim_produto_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{gold_path}/{tb_destino}")
display(dim_produto_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação da Dimensão Categoria

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id
#Nome tabela destino

tb_destino = "dim_categoria"

# Extrair Categorias únicas para a dimensão Categoria
dim_categoria_df = df_silver.select(
    "Categoria").dropDuplicates()

# Adicionar chave substituta (surrogate keys)
dim_categoria_df = dim_categoria_df.withColumn("sk_categoria", monotonically_increasing_id()+1)\
                                   .withColumn("data_atualizacao", current_timestamp())

# Escrever DimCatgoria no formato Parquet, particionando por Categoria
dim_categoria_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{gold_path}/{tb_destino}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação da Dimensão Segmento

# COMMAND ----------

#Nome tabela destino

tb_destino = "dim_segmento"

# Extrair Segmentos únicos para a dimensão Segmentos
dim_segmento_df = df_silver.select(
   "Segmento").dropDuplicates()

# Adicionar chave substituta (surrogate keys)
dim_segmento_df = dim_segmento_df.withColumn("sk_segmento", monotonically_increasing_id()+1) \
                                 .withColumn("data_atualizacao", current_timestamp())


# Escrever DimSegmento no formato Parquet
dim_segmento_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{gold_path}/{tb_destino}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação da Dimensão Fabricante

# COMMAND ----------

#Nome tabela destino
tb_destino = "dim_fabricante"

# Extrair produtos únicos para a dimensão Fabricante    
dim_fabricante_df = df_silver.select(
    "IDFabricante", "Fabricante").dropDuplicates()

# Adicionar chave substituta (surrogate keys)
dim_fabricante_df = dim_fabricante_df.withColumn("sk_fabricante", monotonically_increasing_id()+1)\
                                      .withColumn("data_atualizacao", current_timestamp())


# Escrever DimFabricante no formato Delta
dim_fabricante_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{gold_path}/{tb_destino}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação da Dimensão Geografia

# COMMAND ----------

#Nome tabela destino
tb_destino = "dim_geografia"

# Extrair Geografia  únicos para a dimensão Geografia
dim_geografia_df = df_silver.select(
     "Cidade", "Estado", "Regiao", "Distrito", "Pais", "CodigoPostal"
).dropDuplicates()

# Adicionar chave substituta
dim_geografia_df = dim_geografia_df.withColumn("sk_geografia", monotonically_increasing_id()+1) \
                                   .withColumn("data_atualizacao", current_timestamp())


# Escrever DimGeografia no formato Parquet
dim_geografia_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{gold_path}/{tb_destino}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação da Dimensão Cliente

# COMMAND ----------

#Nome tabela destino
tb_destino = "dim_cliente"

from pyspark.sql.functions import col, monotonically_increasing_id
# Passo 1 - Extrair clientes únicos para a dimensão Cliente
dim_cliente_df = df_silver.select(
    "IDCliente", "Nome", "Email", "Cidade", "Estado", "Regiao", "Distrito", "Pais", "CodigoPostal"
).dropDuplicates()

# Passo 2 - Realizar o join para obter a SK_Geografia
dim_cliente_com_sk_df = dim_cliente_df.alias("cliente") \
    .join(dim_geografia_df.alias("geografia"), 
          (col("cliente.Cidade") == col("geografia.Cidade")) &
          (col("cliente.Estado") == col("geografia.Estado")) &
          (col("cliente.Regiao") == col("geografia.Regiao")) &
          (col("cliente.Distrito") == col("geografia.Distrito")) &
          (col("cliente.Pais") == col("geografia.Pais")) &
          (col("cliente.CodigoPostal") == col("geografia.CodigoPostal")), 
          "left") \
    .select("cliente.IDCliente", "cliente.Nome", "cliente.Email", "geografia.sk_geografia")

# Passo 3 - Adicionar chave substituta
dim_cliente_com_sk_df = dim_cliente_com_sk_df.withColumn("sk_cliente", monotonically_increasing_id()+1) \
                                             .withColumn("data_atualizacao", current_timestamp())


# Passo 4 - Selecionar colunas específicas
dim_cliente_com_sk_df = dim_cliente_com_sk_df.select("IDCliente", "Nome","Email", "sk_geografia", "sk_cliente","data_atualizacao")

# Passo 5 - Escrever DimCliente no formato Delta
dim_cliente_com_sk_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{gold_path}/{tb_destino}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação de Tabela Fato

# COMMAND ----------

#Nome tabela destino
tb_destino = "fato_vendas"

from pyspark.sql.functions import broadcast,year, month
# Juntar dados da Silver com tabelas de dimensões para obter as chaves substitutas
fato_vendas_df = df_silver.alias("s") \
    .join(broadcast(dim_produto_df.select("IDProduto", "sk_produto").alias("dprod")), "IDProduto") \
    .join(broadcast(dim_categoria_df.select("Categoria", "sk_categoria").alias("dcat")), "Categoria") \
    .join(broadcast(dim_segmento_df.select("Segmento", "sk_segmento").alias("dseg")), "Segmento") \
    .join(broadcast(dim_fabricante_df.select("Fabricante", "sk_fabricante").alias("dfab")), "Fabricante") \
    .join(broadcast(dim_cliente_com_sk_df.select("IDCliente", "sk_cliente").alias("dcli")), "IDCliente") \
    .select(
        col("s.Data").alias("DataVenda"),
        "sk_produto",
        "sk_categoria",
        "sk_segmento",
        "sk_fabricante",
        "sk_cliente",
        "Unidades",
        col("s.PrecoUnitario"),
        col("s.CustoUnitario"),
        col("s.TotalVendas"),
        current_timestamp().alias("data_atualizacao")
    )

# Escrever tabela Fato no formato Delta, particionando por DataVenda (ano e mês)
fato_vendas_df.withColumn("Ano", year("DataVenda")) \
             .withColumn("Mes", month("DataVenda")) \
             .write.format("delta") \
             .mode("append")\
             .option("mergeSchema", "true")\
             .option("MaxRecordsPerFile", 1000000)\
             .partitionBy("Ano", "Mes")\
             .save(f"{gold_path}/{tb_destino}")

#colocamos .mode("append") para podermos ver os dados de 2012 e 2013             

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demonstração de informação total vendas por ano

# COMMAND ----------

from pyspark.sql.functions import sum, col
gold_path = "/Volumes/lhdw/data/gold/"
# Consulta da fato vendas por categoria ano a ano com a soma de total de vendas

resultado = spark.read.format("delta").load(f"{gold_path}/fato_vendas") \
    .groupBy("Ano") \
    .agg(sum("TotalVendas").alias("SomaTotalVendas")) \
    .orderBy(col("Ano"), col("SomaTotalVendas").desc())

display(resultado)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limpeza de Memória

# COMMAND ----------

import gc
# Coletar lixo após operações pesadas para liberar memória
gc.collect()