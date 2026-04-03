# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Criar os diretórios (DBFS)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Criar uma pasta no Databricks para vinculo
# MAGIC --Criando diretorios no DBFS
# MAGIC --dbutils.fs.mkdirs("/mnt/lhdw/landingzone/vendas/processar")
# MAGIC --dbutils.fs.mkdirs("/mnt/lhdw/landingzone/vendas/processado")
# MAGIC --dbutils.fs.mkdirs("/mnt/lhdw/bronze")
# MAGIC --dbutils.fs.mkdirs("/mnt/lhdw/silver")
# MAGIC --dbutils.fs.mkdirs("/mnt/lhdw/gold")
# MAGIC
# MAGIC --Na versão gratuita do Databricks, o DBFS é um pouco limitado.
# MAGIC CREATE CATALOG IF NOT EXISTS lhdw;
# MAGIC    CREATE SCHEMA IF NOT EXISTS lhdw.data;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS lhdw.data.landingzone_vendas_processar;
# MAGIC    CREATE VOLUME IF NOT EXISTS lhdw.data.landingzone_vendas_processado;
# MAGIC    CREATE VOLUME IF NOT EXISTS lhdw.data.bronze;
# MAGIC    CREATE VOLUME IF NOT EXISTS lhdw.data.silver;
# MAGIC    CREATE VOLUME IF NOT EXISTS lhdw.data.gold;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Resumo das Diferenças
# MAGIC #####Criar um Diretório: 
# MAGIC   Simplesmente cria uma nova pasta no DBFS para organizar seus dados internos.
# MAGIC
# MAGIC #####Montar um Diretório: 
# MAGIC Conecta um armazenamento de objetos externo ao DBFS, permitindo acesso e manipulação de dados externos como se estivessem localmente no Databricks.
# MAGIC
# MAGIC Documentação de apoio
# MAGIC
# MAGIC https://learn.microsoft.com/pt-br/azure/databricks/files/#work-with-files-in-dbfs-mounts-and-dbfs-root

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conhecendo os diretórios (DBFS)

# COMMAND ----------

# MAGIC %fs ls 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conhecendo os diretórios mnt (DBFS)

# COMMAND ----------

# MAGIC %fs ls /mnt/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conhecendo os diretórios mnt/lhdw (DBFS)

# COMMAND ----------

# MAGIC %fs ls /mnt/lhdw/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conhecendo os diretórios mnt/lhdw/landingzone (DBFS)

# COMMAND ----------

# MAGIC %fs ls /mnt/lhdw/landingzone/

# COMMAND ----------

# MAGIC %md
# MAGIC ####Conhecendo os diretórios /FileStore/ (DBFS)

# COMMAND ----------

# MAGIC %fs ls /FileStore/

# COMMAND ----------

# MAGIC %md
# MAGIC ####Conhecendo os diretórios /databricks-datasets/ (DBFS)

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/

# COMMAND ----------

# MAGIC %md
# MAGIC ###Apagando pastas/diretorios
# MAGIC Processar apenas se necessario

# COMMAND ----------

#dbutils.fs.rm("dbfs:/mnt/lhdw", recurse=True)
#dbutils.fs.rm("dbfs:/mnt/lhdw/landingzone", recurse=True)
#dbutils.fs.rm("dbfs:/mnt/lhdw/bronze", recurse=True)
#dbutils.fs.rm("dbfs:/mnt/lhdw/silver", recurse=True)
#dbutils.fs.rm("dbfs:/mnt/lhdw/gold", recurse=True)
