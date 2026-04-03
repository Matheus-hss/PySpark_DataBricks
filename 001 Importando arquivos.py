# Databricks notebook source
# MAGIC %md
# MAGIC ###Importar arquivos para DBFS Landingzone
# MAGIC
# MAGIC Para baixar um arquivo do GitHub e salvá-lo no Databricks, você pode seguir os passos abaixo:
# MAGIC

# COMMAND ----------

import urllib.request

# URL do arquivo no GitHub
url = 'https://github.com/andrerosa77/trn-pyspark/raw/main/dados_2013.csv'

# Caminho de destino no Volume (Unity Catalog)
volume_path = '/Volumes/lhdw/data/landingzone_vendas_processar/dados_2013.csv'

# Baixar o arquivo diretamente para o Volume
urllib.request.urlretrieve(url, volume_path)

print(f"Arquivo baixado e salvo em: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Evidência do Arquivo criado

# COMMAND ----------

dbutils.fs.ls("/Volumes/lhdw/data/landingzone_vendas_processar/")

# COMMAND ----------

# MAGIC %fs ls /Volumes/lhdw/data/landingzone_vendas_processar/