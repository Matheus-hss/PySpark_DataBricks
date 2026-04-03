# Databricks notebook source
# MAGIC %md
# MAGIC ###Criando Banco de dados (lhdw_vendas)

# COMMAND ----------

# Criar o banco de dados
spark.sql("CREATE DATABASE IF NOT EXISTS lhdw_vendas")

# Usar o banco de dados
spark.sql("USE lhdw_vendas")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Criando tabelas Delta

# COMMAND ----------

delta_table_path = "/Volumes/lhdw/data/gold/dim_produto"

# Registre a tabela Delta no catálogo do Spark
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS lhdw_vendas.dim_produto
    USING DELTA
    LOCATION '{delta_table_path}'
""")

# Verifique se a tabela foi criada
spark.sql("SHOW TABLES").show()

# COMMAND ----------

delta_table_path = "dbfs:/mnt/lhdw/gold/vendas_delta/dim_categoria"
# Registre a tabela Delta no catálogo do Spark
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS lhdw_vendas.dim_categoria
    USING DELTA
    LOCATION '{delta_table_path}'
""")

# Verifique se a tabela foi criada
spark.sql("SHOW TABLES").show()

# COMMAND ----------

delta_table_path = "dbfs:/mnt/lhdw/gold/vendas_delta/dim_segmento"
# Registre a tabela Delta no catálogo do Spark
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS lhdw_vendas.dim_segmento
    USING DELTA
    LOCATION '{delta_table_path}'
""")

# Verifique se a tabela foi criada
spark.sql("SHOW TABLES").show()

# COMMAND ----------

delta_table_path = "dbfs:/mnt/lhdw/gold/vendas_delta/dim_fabricante"
# Registre a tabela Delta no catálogo do Spark
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS lhdw_vendas.dim_fabricante
    USING DELTA
    LOCATION '{delta_table_path}'
""")

# Verifique se a tabela foi criada
spark.sql("SHOW TABLES").show()

# COMMAND ----------

delta_table_path = "dbfs:/mnt/lhdw/gold/vendas_delta/dim_geografia"
# Registre a tabela Delta no catálogo do Spark
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS lhdw_vendas.dim_geografia
    USING DELTA
    LOCATION '{delta_table_path}'
""")

# Verifique se a tabela foi criada
spark.sql("SHOW TABLES").show()

# COMMAND ----------

delta_table_path = "dbfs:/mnt/lhdw/gold/vendas_delta/dim_cliente"
# Registre a tabela Delta no catálogo do Spark
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS lhdw_vendas.dim_cliente
    USING DELTA
    LOCATION '{delta_table_path}'
""")

# Verifique se a tabela foi criada
spark.sql("SHOW TABLES").show()

# COMMAND ----------

delta_table_path = "dbfs:/mnt/lhdw/gold/vendas_delta/fato_vendas"
# Registre a tabela Delta no catálogo do Spark
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS lhdw_vendas.fato_vendas
    USING DELTA
    LOCATION '{delta_table_path}'
""")

# Verifique se a tabela foi criada
spark.sql("SHOW TABLES").show()

# COMMAND ----------

import gc
spark.catalog.clearCache()
# Coletar lixo após operações pesadas para liberar memória
gc.collect()

# COMMAND ----------

