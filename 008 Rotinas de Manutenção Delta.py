# Databricks notebook source
# MAGIC %md
# MAGIC ### Resumo das Rotinas de Manutenção:
# MAGIC - **Vacuum**: Remove arquivos antigos para liberar espaço.
# MAGIC - **Optimize**: Combina arquivos pequenos para melhorar a performance de leitura.
# MAGIC - **Z-Ordering**: Organiza os dados para melhorar o desempenho em consultas filtradas.
# MAGIC - **Update/Delete**: Permite operações eficientes de modificação de dados.
# MAGIC - **History/Time Travel**: Audita e acessa versões anteriores dos dados.
# MAGIC - **Compaction**: Agrupa arquivos pequenos para melhorar a eficiência de leitura.
# MAGIC - Essas práticas de manutenção são fundamentais para gerenciar com eficiência um Delta Lake, mantendo a performance e a integridade dos dados.

# COMMAND ----------

# MAGIC %md
# MAGIC Manter um** Delta Lake** bem gerenciado é fundamental para garantir a performance, a integridade dos dados e o uso eficiente de recursos. Aqui estão as principais rotinas de manutenção do Delta Lake, quando, como e por que usá-las:
# MAGIC
# MAGIC ### 1. Vacuum
# MAGIC **Quando usar**: Para remover arquivos antigos que não são mais necessários, como aqueles gerados por operações de update, merge ou delete.
# MAGIC
# MAGIC **Por que usar**: O Delta Lake mantém versões antigas de dados (histórico) para fornecer recursos como time travel e rollback. Com o tempo, esses arquivos antigos podem consumir muito espaço em disco. O vacuum remove esses arquivos, liberando espaço.
# MAGIC
# MAGIC **Recomendação**: Evite configurar o período de retenção abaixo de 7 dias sem considerar as implicações no time travel. O padrão de 7 dias é seguro para manter a possibilidade de recuperação de dados e, ao mesmo tempo, limpar arquivos obsoletos.

# COMMAND ----------

from delta.tables import DeltaTable

# Define os caminhos de armazenamento no Data Lake
gold_path = "/mnt/lhdw/gold/vendas_delta/fato_vendas"

# Disable retention duration check
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
# Executa vacuum para remover arquivos não utilizados com mais de 7 dias
delta_table = DeltaTable.forPath(spark, gold_path)
delta_table.vacuum(7)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Optimize
# MAGIC **Quando usar**: Para otimizar o layout dos arquivos armazenados no Delta Lake, especialmente após muitas operações de escrita ou atualização que podem gerar arquivos pequenos.
# MAGIC
# MAGIC **Por que usar**: O Delta Lake pode acabar com muitos arquivos pequenos após operações de escrita ou merge. Isso pode prejudicar o desempenho das consultas devido ao overhead de leitura de muitos arquivos. O optimize combina arquivos pequenos em arquivos maiores, melhorando a leitura e o processamento.
# MAGIC
# MAGIC **Recomendação**: Use o optimize em intervalos regulares ou após grandes operações de escrita, para garantir que o layout dos dados continue eficiente. Para melhorar ainda mais o desempenho, o optimize pode ser combinado com Z-Ordering.

# COMMAND ----------

from delta.tables import *
# Define os caminhos de armazenamento no Data Lake
gold_path = "/mnt/lhdw/gold/vendas_delta/fato_vendas"
# Otimiza a tabela combinando arquivos pequenos em arquivos maiores
delta_table = DeltaTable.forPath(spark, gold_path)
delta_table.optimize().executeCompaction()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Z-Ordering
# MAGIC **Quando usar:** Para otimizar as consultas que realizam filtragens frequentes em determinadas colunas, como colunas de data ou chave.
# MAGIC
# MAGIC **Por que usar:** O Z-Ordering melhora o desempenho da leitura ao organizar fisicamente os dados em disco com base em uma coluna ou conjunto de colunas, reduzindo o tempo necessário para buscar os registros filtrados.
# MAGIC
# MAGIC **Recomendação:** Use o Z-Ordering em colunas que são frequentemente usadas em cláusulas de filtro para melhorar a leitura de dados relacionados. Combine isso com o optimize para ter dados organizados de forma mais eficiente no disco.

# COMMAND ----------


from delta.tables import DeltaTable, DeltaOptimizeBuilder
# Executa optimize com Z-Ordering na coluna "DataVenda"

# Load the Delta table
delta_table = DeltaTable.forPath(spark, gold_path)

# Execute Z-Ordering optimization on the column "DataVenda"
#delta_table.optimize().#executeZOrderBy("DataVenda").#execute()

spark.sql(f"""OPTIMIZE delta.`{gold_path}` ZORDER BY (DataVenda)""")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Update and Delete Operations (UPSERT)
# MAGIC **Quando usar:** Para modificar ou remover dados diretamente em uma tabela Delta, sem precisar sobrescrever a tabela inteira.
# MAGIC
# MAGIC **Por que usar:** O Delta Lake permite que você faça operações upsert (combinação de update e insert) e delete, o que é fundamental em pipelines de dados que exigem dados corrigidos, removidos ou atualizados continuamente, como tabelas de fatos ou históricos.
# MAGIC
# MAGIC **Recomendação**: Essas operações são úteis para ajustar os dados de maneira eficiente, especialmente quando o volume de dados não é massivo ou quando os dados precisam ser corrigidos frequentemente.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import lit, max, current_timestamp

# Carregar a tabela Delta
delta_table = DeltaTable.forPath(spark, "/mnt/lhdw/gold/vendas_delta/dim_fabricante")

# Calcular o próximo valor de SK_Fabricante
next_sk = delta_table.toDF().select(max("sk_fabricante")).collect()[0][0] + 1
#Aqui a delta table é transformada em dataframe novamente pois o objeto delta não permite muitas manipulações ou alterações

# Criar uma nova linha para ser inserida
new_row = spark.createDataFrame([
    (8, "Novo Fabricante", next_sk)  # Supondo que 8 seja um IDFabricante de exemplo
], ["IDFabricante", "Fabricante", "sk_fabricante"])

# Adicionar a coluna DataAtualizacao com o tipo correto
new_row = new_row.withColumn("data_atualizacao", current_timestamp())

# Executar a operação de inserção
delta_table.alias("target").merge(
    new_row.alias("source"),
    "target.IDFabricante = source.IDFabricante"
).whenNotMatchedInsertAll().execute()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Exemplo de Update

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp

# Exemplo de update
delta_table = DeltaTable.forPath(spark, "/mnt/lhdw/gold/vendas_delta/dim_fabricante")
delta_table.update(
    condition = col("IDFabricante") == 7,  # Condição de update
    set = { 
        "Fabricante": "'VanArsdel Inc.'",
        "data_atualizacao": current_timestamp()
    }  # Atualização de valor
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Exemplo de Delete

# COMMAND ----------

# Exemplo de delete
delta_table.delete(condition = col("IDFabricante") == 8)



# COMMAND ----------

# MAGIC %md
# MAGIC ###Exemplo de UPSERT

# COMMAND ----------

from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import monotonically_increasing_id, current_timestamp

# Carregue o DataFrame de origem (novos dados)
df_silver = spark.read.format("parquet").load("/mnt/lhdw/silver/vendas/")

#Nome tabela destino
tb_destino = "dim_fabricante"

# Extrair produtos únicos para a dimensão Fabricante    
dim_fabricante_df = df_silver.select(
    "IDFabricante", "Fabricante").dropDuplicates()

# Adicionar chave substituta (surrogate keys)
dim_fabricante_df = dim_fabricante_df.withColumn("sk_fabricante", monotonically_increasing_id()+1)
# Carregue o DataFrame de destino (tabela existente)
df_target = DeltaTable.forPath(spark, "/mnt/lhdw/gold/vendas_delta/dim_fabricante")

# Realize a operação de merge
df_target.alias("target").merge(
    dim_fabricante_df.alias("source"),
    "target.IDFabricante = source.IDFabricante"
).whenMatchedUpdate(set={
    "Fabricante": "source.Fabricante",
    "SK_Fabricante": "source.SK_Fabricante",
    "data_atualizacao": current_timestamp()
}).whenNotMatchedInsert(values={
    "Fabricante": "source.Fabricante",
    "IDFabricante": "source.IDFabricante",
    "SK_Fabricante": "source.SK_Fabricante",
    "data_atualizacao": current_timestamp()
}).execute()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. History e Time Travel
# MAGIC **Quando usar:** Para auditar mudanças na tabela Delta ou para acessar versões anteriores dos dados.
# MAGIC
# MAGIC **Por que usar:** O Delta Lake mantém um log de transações que permite rastrear todas as modificações feitas na tabela. Isso é útil para auditoria e recuperação de dados em um ponto anterior no tempo.
# MAGIC
# MAGIC **Recomendação:** Use o histórico e o time travel para depurar problemas ou restaurar versões anteriores dos dados quando necessário. No entanto, lembre-se de usar o vacuum para gerenciar a quantidade de histórico mantido.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Acessando o histórico da tabela:

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import *

# Visualizar o histórico da tabela Delta
history_df = DeltaTable.forPath(spark, "/mnt/lhdw/gold/vendas_delta/dim_fabricante").history()

display(history_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Acessar versões antigas (Time Travel):

# COMMAND ----------

# Acessar a versão 5 da tabela
df = spark.read.format("delta").option("versionAsOf", 5).load("/mnt/lhdw/gold/vendas_delta/dim_fabricante")
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Restaurar uma versão antiga de tabela Delta

# COMMAND ----------

from delta.tables import DeltaTable

# Inicializar DeltaTable
delta_table_path = "dbfs:/mnt/lhdw/gold/vendas_delta/dim_fabricante"
delta_table = DeltaTable.forPath(spark, delta_table_path)

# Restaurar a tabela para a versão 
delta_table.restoreToVersion(2)

# Apresentar a tabela
display(spark.read.format("delta").load(delta_table_path))


# COMMAND ----------

# MAGIC %md
# MAGIC **7. Compaction (Compactação)**
# MAGIC
# MAGIC **Quando usar**: Para agrupar arquivos pequenos resultantes de múltiplas operações de escrita em arquivos maiores, melhorando a performance de leitura.
# MAGIC
# MAGIC **Por que usar**: Com o tempo, as operações de escrita podem gerar muitos arquivos pequenos, resultando em um número excessivo de partições pequenas, o que afeta a performance. A compactação agrupa esses arquivos pequenos para melhorar o desempenho de leitura e reduzir o overhead.
# MAGIC
# MAGIC **Recomendação:** Execute operações de compactação regularmente ou após grandes operações de escrita para manter o layout dos dados em arquivos otimizados.

# COMMAND ----------

# Reparticionando a tabela fato_vendas por ano e mês e salvando o resultado
# Agrupando os arquivos pequenos em arquivos maiores
df = spark.read.format("delta").load("/mnt/lhdw/gold/vendas_delta/fato_vendas")
df.repartition(2).write.format("delta").mode("overwrite").save("dbfs:/mnt/lhdw/gold/vendas_delta/fato_vendas_repart")

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC **1. Particionamento com `repartition`**
# MAGIC
# MAGIC O `repartition` é utilizado para aumentar ou reduzir o número de partições de forma uniforme, redistribuindo os dados através de um shuffle. Útil quando você precisa de mais paralelismo.

# COMMAND ----------

# Exemplo de criação de um DataFrame 
df_geo = spark.read.format("delta").load("/mnt/lhdw/gold/vendas_delta/dim_geografia")  
# Lendo um arquivo Parquet como exemplo

# Verificar número de partições iniciais
print(f"Número de partições iniciais: {df_geo.rdd.getNumPartitions()}")

# Redefinir para 2 partições usando repartition
df_reparticionado = df_geo.repartition(2)

# Persiste os dados em uma tabela Delta
df_reparticionado.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save("/mnt/lhdw/gold/vendas_delta/geo_repart")

# Verificar número de partições após repartition
print(f"Número de partições após repartition: {df_reparticionado.rdd.getNumPartitions()}")


# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC **2. Reparticionamento com Coluna Específica**
# MAGIC
# MAGIC Se o dataset contiver uma coluna-chave (como Regiao ou Data), você pode usar o repartition para redistribuir os dados com base em uma coluna específica, o que pode ser útil para garantir que os dados relacionados sejam processados juntos.

# COMMAND ----------

df_geo = spark.read.format("delta").load("/mnt/lhdw/gold/vendas_delta/dim_geografia")  
# Reparticionando os dados pela coluna "Regiao"
df_reparticionado_geo = df_geo.repartition(10, "Regiao")


# Persiste os dados em uma tabela Delta
df_reparticionado_geo.write.partitionBy("Regiao").format("delta").option("overwriteSchema", "true").mode("overwrite").save("/mnt/lhdw//gold/vendas_delta/geo_regiao")

# Verificar número de partições após reparticionar pela coluna "Regiao"
print(f"Número de partições após reparticionar pela coluna Regiao: {df_reparticionado_geo.rdd.getNumPartitions()}")


# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC **3. Redução de Partições com `coalesce`**
# MAGIC O `coalesce` é utilizado para reduzir o número de partições sem realizar um shuffle, o que é útil quando se deseja consolidar partições e reduzir o número de tarefas, como ao escrever para o disco.

# COMMAND ----------

# Usando coalesce para reduzir as partições para 5
df_coalesced = df_geo.repartition(100).coalesce(5)

# Persiste os dados em uma tabela Delta
df_coalesced.write.format("delta").mode("overwrite").save("/mnt/lhdw/gold/vendas_delta/geo_coaslece")

# Persiste os dados em uma tabela Delta
df_coalesced.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save("/mnt/lhdw/gold/vendas_delta/geo_coalesce")

# Verificar número de partições após o coalesce
print(f"Número de partições após coalesce: {df_coalesced.rdd.getNumPartitions()}")


# COMMAND ----------

# MAGIC %md
# MAGIC **Resumo das Técnicas:**
# MAGIC
# MAGIC - `repartition(n)`: Redistribui os dados uniformemente em n partições. Útil para aumentar o número de partições ou garantir uma melhor distribuição.
# MAGIC - `repartition(col)`: Redistribui os dados com base em uma ou mais colunas, garantindo que valores semelhantes estejam na mesma partição.
# MAGIC - `coalesce(n)`: Reduz o número de partições sem um shuffle, consolidando as partições existentes de forma eficiente.
# MAGIC
# MAGIC **Quando Usar:**
# MAGIC
# MAGIC - `repartitio`n: Use quando quiser aumentar o número de partições ou redistribuir os dados de forma mais equilibrada, especialmente quando houver um número elevado de partições pequenas.
# MAGIC - `coalesce`: Use quando estiver reduzindo o número de partições para minimizar o shuffle e consolidar os dados, especialmente ao escrever dados para armazenamento.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Evidências de repartição/Compactação

# COMMAND ----------

# MAGIC %fs ls /mnt/lhdw/gold/vendas_delta/

# COMMAND ----------

# MAGIC %fs ls /mnt/lhdw/gold/vendas_delta/fato_vendas_repart/

# COMMAND ----------

# MAGIC %fs ls /mnt/lhdw/gold/vendas_delta/geo_repart/

# COMMAND ----------

# MAGIC %fs ls /mnt/lhdw/gold/vendas_delta/geo_regiao/

# COMMAND ----------

# MAGIC %fs ls /mnt/lhdw/gold/vendas_delta/geo_coalesce/