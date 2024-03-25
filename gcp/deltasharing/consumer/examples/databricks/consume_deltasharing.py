# Databricks notebook source
# MAGIC %md # Deltasharing med skyporten

# COMMAND ----------

# MAGIC %md # Setup

# COMMAND ----------

# MAGIC %sh pip install delta-sharing

# COMMAND ----------

import delta_sharing

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC I manually created a volume to write files, since shared clusters have limited file system permissions

# COMMAND ----------

deltashare = dbutils.secrets.get("skyportensuppeverket", "unitsdeltashare")
deltasharepath = "/Volumes/training/suppeverket/skyportensupperverket/skyportendeltashare/skyporten_config.share"
dbutils.fs.put(deltasharepath, deltashare, True)

# COMMAND ----------

client = delta_sharing.SharingClient(deltasharepath)
client.list_all_tables()

# COMMAND ----------

profile_path = deltasharepath
share_name = 'units'
schema_name= 'silver'
table_name = 'municipalities'
share_uc_path = f"{profile_path}#{share_name}.{schema_name}.{table_name}"
print("share_uc_path", share_uc_path)
kommuner = (
    spark.read.format("deltaSharing")
    .load(share_uc_path)
    .limit(10)
)
kommuner.display()

# COMMAND ----------


