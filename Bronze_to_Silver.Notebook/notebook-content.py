# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3001ca4f-ef06-42d6-99c3-ac997acd945a",
# META       "default_lakehouse_name": "caresync_project_lh",
# META       "default_lakehouse_workspace_id": "4bdf91ac-7b88-43aa-82c5-77b7d0505ab0",
# META       "known_lakehouses": [
# META         {
# META           "id": "3001ca4f-ef06-42d6-99c3-ac997acd945a"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "2e8fcb1e-3593-463c-8de9-aee43b17e5a2",
# META       "known_warehouses": [
# META         {
# META           "id": "2e8fcb1e-3593-463c-8de9-aee43b17e5a2",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

landing_timestamp = '2026-10-26_02:51:44'
table_name= 'hospitals'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as fn
bronze_df= spark.read.table(f'caresync_project_lh.bronze.{table_name}')\
.filter(f"landing_timestamp == '{landing_timestamp}'")

display(bronze_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

updated_df=bronze_df.withColumn('ingest_timestamp',fn.current_timestamp()).withColumn('updated_timestamp',fn.current_timestamp())
display(updated_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

updated_df.write.format('delta')\
.mode('overwrite')\
.saveAsTable(f'caresync_project_lh.silver.{table_name}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from caresync_project_lh.silver.hospitals

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
