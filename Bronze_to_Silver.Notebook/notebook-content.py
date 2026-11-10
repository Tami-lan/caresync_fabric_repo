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
load_type ='FULL'
merge_keys='patient_id'

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

#### Preparing the load type metadata #####

target_table=f'caresync_project_lh.silver.{table_name}'
normalised_load_type= (load_type or 'FULL').strip().upper()
merge_key_list= [ key.strip() for key in merge_keys.split(',') if key.strip()]

####Checking if merge type then merging key are present in the source column
if normalised_load_type == 'MERGE':
    if not merge_key_list:
        raise ValueError("merge_keys must be provided when load_type is 'MERGE'.")
    missing_merge_keys=[ key for key in merge_key_list if key not in bronze_df.columns]
    
    if missing_merge_keys:
        raise ValueError(f"merge_keys not found in source data: {missing_merge_keys}")


####ADD the ingest timestamp and updated timestamp for tracking and auditing purpose
updated_df=bronze_df.withColumn('ingest_timestamp',fn.current_timestamp()).withColumn('updated_timestamp',fn.current_timestamp())
display(updated_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

####Loading source data to target table
if normalised_load_type == 'FULL':
    updated_df.write.format('delta').mode('overwrite').saveAsTable(target_table)

elif normalised_load_type == 'APPEND':
    updated_df.write.format('delta').mode('append').saveAsTable(target_table)

elif normalised_load_type == 'MERGE':

    target_exists=spark.catalog.tableExists(target_table)

    if not target_exists:
        updated_df.write.format('delta').mode('overwrite').saveAsTable(target_table)
    
    else:
        source_view='silver_merge_source'
        updated_df.createOrReplaceTempView(source_view)

        merge_condition =' AND '.join(
            [f"target.`{key}` = source.`{key}`" for key in merge_key_list]
        )

        update_assign = ', '.join(
            [ f"target.`{column}` = source.`{column}`" for column in updated_df.columns 
            if column != 'ingest_timestamp']
        )  

        insert_column= ', '.join([ f"`{column}`" for column in updated_df.columns])
        insert_values= ', '.join([ f"source.`{column}`" for column in updated_df.columns])

#### scd TYPE 1 ####
        merge_sql=f""" 
                       MERGE INTO {target_table} as target
                       USING {source_view} as source 
                       ON {merge_condition}
                       WHEN MATCHED THEN UPDATE SET {update_assign}
                       WHEN NOT MATCHED THEN INSERT ({insert_column})
                       VALUES ({insert_values})

        
        """.strip()

        spark.sql(merge_sql)


    
else:
    raise ValueError("load_type must be one of: FULL, APPEND, MERGE")
  



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

record_count=updated_df.count()
print(record_count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.notebook.exit(str(record_count))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
