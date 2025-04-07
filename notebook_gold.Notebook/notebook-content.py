# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8bde6bbe-9843-4c15-a5d3-8fb47cac8c82",
# META       "default_lakehouse_name": "lh_traffic_accidents_silver",
# META       "default_lakehouse_workspace_id": "8f10a8d6-9370-49e5-b3c8-82dee04a1fec",
# META       "known_lakehouses": [
# META         {
# META           "id": "8bde6bbe-9843-4c15-a5d3-8fb47cac8c82"
# META         },
# META         {
# META           "id": "e4cb559c-1581-4df9-adad-207ba129910e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_traffic_accidents_silver.traffic_accidents")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

id_columns = [column for column in df.columns if "_id" in column]
tables = {id_column.replace("_id", ""): [column for column in df.columns if id_column.replace("_id", "") in column] for id_column in id_columns}
tables["location"] = ["nis_code", "region_fr", "region_nl", "province_fr", "province_nl", "municipality_fr", "municipality_nl"]
tables["traffic_accidents"] = ["year", "month", "hour", "nis_code", "latitude", "longitude"] + id_columns
tables

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(id_columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for table_name, columns in tables.items():
    print(f"Creating table '{table_name}' with columns {columns}")
    df.select(columns).dropDuplicates().write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"lh_traffic_accidents_gold.{table_name}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
