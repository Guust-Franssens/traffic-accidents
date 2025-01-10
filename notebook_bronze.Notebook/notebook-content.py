# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fd76b43f-2865-40ef-adad-8250f1fbee19",
# META       "default_lakehouse_name": "lh_traffic_accidents_bronze",
# META       "default_lakehouse_workspace_id": "8f10a8d6-9370-49e5-b3c8-82dee04a1fec",
# META       "known_lakehouses": [
# META         {
# META           "id": "fd76b43f-2865-40ef-adad-8250f1fbee19"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pathlib import Path

import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

path = Path("/lakehouse/default/Files/OPENDATA_MAP_2017-2022.xlsx")
path.exists()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pandas_df = pd.read_excel(path)
spark_df = spark.createDataFrame(pandas_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark_df.write.mode("overwrite").format("delta").saveAsTable("traffic_accidents")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
