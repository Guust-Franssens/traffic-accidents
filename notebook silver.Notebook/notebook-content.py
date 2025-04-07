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
# META           "id": "fd76b43f-2865-40ef-adad-8250f1fbee19"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # In this silver notebook we will do renaming, and simple transformations

# CELL ********************

%pip install pyproj

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyproj import Transformer
from pyspark.sql.functions import regexp_replace, col, udf, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_traffic_accidents_bronze.traffic_accidents")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

column_mapping = {
	"DT_YEAR_COLLISION": "year",
	"DT_MONTH_COLLISION": "month",
	"DT_TIME": "hour",
	"CD_NIS": "nis_code",
	"TX_RGN_COLLISION_FR": "region_fr",
	"TX_RGN_COLLISION_NL": "region_nl",
	"TX_PROV_COLLISION_FR": "province_fr",
	"TX_PROV_COLLISION_NL": "province_nl",
	"TX_MUNTY_COLLISION_FR": "municipality_fr",
	"TX_MUNTY_COLLISION_NL": "municipality_nl",
	"MS_X_COORD": "x_coord_belgian_lambert",
	"MS_Y_COORD": "y_coord_belgian_lambert",
	"CD_CROSSWAY": "crossway_id",
	"TX_CROSSWAY_FR": "crossway_fr",
	"TX_CROSSWAY_NL": "crossway_nl",
	"CD_WEATHER": "weather_id",
	"TX_WEATHER_FR": "weather_fr",
	"TX_WEATHER_NL": "weather_nl",
	"CD_ROAD_CONDITION": "road_condition_id",
	"TX_ROAD_CONDITION_FR": "road_condition_fr",
	"TX_ROAD_CONDITION_NL": "road_condition_nl",
	"CD_BUILD_UP_AREA": "build_up_area_id",
	"TX_BUILD_UP_AREA_FR": "build_up_area_fr",
	"TX_BUILD_UP_AREA_NL": "build_up_area_nl",
	"CD_LIGHT_CONDITION": "light_condition_id",
	"TX_LIGHT_CONDITION_FR": "light_condition_fr",
	"TX_LIGHT_CONDITION_NL": "light_condition_nl",
	"CD_ROAD_TYPE": "road_type_id",
	"CD_ROAD_TYPE_FR": "road_type_fr",
	"CD_ROAD_TYPE_NL": "road_type_nl",
	"CD_CLASS_ACCIDENTS": "accident_type_id",
	"TX_CLASS_ACCIDENTS_FR": "accident_type_fr",
	"TX_CLASS_ACCIDENTS_NL": "accident_type_nl",
	"CD_ROAD_USR_TYPE1": "first_road_user_id",
	"TX_ROAD_USR_TYPE1_FR": "first_road_user_fr",
	"TX_ROAD_USR_TYPE1_NL": "first_road_user_nl",
	"CD_ROAD_USR_TYPE2": "second_road_user_id",
	"TX_ROAD_USR_TYPE2_FR": "second_road_user_fr",
	"TX_ROAD_USR_TYPE2_NL": "second_road_user_nl",
	"CD_COLLISION_TYPE": "collision_type_id",
	"TX_COLLISON_TYPE_FR": "collision_type_fr",
	"TX_COLLISION_TYPE_NL": "collision_type_nl",
	"CD_OBSTACLES": "obstacle_id",
	"TX_OBSTACLES_FR": "obstacle_fr",
	"TX_OBSTACLES_NL": "obstacle_nl",
}
df = df.select(*[col(old).alias(new) for old, new in column_mapping.items()])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Simple transformations

# CELL ********************

# any hour not between 0 and 23 should be set to null
df = df.withColumn("hour", when((col("hour") < 0) | (col("hour") > 23), None).otherwise(col("hour")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

columns = [
    struct_type.name for struct_type in df.schema 
    if struct_type.name.endswith("_id") and struct_type.dataType == StringType()
]
for column in columns:
    df.groupby(column).count().orderBy("count", ascending=False).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn("weather_id", regexp_replace("weather_id", "/", ""))\
    .withColumn("road_condition_id", regexp_replace("road_condition_id", "/", ""))\
    .withColumn("collision_type_id", regexp_replace("collision_type_id", "A", "99"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

transformer = Transformer.from_crs("EPSG:31370", "EPSG:4326", always_xy=True)
def convert_to_lat_lon(x, y):
    if x is not None and y is not None:
        lon, lat = transformer.transform(float(x), float(y))
        return lat, lon
    return None, None

# Register the UDF
convert_to_lat_lon_udf = udf(lambda x, y: convert_to_lat_lon(x, y), StructType([
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True)
]))

# Apply the UDF to the PySpark DataFrame
df = df.withColumn("coordinates", convert_to_lat_lon_udf(df["x_coord_belgian_lambert"], df["y_coord_belgian_lambert"]))

# Split the Coordinates column into Latitude and Longitude
df = df.withColumn("latitude", df["coordinates"].getItem("latitude")) \
                   .withColumn("longitude", df["coordinates"].getItem("longitude")) \
                   .drop("coordinates") \
                   .drop("x_coord_belgian_lambert") \
                   .drop("y_coord_belgian_lambert")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Schema enforcing

# CELL ********************

df = df.withColumn("year", col("year").cast("int")) \
	.withColumn("month", col("month").cast("int")) \
	.withColumn("hour", col("hour").cast("int")) \
	.withColumn("nis_code", col("nis_code").cast("int")) \
	.withColumn("region_fr", col("region_fr").cast("string")) \
	.withColumn("region_nl", col("region_nl").cast("string")) \
	.withColumn("province_fr", col("province_fr").cast("string")) \
	.withColumn("province_nl", col("province_nl").cast("string")) \
	.withColumn("municipality_fr", col("municipality_fr").cast("string")) \
	.withColumn("municipality_nl", col("municipality_nl").cast("string")) \
	.withColumn("crossway_id", col("crossway_id").cast("int")) \
	.withColumn("crossway_fr", col("crossway_fr").cast("string")) \
	.withColumn("crossway_nl", col("crossway_nl").cast("string")) \
	.withColumn("weather_id", col("weather_id").cast("int")) \
	.withColumn("weather_fr", col("weather_fr").cast("string")) \
	.withColumn("weather_nl", col("weather_nl").cast("string")) \
	.withColumn("road_condition_id", col("road_condition_id").cast("int")) \
	.withColumn("road_condition_fr", col("road_condition_fr").cast("string")) \
	.withColumn("road_condition_nl", col("road_condition_nl").cast("string")) \
	.withColumn("build_up_area_id", col("build_up_area_id").cast("int")) \
	.withColumn("build_up_area_fr", col("build_up_area_fr").cast("string")) \
	.withColumn("build_up_area_nl", col("build_up_area_nl").cast("string")) \
	.withColumn("light_condition_id", col("light_condition_id").cast("int")) \
	.withColumn("light_condition_fr", col("light_condition_fr").cast("string")) \
	.withColumn("light_condition_nl", col("light_condition_nl").cast("string")) \
	.withColumn("road_type_id", col("road_type_id").cast("int")) \
	.withColumn("road_type_fr", col("road_type_fr").cast("string")) \
	.withColumn("road_type_nl", col("road_type_nl").cast("string")) \
	.withColumn("accident_type_id", col("accident_type_id").cast("int")) \
	.withColumn("accident_type_fr", col("accident_type_fr").cast("string")) \
	.withColumn("accident_type_nl", col("accident_type_nl").cast("string")) \
	.withColumn("first_road_user_id", col("first_road_user_id").cast("int")) \
	.withColumn("first_road_user_fr", col("first_road_user_fr").cast("string")) \
	.withColumn("first_road_user_nl", col("first_road_user_nl").cast("string")) \
	.withColumn("second_road_user_id", col("second_road_user_id").cast("int")) \
	.withColumn("second_road_user_fr", col("second_road_user_fr").cast("string")) \
	.withColumn("second_road_user_nl", col("second_road_user_nl").cast("string")) \
	.withColumn("collision_type_id", col("collision_type_id").cast("int")) \
	.withColumn("collision_type_fr", col("collision_type_fr").cast("string")) \
	.withColumn("collision_type_nl", col("collision_type_nl").cast("string")) \
	.withColumn("obstacle_id", col("obstacle_id").cast("int")) \
	.withColumn("obstacle_fr", col("obstacle_fr").cast("string")) \
	.withColumn("obstacle_nl", col("obstacle_nl").cast("string")) \
	.withColumn("latitude", col("latitude").cast("float")) \
	.withColumn("longitude", col("longitude").cast("float"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("traffic_accidents")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
