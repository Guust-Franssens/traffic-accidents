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
from pyspark.sql.functions import regexp_replace, col, udf
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
	"DT_TIME": "time",
	"CD_NIS": "nis_code",
	"TX_RGN_COLLISION_FR": "description_region_in_french",
	"TX_RGN_COLLISION_NL": "description_region_in_dutch",
	"TX_PROV_COLLISION_FR": "description_province_in_french",
	"TX_PROV_COLLISION_NL": "description_province_in_dutch",
	"TX_MUNTY_COLLISION_FR": "description_municipality_in_french",
	"TX_MUNTY_COLLISION_NL": "description_municipality_in_dutch",
	"MS_X_COORD": "x_coord_belgian_lambert",
	"MS_Y_COORD": "y_coord_belgian_lambert",
	"CD_CROSSWAY": "crossway_id",
	"TX_CROSSWAY_FR": "description_crossway_in_french",
	"TX_CROSSWAY_NL": "description_crossway_in_dutch",
	"CD_WEATHER": "weather_id",
	"TX_WEATHER_FR": "description_weather_in_french",
	"TX_WEATHER_NL": "description_weather_in_dutch",
	"CD_ROAD_CONDITION": "road_condition_id",
	"TX_ROAD_CONDITION_FR": "description_road_condition_in_french",
	"TX_ROAD_CONDITION_NL": "description_road_condition_in_dutch",
	"CD_BUILD_UP_AREA": "build_up_area_id",
	"TX_BUILD_UP_AREA_FR": "description_build_up_area_in_french",
	"TX_BUILD_UP_AREA_NL": "description_build_up_area_in_dutch",
	"CD_LIGHT_CONDITION": "light_condition_id",
	"TX_LIGHT_CONDITION_FR": "description_light_condition_in_french",
	"TX_LIGHT_CONDITION_NL": "description_light_condition_in_dutch",
	"CD_ROAD_TYPE": "road_type_id",
	"CD_ROAD_TYPE_FR": "description_road_type_in_french",
	"CD_ROAD_TYPE_NL": "description_road_type_in_dutch",
	"CD_CLASS_ACCIDENTS": "accident_type_id",
	"TX_CLASS_ACCIDENTS_FR": "description_accident_type_in_french",
	"TX_CLASS_ACCIDENTS_NL": "description_accident_type_in_dutch",
	"CD_ROAD_USR_TYPE1": "first_road_user_id",
	"TX_ROAD_USR_TYPE1_FR": "description_first_road_user_in_french",
	"TX_ROAD_USR_TYPE1_NL": "description_first_road_user_in_dutch",
	"CD_ROAD_USR_TYPE2": "second_road_user_id",
	"TX_ROAD_USR_TYPE2_FR": "description_second_road_user_in_french",
	"TX_ROAD_USR_TYPE2_NL": "description_second_road_user_in_dutch",
	"CD_COLLISION_TYPE": "collision_type_id",
	"TX_COLLISON_TYPE_FR": "description_collision_type_in_french",
	"TX_COLLISION_TYPE_NL": "description_collision_type_in_dutch",
	"CD_OBSTACLES": "obstacle_id",
	"TX_OBSTACLES_FR": "description_obstacle_in_french",
	"TX_OBSTACLES_NL": "description_obstacle_in_dutch",
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
    .withColumn("collision_type_id", regexp_replace("road_condition_id", "/", ""))

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
	.withColumn("time", col("time").cast("int")) \
	.withColumn("nis_code", col("nis_code").cast("int")) \
	.withColumn("description_region_in_french", col("description_region_in_french").cast("string")) \
	.withColumn("description_region_in_dutch", col("description_region_in_dutch").cast("string")) \
	.withColumn("description_province_in_french", col("description_province_in_french").cast("string")) \
	.withColumn("description_province_in_dutch", col("description_province_in_dutch").cast("string")) \
	.withColumn("description_municipality_in_french", col("description_municipality_in_french").cast("string")) \
	.withColumn("description_municipality_in_dutch", col("description_municipality_in_dutch").cast("string")) \
	.withColumn("crossway_id", col("crossway_id").cast("int")) \
	.withColumn("description_crossway_in_french", col("description_crossway_in_french").cast("string")) \
	.withColumn("description_crossway_in_dutch", col("description_crossway_in_dutch").cast("string")) \
	.withColumn("weather_id", col("weather_id").cast("int")) \
	.withColumn("description_weather_in_french", col("description_weather_in_french").cast("string")) \
	.withColumn("description_weather_in_dutch", col("description_weather_in_dutch").cast("string")) \
	.withColumn("road_condition_id", col("road_condition_id").cast("int")) \
	.withColumn("description_road_condition_in_french", col("description_road_condition_in_french").cast("string")) \
	.withColumn("description_road_condition_in_dutch", col("description_road_condition_in_dutch").cast("string")) \
	.withColumn("build_up_area_id", col("build_up_area_id").cast("int")) \
	.withColumn("description_build_up_area_in_french", col("description_build_up_area_in_french").cast("string")) \
	.withColumn("description_build_up_area_in_dutch", col("description_build_up_area_in_dutch").cast("string")) \
	.withColumn("light_condition_id", col("light_condition_id").cast("int")) \
	.withColumn("description_light_condition_in_french", col("description_light_condition_in_french").cast("string")) \
	.withColumn("description_light_condition_in_dutch", col("description_light_condition_in_dutch").cast("string")) \
	.withColumn("road_type_id", col("road_type_id").cast("int")) \
	.withColumn("description_road_type_in_french", col("description_road_type_in_french").cast("string")) \
	.withColumn("description_road_type_in_dutch", col("description_road_type_in_dutch").cast("string")) \
	.withColumn("accident_type_id", col("accident_type_id").cast("int")) \
	.withColumn("description_accident_type_in_french", col("description_accident_type_in_french").cast("string")) \
	.withColumn("description_accident_type_in_dutch", col("description_accident_type_in_dutch").cast("string")) \
	.withColumn("first_road_user_id", col("first_road_user_id").cast("int")) \
	.withColumn("description_first_road_user_in_french", col("description_first_road_user_in_french").cast("string")) \
	.withColumn("description_first_road_user_in_dutch", col("description_first_road_user_in_dutch").cast("string")) \
	.withColumn("second_road_user_id", col("second_road_user_id").cast("int")) \
	.withColumn("description_second_road_user_in_french", col("description_second_road_user_in_french").cast("string")) \
	.withColumn("description_second_road_user_in_dutch", col("description_second_road_user_in_dutch").cast("string")) \
	.withColumn("collision_type_id", col("collision_type_id").cast("int")) \
	.withColumn("description_collision_type_in_french", col("description_collision_type_in_french").cast("string")) \
	.withColumn("description_collision_type_in_dutch", col("description_collision_type_in_dutch").cast("string")) \
	.withColumn("obstacle_id", col("obstacle_id").cast("int")) \
	.withColumn("description_obstacle_in_french", col("description_obstacle_in_french").cast("string")) \
	.withColumn("description_obstacle_in_dutch", col("description_obstacle_in_dutch").cast("string")) \
	.withColumn("latitude", col("latitude").cast("float")) \
	.withColumn("longitude", col("longitude").cast("float"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.mode("overwrite").format("delta").saveAsTable("traffic_accidents")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
