from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import random

# to be able to connect to mongodb atlas
connection_string = "mongodb+srv://yahya:wuOCBUNsQ856HP3Z@cluster0.7wbr9.mongodb.net/healthcare.test?retryWrites=true&w=majority"

# Create Spark session with Cassandra connection options using the secure connect bundle
spark = SparkSession.builder \
  .appName("Healthcare Monitoring") \
  .master("local[*]") \
  .config("spark.mongodb.spark.enabled", "true") \
  .config("spark.mongodb.read.connection.uri", connection_string) \
  .config("spark.mongodb.write.connection.uri", connection_string) \
  .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2") \
  .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# SparkContext from sparkSession to reduce written code only
sc = spark.sparkContext

# Get Data from kafka topic
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "my-topic") \
  .load()

################################ Processing will be here #####################################

# convert from json format to string
kafka_values = df.selectExpr("CAST(value AS STRING)")
# {'id': '621e2e8e67b776a24055b564', 'temperature': '-1.466659067979661', 'date': '2021-05-24', 'hour': '0.0', 'calories': '89.04', 'distance': '98.3', 'bpm': '66.87476280834915', 'steps': '134.0', 'age': '<30', 'gender': 'MALE', 'bmi': '<19'}
# all string
parsed_df = kafka_values.selectExpr("explode(from_json(value, 'array<struct<id:string,temperature:string,date:string,hour:string,calories:string,distance:string,bpm:string,\
steps:string,age:string,gender:string,bmi:string>>')) AS data")
df = parsed_df.select(col("data.*"))

# cast all columns from string to specified types
df_casted = df.withColumn("date", col("date").cast(DateType())) \
              .withColumn("hour", col("hour").cast(DoubleType())) \
              .withColumn("temperature", col("temperature").cast(DoubleType())) \
              .withColumn("bpm", col("bpm").cast(DoubleType())) \
              .withColumn("calories", col("calories").cast(DoubleType())) \
              .withColumn("distance", col("distance").cast(DoubleType())) \
              .withColumn("steps", col("steps").cast(IntegerType()))

# Drop rows with more than 6 nulls
total_columns = len(df_casted.columns)
df_dropped = df_casted.dropna(thresh=(total_columns-6))


# Function to calculate the mode for categorical columns
def get_mode(df, column):
    mode_df = df.groupBy(column).count().orderBy(F.desc("count")).limit(1)
    return mode_df.collect()[0][0] if mode_df.count() > 0 else None

def process_batch(df, epoch_id):
    # Compute mean for numerical columns
    print('df.count(): ', df.count())
    if df.count() != 0:

        for name in ["calories", "distance", "steps"]:
            percentile25 = df.approxQuantile(name, [0.25], 0.01)[0]
            # print('percentile25: ', percentile25)
            percentile75 = df.approxQuantile(name, [0.75], 0.01)[0]
            iqr = percentile75 - percentile25
            upper_limit = percentile75 + 3 * iqr
            lower_limit = percentile25 - 3 * iqr
            
            df = df.withColumn(name, F.when(col(name) > upper_limit, upper_limit)
                                    .when(col(name) < lower_limit, lower_limit)
                                    .otherwise(col(name)))
        
        bpm_mean = df.agg(mean("bpm")).first()[0]
        bpm_stddev = df.agg(stddev("bpm")).first()[0]
        upper_limit_bpm = bpm_mean + 3 * bpm_stddev
        lower_limit_bpm = bpm_mean - 3 * bpm_stddev
        
        df_outlier = df.withColumn("bpm", F.when(col("bpm") > upper_limit_bpm, upper_limit_bpm)
                            .when(col("bpm") < lower_limit_bpm, lower_limit_bpm)
                            .otherwise(col("bpm")))
        
        bmi_list = df.select("bmi") \
        .distinct().rdd \
        .flatMap(lambda x: x) \
        .filter(lambda x: x != '"NaN"') \
        .collect()
        print(bmi_list)

        bmi_list_random_value = random.choice(bmi_list)  # Choose a random value from 'bmi' list
        df_batch = df_outlier.withColumn(
            "bmi", when(col("bmi") == '"NaN"', bmi_list_random_value).otherwise(col("bmi"))
        )
    else:
        df_batch = df
    bpm_mean = df_batch.select(avg("bpm")).first()[0] or 0
    calories_mean = df_batch.select(avg("calories")).first()[0] or 0
    distance_mean = df_batch.select(avg("distance")).first()[0] or 0
    steps_mean = df_batch.select(avg("steps")).first()[0] or 0
    temperature_mean = df_batch.select(avg("temperature")).first()[0] or 0

        # check if null replace with 0

    bpm_mean = bpm_mean if bpm_mean else 0
    calories_mean = calories_mean if calories_mean else 0
    distance_mean = distance_mean if distance_mean else 0
    steps_mean = steps_mean if steps_mean else 0
    temperature_mean = temperature_mean if temperature_mean else 0


    # Compute mode for categorical columns
    gender_mode = get_mode(df_batch, "gender")
    age_mode = get_mode(df_batch, "age")
    bmi_mode = get_mode(df_batch, "bmi")

    # check if null replace with <30
    print(distance_mean)
    print(type(distance_mean))

    gender_mode =gender_mode if gender_mode else "MALE"
    age_mode = age_mode if age_mode else "<30"
    bmi_mode = bmi_mode if bmi_mode else "<30"
    print('age_mode: ', age_mode)
    print('gender_mode: ', gender_mode)
    print('bmi_mode: ', bmi_mode)
    print('type ', type(age_mode))


    # Fill null values using the computed means and modes
    # def processbatch:
    df_filled = df_batch.fillna({
        'bpm': bpm_mean,
        'calories': calories_mean,
        'distance': distance_mean,
        'steps': steps_mean,
        'gender': gender_mode,
        'age': age_mode,
        'temperature': temperature_mean
    })\
    .withColumn("age", when(col("age") == '"NaN"', age_mode).otherwise(col("age"))) \
    .withColumn("gender", when(col("gender") == '"NaN"', gender_mode).otherwise(col("gender")))

    ######################### write to mongoDB atlas ##################
    df_filled.write.format("mongodb").mode('append')\
    .option('database', 'healthcare').option('collection', 'streaming').save()

    ####################### to console ################################
    # df_filled.write.format("console").mode("append").save()
    pass
    # return df_filled

df_dropped = df_dropped.dropna(subset=["id", "date", "hour"])

# Directly concatenate 'date' and the integer part of 'hour'
df_dropped = df_dropped.withColumn(
    "time",
    to_timestamp(
        concat(
            col("date").cast("string"),             # Cast date to string
            lit(" "),                               # Add a space
            lpad(floor(col("hour")).cast("string"), 2, "0")  # Extract integer part of hour, pad to 2 digits
        ), 
        "yyyy-MM-dd HH"  # Specify the timestamp format
    )
)

################################# Write Streaming Data to mongoDB atlas ########################################
# query = df_dropped.writeStream \
#   .foreachBatch(lambda batch_df, _: replace_outliers(batch_df))\
#   .foreachBatch(lambda batch_df, _: fill_nulls_with_random(batch_df)) \
#   .foreachBatch(process_batch) \
#   .format("mongodb") \
#   .option("spark.mongodb.write.connection.uri", connection_string) \
#   .option("database", "healthcare") \
#   .option("collection", "test") \
#   .option("checkpointLocation", "/tmp/checkpoint/test") \
#   .outputMode("append") \
#   .start()

########################## start processing the stream ############################################
query = df_dropped.writeStream \
  .foreachBatch(process_batch) \
  .start()

query.awaitTermination()
