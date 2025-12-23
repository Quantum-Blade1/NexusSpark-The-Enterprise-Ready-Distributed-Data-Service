from pyspark.sql import SparkSession
from pyspark.sql.functions import col, log as spark_log, exp, when
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler, StandardScaler
import time

# Create a Spark session
spark = SparkSession.builder \
    .master("local") \
    .appName("NASA Meteorite Landings Prediction Standalone (Log-Transformed)") \
    .getOrCreate()

# Set the logging level to ERROR
spark.sparkContext.setLogLevel("ERROR")

# Measure the time taken to load the dataset
load_start_time = time.time()
df = spark.read.csv("/home/rit-admin/BDAProject/nasaML.csv", header=True, inferSchema=True)
load_end_time = time.time()
print("Time to load dataset: %s seconds" % (load_end_time - load_start_time))

# Filter meteorites that fell after the year 1900 and drop rows with null values
filtered_df = (
    df.filter(col("year") > 1900)
      .dropna(subset=["year", "reclat", "reclong", "mass"])
)

# Create a new column for the log-transformed mass (add 1 to avoid log(0))
df_log = filtered_df.withColumn("log_mass", spark_log(col("mass") + 1))

# Select columns to keep (including original mass)
df_features = df_log.select("year", "reclat", "reclong", "mass", "log_mass")

# Prepare the data for the linear regression model
assembler = VectorAssembler(
    inputCols=["year", "reclat", "reclong"],
    outputCol="features_unscaled"
)
data_unscaled = assembler.transform(df_features)

# Scale the features
scaler = StandardScaler(
    inputCol="features_unscaled",
    outputCol="features",
    withMean=True,
    withStd=True
)
scaler_model = scaler.fit(data_unscaled)
data = scaler_model.transform(data_unscaled)

# Split the data into training and test sets
train_data, test_data = data.randomSplit([0.8, 0.2])

# Create a linear regression model to predict log_mass
lr = LinearRegression(featuresCol="features", labelCol="log_mass")

# Measure the time taken to train the model
train_start_time = time.time()
lr_model = lr.fit(train_data)
train_end_time = time.time()
print("Training time (standalone): %s seconds" % (train_end_time - train_start_time))

# Make predictions (in log scale)
predictions_log = lr_model.transform(test_data)

# Convert predictions back to original mass scale
predictions_temp = predictions_log.withColumn("predicted_mass", exp(col("prediction")) - 1)

# Clip negative predictions (just in case)
predictions_final = predictions_temp.withColumn(
    "clipped_prediction",
    when(col("predicted_mass") < 0, 0).otherwise(col("predicted_mass"))
)

# Show final results
predictions_final.select(
    "year", "reclat", "reclong", "mass", "clipped_prediction"
).show(truncate=False)

# Evaluate the model on the training set in log scale
training_summary = lr_model.summary
print("RMSE (log scale): %f" % training_summary.rootMeanSquaredError)
print("r2  (log scale): %f" % training_summary.r2)

# Stop the Spark session
spark.stop()
