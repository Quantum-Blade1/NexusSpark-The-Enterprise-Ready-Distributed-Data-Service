import os, time
from pyspark.sql.functions import col, log as spark_log, exp, when
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler, StandardScaler
from cryptography.fernet import Fernet
from.engine import get_spark

# Standardizing folders
RESULT_FOLDER = 'results'
key = Fernet.generate_key()
cipher_suite = Fernet(key)

def run_meteorite_analysis(filepath: str):
    spark = get_spark()
    print(f"Processing: {filepath}")
    
    # 1. Load & Clean
    df = spark.read.csv(filepath, header=True, inferSchema=True)
    filtered_df = df.filter(col("year") > 1900).dropna(subset=["year", "reclat", "reclong", "mass"])
    df_log = filtered_df.withColumn("log_mass", spark_log(col("mass") + 1))

    # 2. Feature Engineering
    assembler = VectorAssembler(inputCols=["year", "reclat", "reclong"], outputCol="features_unscaled")
    data_unscaled = assembler.transform(df_log.select("year", "reclat", "reclong", "mass", "log_mass"))
    
    scaler = StandardScaler(inputCol="features_unscaled", outputCol="features", withMean=True, withStd=True)
    data = scaler.fit(data_unscaled).transform(data_unscaled)

    # 3. ML Training
    train_data, test_data = data.randomSplit([0.8, 0.2])
    lr = LinearRegression(featuresCol="features", labelCol="log_mass")
    lr_model = lr.fit(train_data)

    # 4. Predict & Save
    predictions = lr_model.transform(test_data)
    final = predictions.withColumn("predicted_mass", exp(col("prediction")) - 1)
    
    result_path = os.path.join(RESULT_FOLDER, 'analysis_result.csv')
    final.toPandas().to_csv(result_path, index=False)
    
    # 5. Encrypt for Security
    encrypt_result(result_path)
    return result_path

def encrypt_result(path):
    with open(path, 'rb') as f:
        data = f.read()
    with open(path + '.enc', 'wb') as f:
        f.write(cipher_suite.encrypt(data))