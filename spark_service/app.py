from flask import Flask, request, send_file, jsonify
from flask_cors import CORS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, log as spark_log, exp, when
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler, StandardScaler
from cryptography.fernet import Fernet
import time
import os

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})  # Allow all origins for CORS
UPLOAD_FOLDER = '/shared/uploads'
RESULT_FOLDER = 'results'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULT_FOLDER, exist_ok=True)

# Generate a key for encryption
# This key should be securely shared with the client
key = Fernet.generate_key()
cipher_suite = Fernet(key)

# Create a Spark session with updated resource configuration
spark = SparkSession.builder \
    .master("spark://172.1.44.73:7077") \
    .appName("Spark as a Service") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "2g") \
    .config("spark.driver.cores", "1") \
    .getOrCreate()

@app.route('/')
def index():
    return "Spark as a Service: Backend is running."

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    if file:
        filepath = os.path.join(UPLOAD_FOLDER, file.filename)
        print(f"Saving file to {filepath}")  # Debugging statement
        file.save(filepath)
        if os.path.exists(filepath):
            print(f"File {filepath} successfully saved.")  # Debugging statement
        else:
            print(f"Failed to save file {filepath}.")  # Debugging statement
        result_path = process_file(filepath)
        print(f"Result file created at {result_path}")  # Debugging statement
        encrypted_path = encrypt_file(filepath)
        print(f"Encrypted file created at {encrypted_path}")  # Debugging statement
        return jsonify({"download_link": f"http://172.1.44.73:5000/download/result.csv"})

def process_file(filepath):
    print(f"Processing file {filepath}")  # Debugging statement
    load_start_time = time.time()
    df = spark.read.csv(filepath, header=True, inferSchema=True)
    load_end_time = time.time()
    print("Time to load dataset: %s seconds" % (load_end_time - load_start_time))

    filtered_df = (
        df.filter(col("year") > 1900)
          .dropna(subset=["year", "reclat", "reclong", "mass"])
    )

    df_log = filtered_df.withColumn("log_mass", spark_log(col("mass") + 1))

    df_features = df_log.select("year", "reclat", "reclong", "mass", "log_mass")

    assembler = VectorAssembler(
        inputCols=["year", "reclat", "reclong"],
        outputCol="features_unscaled"
    )
    data_unscaled = assembler.transform(df_features)

    scaler = StandardScaler(
        inputCol="features_unscaled",
        outputCol="features",
        withMean=True,
        withStd=True
    )
    scaler_model = scaler.fit(data_unscaled)
    data = scaler_model.transform(data_unscaled)

    train_data, test_data = data.randomSplit([0.8, 0.2])

    lr = LinearRegression(featuresCol="features", labelCol="log_mass")

    train_start_time = time.time()
    lr_model = lr.fit(train_data)
    train_end_time = time.time()
    print("Training time: %s seconds" % (train_end_time - train_start_time))

    predictions_log = lr_model.transform(test_data)

    predictions_temp = predictions_log.withColumn("predicted_mass", exp(col("prediction")) - 1)

    predictions_final = predictions_temp.withColumn(
        "clipped_prediction",
        when(col("predicted_mass") < 0, 0).otherwise(col("predicted_mass"))
    )

    result_path = os.path.join(RESULT_FOLDER, 'result.csv')
    predictions_final.select("year", "reclat", "reclong", "mass", "clipped_prediction") \
        .toPandas().to_csv(result_path, index=False)

    return result_path

def encrypt_file(filepath):
    with open(filepath, 'rb') as file:
        file_data = file.read()
    encrypted_data = cipher_suite.encrypt(file_data)
    encrypted_path = filepath + '.enc'
    with open(encrypted_path, 'wb') as file:
        file.write(encrypted_data)
    return encrypted_path

@app.route('/download/<filename>')
def download_file(filename):
    return send_file(os.path.join(RESULT_FOLDER, filename), as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
