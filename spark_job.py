from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, month, year, udf
from pyspark.sql.types import DateType
from pyspark.ml.feature import OneHotEncoder, StringIndexer

# Initialize a Spark session
spark = SparkSession.builder.appName("MySparkJob").getOrCreate()

# Load the CSV data
df = spark.read.csv("cvas_data.csv", header=True, inferSchema=True)

# Define a user-defined function (UDF) to convert date strings to DateType
func = udf(lambda x: datetime.strptime(x, '%d/%m/%Y'), DateType())

# Apply the UDF to create a 'date' column
df = df.withColumn('date', func(col('loan_date')))

# Cast the 'date' column to DateType
df = df.withColumn("date", df["date"].cast(DateType()))

# Extract relevant date components (day, month, year)
df = df.withColumn("day", dayofmonth("date"))
df = df.withColumn("month", month("date"))
df = df.withColumn("year", year("date"))

# Use StringIndexer to index columns
indexer_day = StringIndexer(inputCol="day", outputCol="day_index")
indexer_month = StringIndexer(inputCol="month", outputCol="month_index")
indexer_year = StringIndexer(inputCol="year", outputCol="year_index")
indexer_term = StringIndexer(inputCol="term", outputCol="term_index")

df = indexer_day.fit(df).transform(df)
df = indexer_month.fit(df).transform(df)
df = indexer_year.fit(df).transform(df)
df = indexer_term.fit(df).transform(df)

# Apply OneHotEncoder to the indexed columns
encoder = OneHotEncoder(inputCols=["day_index", "month_index", "year_index", "term_index"],
                        outputCols=["day_encoded", "month_encoded", "year_encoded", "term_encoded"])

model = encoder.fit(df)
df_encoded = model.transform(df)

# Show the resulting DataFrame
df_encoded.show()
output_path = "/tmp/output.parquet"
df_encoded = df_encoded.coalesce(1)  # Reduce to a single partition
df_encoded.write.parquet(output_path, mode="overwrite")
# Stop the Spark session
spark.stop()
