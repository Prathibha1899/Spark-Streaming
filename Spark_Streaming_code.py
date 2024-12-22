from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import *

# Initializing SparkSession with 2 threads
streaming_app = SparkSession.builder \
    .appName("StreamingKMerCount") \
    .master("local[2]") \
    .getOrCreate()

# Defining a Function to Generate K-mers
# This function creates k-mers (substrings of length 3) from a given word
def extract_kmers(text, length=3):
    kmers = []
    if text and len(text) >= length:  # Handling nulls or short strings
        for i in range(len(text) - length + 1):
            kmers.append(text[i:i + length])
    return kmers

def generate_kmers_udf(text):
    return extract_kmers(text, length=3)

kmers_of_3 = udf(generate_kmers_udf, ArrayType(StringType()))

# Reading streaming data from a socket on localhost:9999
stream_data = streaming_app.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Splitting incoming lines into individual words
split_words = stream_data.select(explode(split(col("value"), "\\s+")).alias("individual_word"))

# Using the UDF to create k-mers from each word
kmer_results = split_words.select(explode(kmers_of_3(col("individual_word"))).alias("kmer_value"))

# Grouping k-mers and counting their occurrences
aggregated_counts = kmer_results.groupBy("kmer_value").count()

# Using transform to sort the results
sorted_results = aggregated_counts.transform(lambda df: df.orderBy(desc("count")))

# Outputs the sorted k-mer counts to the console every 10 seconds
output_query = (sorted_results.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .trigger(processingTime="10 seconds")
    .start())

# Keeping the Query Active
output_query.awaitTermination()
