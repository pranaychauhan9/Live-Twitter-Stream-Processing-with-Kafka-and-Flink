
import emojis
from textblob import TextBlob

from pyspark.sql.functions import col, split, expr, udf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, IntegerType, LongType, \
    ArrayType

# Create a Spark session
spark = (SparkSession.builder
         .appName("KafkaToPySpark")
         .getOrCreate())


# Define the schema based on your headers
schema = StructType([
    StructField("status_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("screen_name", StringType(), True),
    StructField("text", StringType(), True),
    StructField("source", StringType(), True),
    StructField("reply_to_status_id", StringType(), True),
    StructField("reply_to_user_id", StringType(), True),
    StructField("reply_to_screen_name", StringType(), True),
    StructField("is_quote", BooleanType(), True),
    StructField("is_retweet", BooleanType(), True),
    StructField("favourites_count", IntegerType(), True),
    StructField("retweet_count", IntegerType(), True),
    StructField("country_code", StringType(), True),
    StructField("place_full_name", StringType(), True),
    StructField("place_type", StringType(), True),
    StructField("followers_count", IntegerType(), True),
    StructField("friends_count", IntegerType(), True),
    StructField("account_lang", StringType(), True),
    StructField("account_created_at", TimestampType(), True),
    StructField("verified", BooleanType(), True),
    StructField("lang", StringType(), True)
])

# Define the Kafka parameters
kafka_params = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "demo",
    "startingOffsets": "earliest",
    "failOnDataLoss": "false"
}

# Read data from Kafka topic
raw_stream_data = spark \
    .readStream \
    .format("kafka") \
    .options(**kafka_params) \
    .load()

# Convert the binary value from Kafka into a string
stream_data = raw_stream_data.selectExpr("CAST(value AS STRING)")

# parsed_data = raw_stream_data \
#     .select(split("value", ",").alias("data")) \
#     .select(*[col("data")[i].alias(field.name) for i, field in enumerate(schema.fields)])

stream_data.drop('status_id','user_id','screen_name','source','reply_to_status_id',
                                        'reply_to_user_id','is_retweet','place_full_name','place_type',
                                        'reply_to_screen_name','is_quote','followers_count','friends_count',
                                        'account_lang','account_created_at','verified')


#Data Cleaning

import re
from nltk.corpus import stopwords
stop_words = set(stopwords.words('english'))
import string
punct = string.punctuation

def deEmojify(text):
    if text is None:
        return ''
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F700-\U0001F77F"  # alchemical symbols
                               u"\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
                               u"\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
                               u"\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
                               u"\U0001FA00-\U0001FA6F"  # Chess Symbols
                               u"\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
                               u"\U00002702-\U000027B0"  # Dingbats
                               u"\U000024C2-\U0001F251"
                               "]+", flags=re.UNICODE)

    if isinstance(text, str):
        clean_text = emoji_pattern.sub(r'', text)
        return clean_text
    else:
        return ''
def removeURL(text):
    if text is not None and isinstance(text, str):
        clean_text = re.sub(r"http\S+", "", text)
        return clean_text
    else:
        # Handle the case when text is None or not a string
        return text
def analyze_sentiment(text):
    if text:
        blob = TextBlob(text)
        polarity = blob.sentiment.polarity
        if polarity > 0:
            sentiment = 'positive'
        elif polarity < 0:
            sentiment = 'negative'
        else:
            sentiment = 'neutral'
        return sentiment
    else:
        return None


removeURL_udf = udf(removeURL, StringType())
deEmojify_udf = udf(deEmojify, StringType())
tokenize_tweets_udf = udf(lambda text: analyze_sentiment(text, stop_words, punct), ArrayType(StringType()))
analyze_sentiment_udf = udf(analyze_sentiment, StringType())

# Apply UDF to the DataFrame
processed_data = stream_data \
    .select(split("value", ",").alias("data")) \
    .select(*[col("data")[i].alias(field.name) for i, field in enumerate(schema.fields)]) \
    .withColumn("text_cleaned", removeURL_udf(deEmojify_udf(col("text")))) \
    .withColumn("sentiment", analyze_sentiment_udf(col("text_cleaned")))



# Output the streaming data to the console
query = processed_data \
    .select("screen_name","sentiment") \
    .filter((col("sentiment") == "neutral")) \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for the stream to finish
query.awaitTermination(30)
query.stop()
