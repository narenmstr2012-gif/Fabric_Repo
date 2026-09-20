# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "696f3f8e-9953-4324-89b7-740301c19fdf",
# META       "default_lakehouse_name": "ShoppingMart_GoldLayer",
# META       "default_lakehouse_workspace_id": "36aafa50-ca57-42cc-a28b-791224004b14",
# META       "known_lakehouses": [
# META         {
# META           "id": "696f3f8e-9953-4324-89b7-740301c19fdf"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Gold Layer Transformations & Aggregations: ShoppingMart Data


# CELL ********************

from pyspark.sql.functions import *

orders_df =spark.read.parquet("Files/ShoppingMart_Silver_Orders/ShoppingMart_customers_orderdata.csv")
reviews_df = spark.read.parquet("Files/ShoppingMart_Silver_Reviews/ShoppingMart_review")
social_df= spark.read.parquet("Files/ShoppingMart_Silver_Social_Media/ShoppingMart_social_media")
weblogs_df = spark.read.parquet("Files/ShoppingMart_Silver_Web_Logs/ShoppingMart_web_logs")

# display(weblogs_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Aggregates web log data to measure engangement per user on each page and action.
weblogs_df = spark.read.parquet("Files/ShoppingMart_Silver_Web_Logs/ShoppingMart_web_logs")
weblogs_df=weblogs_df.groupBy("user_id","page","action").count()
weblogs_df.write.mode("overwrite").parquet("Files/ShoppingMart_Gold_Web_Logs/ShoppingMart_web_logs")

# display(weblogs_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Aggregates unstructured social media data to crack sentiment trends across different platforms.
social_df= spark.read.parquet("Files/ShoppingMart_Silver_Social_Media/ShoppingMart_social_media")
social_df=social_df.groupBy("platform","sentiment").count()
social_df.write.mode("overwrite").parquet("Files/ShoppingMart_Gold_Social_Media/ShoppingMart_social_media")

# display(social_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Aggregates product reviews to calculate the average rating per product.
reviews_df = spark.read.parquet("Files/ShoppingMart_Silver_Reviews/ShoppingMart_review")
reviews_df = reviews_df.groupBy("product_id").agg(avg("rating").alias("AvgRating"))
reviews_df.write.mode("overwrite").parquet("Files/ShoppingMart_Gold_Reviews/ShoppingMart_review")

# display(reviews_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orders_df.write.mode("overwrite").parquet("Files/ShoppingMart_Gold_Orders/ShoppingMart_customers_orderdata.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
