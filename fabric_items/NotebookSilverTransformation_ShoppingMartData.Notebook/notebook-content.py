# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ba3db939-805a-4e41-9a42-0a865191ea7a",
# META       "default_lakehouse_name": "ShoppingMart_SilverLayer",
# META       "default_lakehouse_workspace_id": "36aafa50-ca57-42cc-a28b-791224004b14",
# META       "known_lakehouses": [
# META         {
# META           "id": "889b5dde-f1a6-48ba-96c6-ac88ae8cc1c6"
# META         },
# META         {
# META           "id": "ba3db939-805a-4e41-9a42-0a865191ea7a"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Silver Layer Notebook : Data Cleaning & Integration


# MARKDOWN ********************

# ## Load Bronze Data

# CELL ********************

from pyspark.sql.functions import*

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_customers = spark.read.format("csv").option("header","true").load("Files/ShoppingMart_Bronze_Customers/ShoppingMart_customers.csv")
df_orders= spark.read.format("csv").option("header","true").load("Files/ShoppingMart_Bronze_Orders/ShoppingMart_orders.csv")
df_products= spark.read.format("csv").option("header","true").load("Files/ShoppingMart_Bronze_Products/ShoppingMart_products.csv")
df_reviews=spark.read.json("Files/ShoppingMart_Bronze_Reviews/ShoppingMart_review.json")
df_social=spark.read.json("Files/ShoppingMart_Bronze_Social_Media/ShoppingMart_social_media.json")
df_weblogs=spark.read.json("Files/ShoppingMart_Bronze_Web_Logs/ShoppingMart_web_logs.json")

display(df_weblogs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Data cleaning & enriching

# CELL ********************

df_orders=df_orders.dropna(subset=["OrderID","CustomerID","ProductID","Orderdate","TotalAmount"])
df_orders=df_orders.withColumn("OrderDate",to_date(col("orderDate")))
display(df_orders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Join with Products and Customers

# CELL ********************

df_orders=df_orders \
    .join(df_customers,on= "CustomerID",how="inner")\
    .join(df_products,on= "ProductID",how="inner")
display(df_orders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Writing to Silver Layer

# CELL ********************

df_orders.write.mode("overwrite").parquet("Files/ShoppingMart_Silver_Orders/ShoppingMart_customers_orderdata.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_reviews.write.mode("overwrite").parquet("Files/ShoppingMart_Silver_Reviews/ShoppingMart_review")
df_social.write.mode("overwrite").parquet("Files/ShoppingMart_Silver_Social_Media/ShoppingMart_social_media")
df_weblogs.write.mode("overwrite").parquet("Files/ShoppingMart_Silver_Web_Logs/ShoppingMart_web_logs")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
