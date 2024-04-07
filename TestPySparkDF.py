from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("ProductCategoryPairs").getOrCreate()

products_data = [("ProductA", 1), ("ProductB", 2), ("ProductC", None)]
categories_data = [(1, "CategoryX"), (2, "CategoryY")]

products_df = spark.createDataFrame(products_data, ["Product", "CategoryID"])
categories_df = spark.createDataFrame(categories_data, ["CategoryID", "Category"])

print("Products data:")
products_df.show()

print("Categories data:")
categories_df.show()

def get_product_category_pairs(products_df, categories_df):

    joined_df = products_df.join(categories_df, "CategoryID", "left")

    products_without_categories = joined_df.filter(col("Category").isNull())

    product_category_pairs = joined_df.select("Product", "Category")
    products_without_categories_names = products_without_categories.select("Product")

    return product_category_pairs, products_without_categories_names

product_category_pairs, products_without_categories = get_product_category_pairs(products_df, categories_df)

print("Product-Category pairs:")
product_category_pairs.show()

print("Products without categories:")
products_without_categories.show()