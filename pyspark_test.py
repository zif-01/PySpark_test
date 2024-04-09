from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def get_product_category_pairs(products_df, categories_df):
    
    product_category_df = products_df.join(categories_df, "product_id", "left_outer")

   
    pairs_df = product_category_df.select("product_name", "category_name").distinct()

    
    products_without_categories_df = products_df.join(
        categories_df, products_df.product_id == categories_df.product_id, "left_anti"
    ).select("product_name")

    return pairs_df, products_without_categories_df

# Пример использования
if __name__ == "__main__":
    
    spark = SparkSession.builder \
        .appName("ProductCategoryPairs") \
        .getOrCreate()

    # Примеры данных 
    products_data = [("product1", 1), ("product2", 2), ("product3", 3)]
    categories_data = [(1, "category1"), (2, "category2")]

    
    products_df = spark.createDataFrame(products_data, ["product_name", "product_id"])
    categories_df = spark.createDataFrame(categories_data, ["product_id", "category_name"])

   
    pairs_df, products_without_categories_df = get_product_category_pairs(products_df, categories_df)

    
    print("Product-Category Pairs:")
    pairs_df.show()

    print("Products Without Categories:")
    products_without_categories_df.show()

    
    spark.stop()
