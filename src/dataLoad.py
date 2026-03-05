catalog_name = "databricks-poc-updated"
schema_name = "nike_poc"

target_tables = [
    "sem_poc_fact_sales_transactions",
    "sem_poc_fact_inventory_snapshot",
    "sem_poc_dim_customer",
    "sem_poc_dim_promotion",
    "sem_poc_dim_product",
    "sem_poc_dim_store",
    "sem_poc_dim_date",
    "sem_poc_dim_channel",
    "sem_poc_dim_employee",
    "sem_poc_dim_geography",
    "sem_poc_fact_returns",
    "sem_poc_fact_web_sessions"
]

base_path = "/Volumes/databricks-poc-updated/nike_poc/data_volume/nike_sample_data_csv/"

for table in target_tables:
    
    full_table_name = f"`{catalog_name}`.{schema_name}.{table}"
    file_path = f"{base_path}/{table}.csv"
    
    print(f"Loading {file_path} into {full_table_name}")
    
    # Get existing table schema
    target_schema = spark.table(full_table_name).schema
    
    # Read CSV using target schema
    df = (
        spark.read
             .format("csv")
             .option("header", "true")
             .schema(target_schema)
             .load(file_path)
    )
    
    # Insert into existing table
    df.write \
      .mode("overwrite") \
      .insertInto(full_table_name)
    

queries = []

for table in target_tables:
    queries.append(
        f"SELECT '{table}' AS table_name, COUNT(*) AS cnt "
        f"FROM `{catalog_name}`.`{schema_name}`.`{table}`"
    )

final_query = " UNION ALL ".join(queries)

df = spark.sql(final_query)
df.show()    