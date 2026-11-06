# Databricks notebook source
from datetime import datetime
import uuid

run_id = str(uuid.uuid4())
run_start_time = datetime.now()

# COMMAND ----------

dbutils.widgets.text("run_date", "")

RUN_DATE = dbutils.widgets.get("run_date")

spark.sql("USE CATALOG atliq")
spark.sql("USE SCHEMA silver")

print("Pipeline run date:", RUN_DATE)

# COMMAND ----------

BRONZE = "abfss://lakehouse@atliqcommerce20261104.dfs.core.windows.net/bronze"

customers = spark.read.parquet(f"{BRONZE}/customers")

display(customers.limit(10))

# COMMAND ----------


from pyspark.sql import functions as F

# Step 1: Clean and transform customer data
customers_silver = (
    customers
    .withColumn(
        "city",
        F.initcap(F.trim(F.col("city")))
    )
    .withColumn(
        "city",
        F.when(
            F.lower(F.col("city")) == "bangalore",
            "Bengaluru"
        ).otherwise(F.col("city"))
    )
    .withColumn(
        "email",
        F.lower(F.trim(F.col("email")))
    )
    .withColumn(
        "signup_date",
        F.to_date("signup_date")
    )
    .withColumn(
        "is_test",
        F.col("email").rlike(r"(?i)@atliq-test\.com$")
    )
    .dropDuplicates(["customer_id"])
    .filter(F.col("customer_id").isNotNull())
)

# Step 2: Write to Silver Delta table
# overwriteSchema handles the addition of the is_test column
customers_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("atliq.silver.customers")

# Step 3: Read back the Silver table
silver_customers = spark.table("atliq.silver.customers")

# Step 4: Validate
print("Total rows:", silver_customers.count())
print(
    "Distinct customer IDs:",
    silver_customers.select("customer_id").distinct().count()
)
print(
    "Duplicate customer IDs:",
    silver_customers.count() -
    silver_customers.select("customer_id").distinct().count()
)

print("Customer schema:")
silver_customers.printSchema()

# Step 5: Check test and genuine customers
display(
    silver_customers.groupBy("is_test").count()
)


# COMMAND ----------

silver_customers = spark.table("atliq.silver.customers")

print("Total rows:", silver_customers.count())
print("Distinct customer IDs:",
      silver_customers.select("customer_id").distinct().count())

display(
    silver_customers.groupBy("is_test").count()
)

# COMMAND ----------

display(
    silver_customers
    .filter(F.col("is_test") == True)
    .select("customer_id", "customer_name", "email")
)

# COMMAND ----------

from pyspark.sql import functions as F

customers = (
    spark.read.parquet(f"{BRONZE}/customers")
    .withColumn("city", F.initcap(F.trim("city")))
    .withColumn("signup_date", F.to_date("signup_date"))
    .dropDuplicates(["customer_id"])
    .filter(F.col("customer_id").isNotNull())
)

customers.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("atliq.silver.customers")

# COMMAND ----------

silver_customers = spark.table("atliq.silver.customers")

print("Silver Customers:", silver_customers.count())
print("Distinct IDs:", silver_customers.select("customer_id").distinct().count())

display(silver_customers.limit(10))

# COMMAND ----------

from pyspark.sql.functions import col

silver_customers = (
    spark.table("atliq.silver.customers")
    .select(
        col("customer_id").cast("int"),
        col("customer_name").cast("string"),
        col("email").cast("string"),
        col("city").cast("string"),
        col("signup_date").cast("date"),
        col("updated_at").cast("timestamp")
    )
)

silver_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("atliq.silver.customers")

# COMMAND ----------

silver_customers = spark.table("atliq.silver.customers")

print("Total rows:", silver_customers.count())
print("Distinct customer IDs:",
      silver_customers.select("customer_id").distinct().count())

print("Duplicate customer IDs:",
      silver_customers.count() -
      silver_customers.select("customer_id").distinct().count())

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read Bronze products
products = spark.read.parquet(f"{BRONZE}/products")

# Clean and standardize
products_clean = (
    products
    .withColumn(
        "product_name",
        F.regexp_replace(F.trim(F.col("product_name")), r"\s+", " ")
    )
    .withColumn(
        "category",
        F.initcap(F.trim(F.col("category")))
    )
    .withColumn(
        "category",
        F.when(
            F.lower(F.col("category")).isin("home and kitchen", "home & kitchen"),
            "Home & Kitchen"
        ).otherwise(F.col("category"))
    )
    .withColumn("product_id", F.col("product_id").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)"))
    .withColumn("updated_at", F.col("updated_at").cast("timestamp"))
    .filter(F.col("product_id").isNotNull())
)

# Deduplicate: retain latest record per product_id
window_spec = Window.partitionBy("product_id").orderBy(
    F.col("updated_at").desc_nulls_last(),
    F.col("product_name").asc_nulls_last()
)

silver_products = (
    products_clean
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# Write Silver Delta table
silver_products.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("atliq.silver.products")

# COMMAND ----------

silver_products = spark.table("atliq.silver.products")

print("Total rows:", silver_products.count())

print(
    "Distinct product IDs:",
    silver_products.select("product_id").distinct().count()
)

print(
    "Duplicate product IDs:",
    silver_products.count()
    - silver_products.select("product_id").distinct().count()
)

print("Null product IDs:",
      silver_products.filter(F.col("product_id").isNull()).count())

display(silver_products.groupBy("category").count())

# COMMAND ----------

orders = spark.read.parquet(f"{BRONZE}/orders")

orders.printSchema()
display(orders.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Keep latest version of each order
w = Window.partitionBy("order_id").orderBy(
    F.col("updated_at").desc_nulls_last()
)

orders_clean = (
    orders
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("order_amount", F.col("order_amount").cast("decimal(12,2)"))
    .filter(F.col("order_id").isNotNull())
)

# Create target table if it does not exist
(
    DeltaTable.createIfNotExists(spark)
    .tableName("atliq.silver.orders")
    .addColumns(orders_clean.schema)
    .execute()
)

# Upsert into Silver
(
    DeltaTable.forName(spark, "atliq.silver.orders")
    .alias("t")
    .merge(orders_clean.alias("s"), "t.order_id = s.order_id")
    .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------


from pyspark.sql import functions as F
silver_orders = spark.table("atliq.silver.orders")

print("Total rows:", silver_orders.count())

print(
    "Distinct order IDs:",
    silver_orders.select("order_id").distinct().count()
)

print(
    "Duplicate order IDs:",
    silver_orders.count()
    - silver_orders.select("order_id").distinct().count()
)

print(
    "Null order IDs:",
    silver_orders.filter(F.col("order_id").isNull()).count()
)

display(silver_orders.groupBy("status").count())

# COMMAND ----------

from pyspark.sql import functions as F

orders_clean = orders.withColumn(
    "status",
    F.initcap(F.trim(F.col("status")))
)

display(orders_clean.groupBy("status").count())

# COMMAND ----------

print("CURRENT DATAFRAME SCHEMA:")
customers_silver.printSchema()

print("EXISTING DELTA TABLE SCHEMA:")
spark.table("atliq.silver.customers").printSchema()

# COMMAND ----------

order_items = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet("abfss://lakehouse@atliqcommerce20261104.dfs.core.windows.net/bronze/order_items/")
)

print("Bronze Order Items:", order_items.count())
order_items.printSchema()
display(order_items.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

print("Total rows:", order_items.count())

print(
    "Distinct Order Item IDs:",
    order_items.select("order_item_id").distinct().count()
)

print(
    "Duplicate Order Item IDs:",
    order_items.count()
    - order_items.select("order_item_id").distinct().count()
)

print(
    "Null Order Item IDs:",
    order_items.filter(F.col("order_item_id").isNull()).count()
)

print(
    "Null Order IDs:",
    order_items.filter(F.col("order_id").isNull()).count()
)

print(
    "Null Product IDs:",
    order_items.filter(F.col("product_id").isNull()).count()
)

print(
    "Invalid quantities:",
    order_items.filter(
        F.col("quantity").isNull() | (F.col("quantity") <= 0)
    ).count()
)

print(
    "Invalid item prices:",
    order_items.filter(
        F.col("item_price").isNull() | (F.col("item_price") < 0)
    ).count()
)

# COMMAND ----------

from pyspark.sql import functions as F

silver_orders = spark.table("atliq.silver.orders")
silver_products = spark.table("atliq.silver.products")

unmatched_orders = order_items.join(
    silver_orders.select("order_id"),
    on="order_id",
    how="left_anti"
)

unmatched_products = order_items.join(
    silver_products.select("product_id"),
    on="product_id",
    how="left_anti"
)

print("Unmatched Order IDs:", unmatched_orders.count())
print("Unmatched Product IDs:", unmatched_products.count())

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

# Prepare source data
order_items_src = (
    order_items
    .select(
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "item_price",
        "created_at"
    )
    .filter(F.col("order_item_id").isNotNull())
)

# Create Silver table if it does not exist
(
    DeltaTable.createIfNotExists(spark)
    .tableName("atliq.silver.order_items")
    .addColumns(order_items_src.schema)
    .execute()
)

# Insert only new order items
(
    DeltaTable.forName(spark, "atliq.silver.order_items")
    .alias("t")
    .merge(
        order_items_src.alias("s"),
        "t.order_item_id = s.order_item_id"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

# Read Silver table
silver_order_items = spark.table("atliq.silver.order_items")

# Validation
print("Silver Order Items:", silver_order_items.count())
print(
    "Distinct Order Item IDs:",
    silver_order_items.select("order_item_id").distinct().count()
)

display(silver_order_items.limit(10))

# COMMAND ----------

payments = spark.read.parquet(f"{BRONZE}/payments")

print("Bronze Payments:", payments.count())
payments.printSchema()

display(payments.limit(10))

# COMMAND ----------

display(
    payments
    .groupBy("method")
    .count()
    .orderBy("method")
)

# COMMAND ----------

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

# 1. Standardise payment methods
PAYMENT_MAP = {
    "upi": "UPI",
    "credit card": "Credit Card",
    "creditcard": "Credit Card",
    "debit card": "Debit Card",
    "debitcard": "Debit Card",
    "net banking": "Net Banking",
    "netbanking": "Net Banking",
    "wallet": "Wallet",
    "cod": "COD",
    "cash on delivery": "COD"
}

payment_map = F.create_map(
    *[F.lit(x) for pair in PAYMENT_MAP.items() for x in pair]
)

# 2. Clean the payment data
payments_clean = (
    payments
    .withColumn(
        "method_key",
        F.lower(F.regexp_replace(F.trim(F.col("method")), r"\s+", " "))
    )
    .withColumn("method", payment_map[F.col("method_key")])
    .drop("method_key")
    .withColumn("amount", F.col("amount").cast("decimal(12,2)"))
    .withColumn("paid_at", F.col("paid_at").cast("timestamp"))
    .withColumn("updated_at", F.col("updated_at").cast("timestamp"))
    .filter(F.col("payment_id").isNotNull())
)

# 3. Keep the latest version of each payment_id in this batch
window = Window.partitionBy("payment_id").orderBy(
    F.col("updated_at").desc_nulls_last()
)

payments_clean = (
    payments_clean
    .withColumn("rn", F.row_number().over(window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# 4. Create Silver table if it does not exist
(
    DeltaTable.createIfNotExists(spark)
    .tableName("atliq.silver.payments")
    .addColumns(payments_clean.schema)
    .execute()
)

# 5. Merge into Silver
(
    DeltaTable.forName(spark, "atliq.silver.payments")
    .alias("t")
    .merge(
        payments_clean.alias("s"),
        "t.payment_id = s.payment_id"
    )
    .whenMatchedUpdateAll(
        condition="s.updated_at > t.updated_at"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print("Silver Payments loaded successfully.")

# COMMAND ----------

silver_payments = spark.table("atliq.silver.payments")

print("Total rows:", silver_payments.count())
print(
    "Distinct Payment IDs:",
    silver_payments.select("payment_id").distinct().count()
)
print(
    "Duplicate Payment IDs:",
    silver_payments.count() -
    silver_payments.select("payment_id").distinct().count()
)
print(
    "Null Payment IDs:",
    silver_payments.filter(F.col("payment_id").isNull()).count()
)

display(
    silver_payments.groupBy("method")
    .count()
    .orderBy("method")
)

# COMMAND ----------

silver_orders = spark.table("atliq.silver.orders")

unmatched_payments = (
    silver_payments
    .join(
        silver_orders.select("order_id").distinct(),
        on="order_id",
        how="left_anti"
    )
)

print("Unmatched Order IDs:", unmatched_payments.count())
display(unmatched_payments.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

display(
    silver_payments.select(
        F.count("*").alias("total_rows"),
        F.sum(F.when(F.col("amount").isNull(), 1).otherwise(0)).alias("null_amounts"),
        F.sum(F.when(F.col("amount") <= 0, 1).otherwise(0)).alias("non_positive_amounts"),
        F.sum(F.when(F.col("amount") < 0, 1).otherwise(0)).alias("negative_amounts")
    )
)

# COMMAND ----------

display(
    silver_payments
    .groupBy("payment_id")
    .count()
    .filter(F.col("count") > 1)
)

# COMMAND ----------

spark.sql("SHOW TABLES IN atliq.silver").show(truncate=False)

# COMMAND ----------

CLICKSTREAM_PATH = "abfss://lakehouse@atliqcommerce20261104.dfs.core.windows.net/bronze/clickstream/"

files = dbutils.fs.ls(CLICKSTREAM_PATH)

print("Clickstream files:", len(files))
display(files[:10])

# COMMAND ----------

from pyspark.sql import functions as F

clickstream_raw = spark.read.text(CLICKSTREAM_PATH)

print("Raw clickstream lines:", clickstream_raw.count())

display(clickstream_raw.limit(5))

# COMMAND ----------

from pyspark.sql import functions as F

# Get one sample JSON record
sample_json = (
    clickstream_raw
    .filter(F.col("value").isNotNull())
    .first()["value"]
)

# Infer schema from the sample
json_schema = (
    spark.range(1)
    .select(F.schema_of_json(F.lit(sample_json)).alias("schema"))
    .first()["schema"]
)

print("Inferred JSON schema:")
print(json_schema)

# Parse the JSON records
clickstream_parsed = clickstream_raw.withColumn(
    "event",
    F.from_json(F.col("value"), json_schema)
)

display(clickstream_parsed.select("event.*").limit(5))

# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Extract JSON fields and retain event_ts as string
clickstream_clean = clickstream_parsed.select(
    F.col("event.event_id").alias("event_id"),
    F.col("event.session_id").alias("session_id"),
    F.col("event.customer_id").cast("long").alias("customer_id"),
    F.col("event.event_type").alias("event_type"),
    F.col("event.event_ts").cast("string").alias("event_ts_raw"),
    F.col("event.product_id").cast("long").alias("product_id"),
    F.col("event.order_id").cast("long").alias("order_id"),
    F.col("event.device").alias("device"),
    F.col("event.user_agent").alias("user_agent"),
    F.col("event.utm_source").alias("utm_source")
)

# Parse both epoch milliseconds and ISO timestamps
clickstream_clean = clickstream_clean.withColumn(
    "event_ts",
    F.when(
        F.col("event_ts_raw").rlike(r"^\d+$"),
        F.timestamp_millis(F.col("event_ts_raw").cast("long"))
    ).otherwise(
        F.try_to_timestamp(
            F.col("event_ts_raw"),
            F.lit("yyyy-MM-dd'T'HH:mm:ss.SSSX")
        )
    )
).drop("event_ts_raw")

# Normalize event type
clickstream_clean = clickstream_clean.withColumn(
    "event_type",
    F.lower(F.trim(F.col("event_type")))
)

# Retain valid events with a session
clickstream_clean = clickstream_clean.filter(
    F.col("event_id").isNotNull() &
    F.col("session_id").isNotNull() &
    (F.trim(F.col("session_id")) != "") &
    F.col("event_ts").isNotNull()
)

# Deduplicate by event_id
window_spec = Window.partitionBy("event_id").orderBy(
    F.col("event_ts").desc()
)

clickstream_clean = (
    clickstream_clean
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

print("Clean clickstream records:", clickstream_clean.count())
print(
    "Distinct event IDs:",
    clickstream_clean.select("event_id").distinct().count()
)

clickstream_clean.printSchema()


# COMMAND ----------

# Identify bot sessions
bot_sessions = (
    clickstream_clean
    .filter(
        F.col("user_agent").rlike(
            r"(?i)(bot|crawler|spider|python-requests)"
        )
    )
    .select("session_id")
    .distinct()
)

print("Bot sessions:", bot_sessions.count())

# Remove entire bot sessions
events_no_bots = clickstream_clean.join(
    bot_sessions,
    on="session_id",
    how="left_anti"
)

print("Events after bot removal:", events_no_bots.count())

# Remove events referring to unknown products
known_products = spark.table("atliq.silver.products").select("product_id")

events_final = (
    events_no_bots
    .join(
        known_products.withColumnRenamed("product_id", "known_product_id"),
        events_no_bots.product_id == F.col("known_product_id"),
        "left"
    )
    .filter(
        F.col("product_id").isNull() |
        F.col("known_product_id").isNotNull()
    )
    .drop("known_product_id")
)

print("Events after product validation:", events_final.count())

# COMMAND ----------

events_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("atliq.silver.web_events")

print("Silver Web Events created successfully.")

silver_web_events = spark.table("atliq.silver.web_events")

print("Total Silver Events:", silver_web_events.count())
print(
    "Distinct Event IDs:",
    silver_web_events.select("event_id").distinct().count()
)

silver_web_events.printSchema()

# COMMAND ----------

supplier_bronze = spark.read.parquet(
    f"{BRONZE}/supplier_price_list"
)

print("Supplier rows:", supplier_bronze.count())
supplier_bronze.printSchema()
display(supplier_bronze.limit(5))

# COMMAND ----------

from pyspark.sql import functions as F

# Inspect supplier cost formats
display(
    supplier_bronze
    .select("supplier_cost")
    .distinct()
    .orderBy("supplier_cost")
)

# Inspect effective date formats
display(
    supplier_bronze
    .select("effective_date")
    .distinct()
    .orderBy("effective_date")
)

# Check exact duplicate rows
print("Total rows:", supplier_bronze.count())
print("Distinct rows:", supplier_bronze.distinct().count())

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Remove exact duplicate rows
supplier_clean = supplier_bronze.dropDuplicates()

# 2. Clean and cast columns
supplier_clean = (
    supplier_clean
    .withColumn(
        "product_id",
        F.col("product_id").cast("int")
    )
    .withColumn(
        "supplier_cost",
        F.regexp_extract(
            F.regexp_replace(F.col("supplier_cost"), ",", ""),
            r"(\d+(?:\.\d+)?)",
            1
        ).cast("decimal(12,2)")
    )
    .withColumn(
        "effective_date",
        F.coalesce(
            F.try_to_date("effective_date", "yyyy-MM-dd"),
            F.try_to_date("effective_date", "dd/MM/yyyy"),
            F.try_to_date("effective_date", "dd-MMM-yyyy")
        )
    )
)

# 3. Remove invalid records
supplier_clean = supplier_clean.filter(
    F.col("product_id").isNotNull() &
    F.col("supplier_cost").isNotNull() &
    F.col("effective_date").isNotNull()
)

# 4. Keep only products existing in Silver Products
known_products = (
    spark.table("atliq.silver.products")
    .select("product_id")
    .distinct()
)

supplier_clean = supplier_clean.join(
    known_products,
    on="product_id",
    how="inner"
)

# 5. Keep latest effective date per product
window_spec = Window.partitionBy("product_id").orderBy(
    F.col("effective_date").desc()
)

supplier_silver = (
    supplier_clean
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# 6. Save Silver table
(
    supplier_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("atliq.silver.supplier_price_list")
)

print("Silver supplier price list created successfully")
print("Silver rows:", supplier_silver.count())
print(
    "Distinct products:",
    supplier_silver.select("product_id").distinct().count()
)

display(supplier_silver.orderBy("product_id"))

# COMMAND ----------

# Check the original Bronze data
supplier_bronze.printSchema()

display(
    supplier_bronze
    .select("supplier_cost")
    .distinct()
    .orderBy("supplier_cost")
)

display(
    supplier_bronze
    .filter(F.col("supplier_cost").contains("718.47"))
)

# COMMAND ----------

from datetime import datetime

run_date = datetime.now().strftime("%Y-%m-%d")

spark.createDataFrame(
    [(run_date, "supplier_price_list", "exact duplicate rows removed", 122)],
    "run_date STRING, table_name STRING, rule STRING, rows_affected LONG"
).write.mode("append").saveAsTable("atliq.silver.dq_log")

print("Supplier duplicate-removal logged.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'dim_customer' AS table_name, COUNT(*) AS row_count
# MAGIC FROM atliq.gold.dim_customer
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dim_product', COUNT(*)
# MAGIC FROM atliq.gold.dim_product
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dim_date', COUNT(*)
# MAGIC FROM atliq.gold.dim_date
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'fact_sales', COUNT(*)
# MAGIC FROM atliq.gold.fact_sales
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'fact_web_sessions', COUNT(*)
# MAGIC FROM atliq.gold.fact_web_sessions;

# COMMAND ----------

from pyspark.sql import Row

audit_record = [Row(
    run_id=run_id,
    pipeline_name="JOB_AtliQ_Silver_Transformation",
    run_start_time=run_start_time,
    run_end_time=datetime.now(),
    status="SUCCESS",
    customers_rows=spark.table("atliq.silver.customers").count(),
    products_rows=spark.table("atliq.silver.products").count(),
    orders_rows=spark.table("atliq.silver.orders").count(),
    order_items_rows=spark.table("atliq.silver.order_items").count(),
    payments_rows=spark.table("atliq.silver.payments").count(),
    web_events_rows=spark.table("atliq.silver.web_events").count()
)]

spark.createDataFrame(audit_record).write.mode("append").saveAsTable(
    "atliq.silver.pipeline_audit"
)