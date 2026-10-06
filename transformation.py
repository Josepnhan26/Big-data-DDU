from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    hour,
    day,
    month,
    when
)
import os
from pyspark.sql import SparkSession
os.environ['TEMP'] = 'C:\\BigdataProject'
os.environ['TMP'] = 'C:\\BigdataProject'
# Trỏ chính xác đến thư mục vừa giải nén
os.environ['JAVA_HOME'] = 'C:\\Java\\jdk-17.0.20.1+1'


# 1. Khởi tạo Spark
spark = SparkSession.builder \
    .appName("Data_Transformation") \
    .getOrCreate()

# 2. Đọc dữ liệu đã làm sạch
df_clean = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("C:/BigdataProject/Bigdata/Bigdata/df_clean_final.csv")

# 3. Transformation
# 3.1 Chuyển Order_Time thành kiểu thời gian
df_transform = df_clean.withColumn(
    "Order_Timestamp",
    to_timestamp(
        col("Order_Time"),
        "dd-MM-yyyy HH:mm"
    )
)
# 3.2 Tách ngày
df_transform = df_transform.withColumn(
    "Order_Date",
    to_date(col("Order_Timestamp"))
)
# 3.3 Tách giờ
df_transform = df_transform.withColumn(
    "Order_Hour",
    hour(col("Order_Timestamp"))
)
# 3.4 Tách ngày trong tháng
df_transform = df_transform.withColumn(
    "Order_Day",
    day(col("Order_Timestamp"))
)
# 3.5 Tách tháng
df_transform = df_transform.withColumn(
    "Order_Month",
    month(col("Order_Timestamp"))
)
# 4. Biến đổi thời gian giao hàng

df_transform = df_transform.withColumn(
    "Delivery_Duration_Hours",
    col("Delivery_Duration_Minutes") / 60
)
# 5. Phân loại khoảng cách
df_transform = df_transform.withColumn(
    "Distance_Category",
    when(col("Delivery_Distance_km") < 3, "Near")
    .when(col("Delivery_Distance_km") <= 5, "Medium")
    .otherwise("Far")
)
# 6. Phân loại thời gian giao hàng
df_transform = df_transform.withColumn(
    "Delivery_Category",
    when(col("Delivery_Duration_Minutes") <= 30, "Fast")
    .when(col("Delivery_Duration_Minutes") <= 45, "Normal")
    .otherwise("Slow")
)
# 7. Kiểm tra kết quả
print("===== SCHEMA SAU TRANSFORMATION =====")
df_transform.printSchema()
print("===== KẾT QUẢ TRANSFORMATION =====")
df_transform.select(
    "Order_ID",
    "Order_Time",
    "Order_Date",
    "Order_Hour",
    "Order_Month",
    "Delivery_Duration_Minutes",
    "Delivery_Duration_Hours",
    "Delivery_Distance_km",
    "Distance_Category",
    "Delivery_Category"
).show(10, truncate=False)
# 8. Đóng Spark
spark.stop()