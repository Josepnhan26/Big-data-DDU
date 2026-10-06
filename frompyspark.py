from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import os
from pyspark.sql import SparkSession

os.environ['TEMP'] = 'C:\\BigdataProject'
os.environ['TMP'] = 'C:\\BigdataProject'
# Trỏ chính xác đến thư mục vừa giải nén
os.environ['JAVA_HOME'] = 'C:\\Java\\jdk-17.0.20.1+1'

spark = SparkSession.builder.appName("Data_Transformation").getOrCreate()
df = spark.read.csv("C:/BigdataProject/Bigdata/Bigdata/data.csv", header=True, inferSchema=True)

# 2. Đường dẫn đến file dữ liệu của nhóm
file_path = r"C:\BigdataProject\Bigdata\Bigdata\Order_delivery.csv"

# 3. Đọc dữ liệu gốc
df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(file_path)

print(f"=== Số lượng bản ghi ban đầu: {df_raw.count()} ===")

# 4. Làm sạch dữ liệu (Data Cleaning) & Chuẩn hóa theo yêu cầu nhóm
df_clean = (
    df_raw
    .dropDuplicates()                                              # Loại bỏ dòng trùng lặp hoàn toàn
    .na.drop(subset=["Order_ID", "User_ID"])                        # Xóa dòng thiếu khóa chính
    .withColumn("Item_Name", trim(col("Item_Name")))                # Làm sạch khoảng trắng tên món
    .withColumn("City", trim(col("City")))                          # Làm sạch khoảng trắng thành phố
    # Chuẩn hóa cột `revenue` dựa trên Total_Price
    .withColumn("revenue", col("Total_Price"))                      
)

# Lọc bỏ các dòng có số lượng hoặc giá tiền âm bất thường
df_clean = (
    df_clean
    .na.fill({"Quantity": 0, "revenue": 0.0})
    .filter((col("Quantity") >= 0) & (col("revenue") >= 0))
)

# Tạo sẵn cột `order_segment` (Phân khúc đơn hàng) 
df_clean = df_clean.withColumn(
    "order_segment",
    when(col("revenue") < 200, "Low")
    .when((col("revenue") >= 200) & (col("revenue") <= 500), "Medium")
    .otherwise("High")
)

print(f"=== Số lượng bản ghi sau khi làm sạch (`df_clean`): {df_clean.count()} ===")

# 5. Chuyển đổi sang Pandas DataFrame và lưu ra 1 file CSV duy nhất 
output_csv_path = r"C:\BigdataProject\Bigdata\Bigdata\df_clean_final.csv"

# Chuyển từ Spark DataFrame sang Pandas DataFrame
pdf_clean = df_clean.toPandas()

# Lưu file CSV bằng Pandas
pdf_clean.to_csv(output_csv_path, index=False, encoding="utf-8-sig")

print(f"🎉 Đã xuất file `df_clean` thành công ra CSV tại: {output_csv_path}")

# Đăng ký bảng tạm `df_clean` để phục vụ Spark SQL nếu các thành viên khác cần
df_clean.createOrReplaceTempView("df_clean")