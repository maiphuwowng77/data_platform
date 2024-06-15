import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


val logDF = spark.read.parquet("hdfs://localhost:9000/raw_zone/fact/activity/log_action.parquet")
val svDF = spark.read.csv("hdfs://localhost:9000/test/danh_sach_sv_de.csv")


val uniqueStudentNames = svDF.select("_c1").distinct().collect().map(_.getString(0))

uniqueStudentNames.foreach { studentName =>
  val filteredDF = logDF.join(svDF, logDF("_c0") === svDF("_c0"))
                    .filter(svDF("_c1") === studentName)
                    .groupBy(logDF("_c3").as("date"), svDF("_c0").as("student_code"), svDF("_c1").as("student_name"), logDF("_c1").as("activity"))
                    .agg(sum(logDF("_c2")).as("totalFile"))
                    .orderBy(col("date").asc)
  val outputPath = s"hdfs://namenode:9000/output/student_activity_summary/student_log/${studentName.replace(" ", "_")}.csv"

// Lưu dữ liệu vào file csv
  filteredDF
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", true)
    .csv(outputPath)
}
