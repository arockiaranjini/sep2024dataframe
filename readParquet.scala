import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.util.DropMalformedMode
////read from datasources
object readParquet extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "myfirstapplication")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val ordersDf = spark.read
    // .format("parquet") no need to give it is a default
    .option("path", "C:/Users/MCT/Desktop/sparkdataset/users.parquet")
    .load
  ordersDf.printSchema()
  ordersDf.show
}

