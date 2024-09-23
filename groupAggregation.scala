import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object groupAggregation extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appname", "myfirstapplication")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  val invoiceDf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:/Users/MCT/Desktop/sparkdataset/order_data.csv")
    .load

  invoiceDf.createOrReplaceTempView("sales")

  val summarydf2 = spark.sql("select Country," +
    "InvoiceNo,sum(Quantity)as TotalQuantity," +
    "sum(Quantity * UnitPrice)as InvoiceValue from sales " +
    "group by Country, " +
    "InvoiceNo")

  summarydf2.show()
  spark.stop()
}

