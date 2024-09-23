import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object simpleAggre extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appname", "myfirstapplication")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    // .enableHiveSupport()
    .getOrCreate()

  val invoiceDf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path","C:/Ranjini/Ranjini/Dataset/Dataset3/order_data.csv")
    .load
  //coulumn object expression

  invoiceDf.select(
    count("*").as("RowCount"),
    sum("Quantity").as("TotalQuantity"),
    avg("UnitPrice").as("AvgPrice"),
    countDistinct("InvoiceNo").as("CountDistinct")).show
  //string expression
  invoiceDf.selectExpr(
    "count(*) as RouwCount",
    "sum(Quantity) as TotalQuantity",
    "avg(UnitPrice)as AvgPrice",
    "count(Distinct(InvoiceNo)) as CountDistinct"
  ).show()

  //spark sql expression
  invoiceDf.createOrReplaceTempView("sales")
  spark.sql("select count(*)," +
    "sum(Quantity)," +
    "avg(UnitPrice)," +
    "count(distinct(InvoiceNo))" +
    "from sales").show()
  //invoiceDf.show()
  spark.stop()
}
