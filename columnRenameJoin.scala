import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object columnRenameJoin extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appname", "myfirstapplication")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  val ordersDf = spark.read
    .format("csv")
    .option("header",true)
    .option("inferSchema",true)
    .option("path", "C:/Users/MCT/Desktop/sparkdataset/csvfolder/orders.csv")
    .load
  //ordersDf.show()
  val customerDf = spark.read
    .format("csv")
    .option("header",true)
    .option("inferSchema",true)
    .option("path","C:/Users/MCT/Desktop/sparkdataset/csvfolder/customers.csv")
    .load
  //customerDf.show()

  val orderitemsDf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:/Users/MCT/Desktop/sparkdataset/csvfolder/order_items.csv")
    .load
  orderitemsDf.show()
  val renamedorderitemsDf=orderitemsDf.withColumnRenamed("order_id","orders_id")
    .withColumnRenamed("list_price","lists_price")
  renamedorderitemsDf.show
  val joinDf = ordersDf
    .join(customerDf,ordersDf.col("customer_id")=== customerDf.col("customer_id"),"inner")
    .join(renamedorderitemsDf,ordersDf.col("order_id")===renamedorderitemsDf.col("orders_id"),"inner")

  //joinDf.show
  joinDf.select("order_id","order_date","first_name","email","product_id","lists_price").show

  spark.stop()
}
