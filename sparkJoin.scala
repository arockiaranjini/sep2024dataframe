import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object sparkJoin extends App {

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
    .option("path", "C:/Users/MCT/Desktop/sparkdataset/order1.csv")
    .load
  ordersDf.show()
  val customerDf = spark.read
    .format("csv")
    .option("header",true)
    .option("inferSchema",true)
    .option("path", "C:/Users/MCT/Desktop/sparkdataset/cust1.csv")
    .load
  customerDf.show()
 // val joinCondition = ordersDf.col("cust_id")=== customerDf.col("cust_id")

  //val joinType ="inner"
  //outer,right,left

  //val joinDf = ordersDf.join(customerDf,joinCondition,joinType)
  val joinDf = ordersDf.join(customerDf,ordersDf.col("cust_id")===customerDf.col("cust_id"),"inner")

  joinDf.show

  spark.stop()
}

