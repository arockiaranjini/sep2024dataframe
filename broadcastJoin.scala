import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
object broadcastJoin extends App {

  //when on dataset is small and another one is big

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
    .option("path", "C:/Users/mctcl/Desktop/sparkdataset/orders.csv")
    .load
  ordersDf.show()

  val customerDf= spark.read
    .format("csv")
    .option("header",true)
    .option("inferSchema",true)
    .option("path", "C:/Users/mctcl/Desktop/sparkdataset/smallcust.csv")
    .load
  customerDf.show()
  spark.sql("SET.sql.autoBroadcastJoinThreshold=-1")

  val joinCondition = ordersDf.col("order_id")=== customerDf.col("customer_id")

  val joinType ="inner"
  //outer,right,left

  val joinDf = ordersDf.join(broadcast(customerDf),joinCondition,joinType)

  // val joinDf = ordersDf.join(broadcast(customerDf),ordersDf.col("order_customer_id")===
  //customerDf.col("customer_id"),"inner")

  joinDf.show

  spark.stop()
}

