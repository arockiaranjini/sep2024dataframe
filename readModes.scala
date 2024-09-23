import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.util.DropMalformedMode
////read from datasources
object readModes extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "myfirstapplication")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  // ======read json========//defaultmode permissive mode=====
  val ordersDf = spark.read
    .format("csv")
    .option("header", true) //no header in json
    //.option("path", "C:/Ranjini/Ranjini/Dataset/Dataset3/players1.json")
  .option("path","C:/Users/mctcl/Desktop/sparkdataset/testmal.csv")
   .option("mode","DROPMALFORMED")
    //.option("mode","FAILFAST")
    .load
  ordersDf.printSchema()
  ordersDf.show(false)
}

