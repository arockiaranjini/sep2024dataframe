import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.SparkContext
import scala.io.Source
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
case class OrderDatas(emp_ID:Int,emp_NAME:String,DEPT_NAME:String,SALARY:String)

object All_fn extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "wordcount")

  val sparkConf = new SparkConf()
  sparkConf.set("spark.appname", "myfirstapplication")
  sparkConf.set("spark.master", "local[2]")
  //setting up all the properties imported

  //val spark = SparkSession.builder()
  //.config(sparkConf)
  //.config("spark.sql.sources.bucketing.enabled", true)
  //.enableHiveSupport()
  //.getOrCreate()
  //setting up the SparkSession builder

  val ordersDf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:/Users/mctcl/Desktop/sparkdataset/orders223.txt")
    .load
  //ordersDf.show() //loading the first file in the dataframe
  val customerDf = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:/Users/mctcl/Desktop/sparkdataset/cust1.csv")
    .load
  //customerDf.show() // loading the second csv file in an dataframe
  customerDf.printSchema() //identifying and printing the datatypes used

  val joinDf = ordersDf.as("o").join(customerDf.as("c"), col("o.cust_id ") === col("c.cust_id"), "Full")
    //creating aliases for each dataframe, so that it can be used to join the two dataframes

    .select(col("o.cust_id "), col("c.cust_name"), col("o.order_status"))
  //selecting each column from the two tables using aliases to join the tables.
  joinDf.show

  print("joinDf has=" + joinDf.rdd.getNumPartitions + "\n")
  //priting the number of partition is created of the given dataframe

  val renamedorderitemsDf = joinDf.withColumnRenamed("order_status", "final_status")
  //renamed a column header

  renamedorderitemsDf.write
    .format("csv")
    .partitionBy("final_status")
    .option("maxRecordsPerFile", 200)
    .mode(SaveMode.Overwrite)
    .option("path", "C:/Users/mctcl/Desktop/sparkdataset/AllFunctionProject_SEP)")
    .save()
  //using partitionby to partition the records based on the order_status
  //BUCKETING
  val spark = SparkSession.builder()
    .config(sparkConf)
    //to integrate hive with spark
    .config("spark.sql.sources.bucketing.enabled", true)
    .enableHiveSupport()
    .getOrCreate()

  val dk = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "C:/Users/mctcl/Desktop/sparkdataset/order1.csv")
    .load
  //create own databse
  //C:\apple%20intelli\movie\spark-warehouse\newretail.db
  spark.sql("create database if not exists newretail")

  dk.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .bucketBy(2, "order_id")
    .sortBy("order_id")
    .saveAsTable("newretail.orders8")
  //using buckets


  val empDf = spark.read
    .format("csv")
    .option("header", true)
    .option("path", "C:/Users/mctcl/Desktop/sparkdataset/emp1.csv")
    .option("mode", "DROPMALFORMED")
    // .option("mode","FAILFAST")
    .load
  empDf.show() //using the readmodes
  println(empDf.count)


  val input = sc.textFile("C:/Users/mctcl/Desktop/sparkdataset/Data.txt")

  val mappedInput = input.map(x => (x.split(",")(0), x.split(",")(2).toFloat)).cache()
  //val mappedInput = input.map(x => (x.split(",")(0), x.split(",")(2).toFloat)).persist(StorageLevel.MEMORY_ONLY)
  //val mappedInput = input.map(x => (x.split(",")(0), x.split(",")(2).toFloat)).persist(StorageLevel.DISK_ONLY)
  mappedInput.take(10).foreach(println)
  //USED CACHE AND PERSIST

  val combining = mappedInput.reduceByKey((x, y) => x + y)
  val sorting = combining.sortBy(x => x._2, false)
  val filtering = sorting.filter(x => x._2 > 200)
  filtering.collect.foreach(println)
  //empDf.printSchema()
  //extracting two different columns and and printing it after adding sortby and filter to it

  val parquetDf = spark.read
    .option("path", "C:/Users/mctcl/Desktop/sparkdataset/users.parquet")
    .load
  parquetDf.printSchema()
  parquetDf.show
  //reading parquet file
  // spark reads the parquet file by default

  val ACCUMV = sc.textFile("C:/Users/mctcl/Desktop/sparkdataset/Wordcount.txt")
  ACCUMV.collect.foreach(println)
  val myAccumulator = sc.longAccumulator("blank lines accumulator")

  ACCUMV.foreach(x=>if(x.contains("Microsoft"))myAccumulator.add(1))
  println("Number of lines containing 'Microsoft': " + myAccumulator.value)
  //Accumvariable count of a particualr word

  def loadBoaringWords(): Set[String] = {

    var boringWords: Set[String] = Set()

    val lines = Source.fromFile("C:/Users/mctcl/Desktop/sparkdataset/boaringwords.txt").getLines()

    for (line <- lines) {
      //put into the set
      boringWords += line
    }
    boringWords
  }

  var broadcastSet = sc.broadcast(loadBoaringWords)
  println("Words removed=" + broadcastSet.value)
  val inputNew = sc.textFile("C:/Users/mctcl/Desktop/sparkdataset/datafile.txt")
  val words = inputNew.flatMap(x => x.split(" "))
  val wordmap = words.map(x => (x, 1))
  val wordsRem = wordmap.filter(x => !broadcastSet.value(x._1))
  val reduceKey = wordsRem.reduceByKey(_ + _)
  reduceKey.collect.foreach(println)
  //boarding words

  val safetyDf: Dataset[Row] = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .csv("C:/Users/mctcl/Desktop/sparkdataset/emp1.csv") //safety key

  import spark.implicits._

  val ordersDbs = safetyDf.as[OrderDatas]
  ordersDbs.filter(x => x.emp_ID < 130)
  ordersDbs.collect.foreach(println)


  scala.io.StdIn.readLine()
}
