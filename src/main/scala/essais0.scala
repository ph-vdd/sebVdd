package main.scala

import com.typesafe.config._
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.io.File
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.size
import org.apache.spark.sql.functions.substring
import java.util.regex.Matcher

object essais0 {

  val configFilePath: String = "conf/application_j2.conf"
  val jobConfig: Config = ConfigFactory.parseFile(new File(configFilePath))
  var sparkConf: SparkConf = null
  var sc: SparkContext = null
  var spark: SparkSession = null

  def init() = {
    //    sparkConf = new SparkConf().setAppName("MyStandardScalerDecathlon").setMaster("local[*]")
    //    sc = new SparkContext(sparkConf)
    //    sc.setLogLevel("ERROR")
    //    spark = SparkSession.builder.master("local").getOrCreate
    //    spark.sparkContext.setLogLevel("ERROR")
  }

  def main(args: Array[String]): Unit = {
    regex()    
    //verif()
  }
  
  def debut(): Unit = {
    print("Debut")
  }
  
  def regex(): Unit = {
    
    val string = "one493two483three"
    val pattern = """two(\d+)three""".r
    pattern.findAllIn(string).foreach(println)

    val filename_pattern = "^SFMC_events_([0-9]{8})\\.csv$".r
    filename_pattern.findAllIn("SFMC_events_12345678.csv").foreach(println)
    val filename_glob = "SFMC_events_*.csv"

  }
  
  def autre(s: String): String = {
    s+"OK"
  }
  
  def verif(): String = {
    val configFilePath: String = "C:\\Users\\pvandendriessche\\workspace\\vdd7\\conf\\application_j2.conf"
    val jobConfig: Config = ConfigFactory.parseFile(new File(configFilePath))
    val obj=jobConfig.getConfig("referentials").getConfig("pi2")
    val str=obj.getString("database")+obj.getString("table_pi2")
    str
  }
  
  def getValue(valeur: String): String = {
    "Bonjour "+valeur
  }


  def getConfiguration(s: String): String = {
//    val dbConfig:Config = ConfigFactory.load(jobConfig)
//    dbConfig.getString(s)
    ""
  }
  
  def dfPfs(spark: SparkSession): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load("Source/pfr.csv")
  }
  
  def getPfsReturns(spark: SparkSession): DataFrame = {
    import spark.implicits._
    dfPfs(spark)
      .select(
        $"orderid".as("orderidRet"),
        $"rcuid".as("rcuidRet"),
        $"line_number".as("line_numberRet"),
        $"cmmf".as("cmmfRet"),
        $"amount".as("amountRet"),
        $"quantity".as("qteRet").cast("Int"))
      .withColumn("key",concat($"orderidRet",$"cmmfRet"))
      .groupBy("key") // Attention il se peut qu'il y ai des retours multiples pour la même commande
      .sum("qteRet")
      .withColumnRenamed("sum(qteRet)","qtRet")
  }
}