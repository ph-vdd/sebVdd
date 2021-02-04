
package test.scala

import java.io.ByteArrayOutputStream
import java.io.File



import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.scalatest._

import main.scala.essais0
import scala.io.Source

class AppSpec2 extends FunSuite {
  val sparkConf = new SparkConf().setAppName("MyStandardScalerDecathlon").setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("ERROR")
  val spark = SparkSession.builder.master("local").getOrCreate
  spark.sparkContext.setLogLevel("ERROR")
  
  var result = 
"""+------------------------------+-----+
|key                           |qtRet|
+------------------------------+-----+
|RWDE05604054000000002211400858|1    |
|RWES05559500000000001830007045|1    |
+------------------------------+-----+
only showing top 2 rows
"""

  test("df Return") {
    val dfPfs: DataFrame = 
    spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load("Source/pfr.csv")
    val res: DataFrame = essais0.getPfsReturns(spark)

    val outCapture = new ByteArrayOutputStream
    Console.withOut(outCapture) {
      res.show(2,false)
    }
    var str = new String(outCapture.toByteArray)
    str = str.slice(0,result.size-1)
    result = result.slice(0,str.size)
    assert(str == result)
  }
  
  test("df Return_with_csv") {
    var pos = -1
    var i = 0
    //var ref:BufferedSource = null 
    //var res:BufferedSource = null 
    try {
      val dfPfs: DataFrame = 
      spark.read
        .format("csv")
        .option("header", "true") //first line in file has headers
        .option("mode", "DROPMALFORMED")
        .load("Source/pfr.csv")
      val dfResult: DataFrame = essais0.getPfsReturns(spark)
      dfResult.coalesce(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .save("result/pfrResult.csv")
      val d = new File("Result/pfrResult.csv").listFiles.filter(_.getName endsWith ".csv").toList
      var fresult = d(0)
      val ref = Source.fromFile("Refere/pfRef.csv")
      val res = Source.fromFile(fresult)
      while (ref.hasNext) {
        var lineRef = ref.next()
        if (res.hasNext) {
          var lineRes = res.next()
          if (lineRef != lineRes) {
            pos = i
          }
          i +=1
        }
      }
    } 
    catch {
        case e: Throwable => {
          e.printStackTrace
          pos = 9999999
        }
    } finally {
      println(i)
    }
    
    assert(pos == -1)
  }

  
//  test("coucou") {
//      println(essais0.verif())
//  }
  
  test("An empty Set should have size 0") {
     assert(Set.empty.size == 0)
  }

  test("autre") {
     assert(essais0.autre("Françoise") == "FrançoiseOK")
  }

//    println(str.trim.size)
//    println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
//    println(str.trim)
//    println("________________________________")
//    println(result.trim)
//    println("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")
//    var x = 0
//    for (x <- 0 to result.size-1 ){
//      if (result.slice(x,x+1) != str.slice(x,x+1)) {
//        println(x,result.slice(x,x+1), str.slice(x,x+1))
//      }
//    }
//    

}