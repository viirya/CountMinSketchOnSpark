
package org.viirya

import scala.collection.mutable

import org.scalatest.{BeforeAndAfterAll, FunSuite, Suite}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

class CountMinSketchSuite extends FunSuite with BeforeAndAfterAll { self: Suite =>
  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("CountMinSketchUnitTest")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._
  import org.viirya.CountMinSketch._

  private def toLetter(i: Int): String = (i + 97).toChar.toString

  test("Count-min sketch to estimate frequencies") {
    val rows = Seq.tabulate(20) { i =>
      if (i % 3 == 0) (1, toLetter(1), -1.0) else (i, toLetter(i), i * -1.0)
    }

    var numOfOne = 0
    rows.foreach{ r =>
      if (r._1 == 1) numOfOne += 1
    }

    val df = rows.toDF("numbers", "letters", "negDoubles")
    val results = countMinSketch(df, "numbers").collect()

    results.foreach{ row =>
      if (row.getInt(0) == 1) {
        assert(row.getLong(1) > numOfOne * 0.8) 
      } else {
        assert(row.getLong(1) === 1L)
      }
    }    
  }

  test("Count-min sketch to estimate frequencies with specified depth and width") {
    val rows = Seq.tabulate(1000) { i =>
      if (i % 3 == 0) (1, toLetter(1), -1.0) else (i, toLetter(i), i * -1.0)
    }

    var numOfOne = 0
    rows.foreach{ r =>
      if (r._1 == 1) numOfOne += 1
    }

    val df = rows.toDF("numbers", "letters", "negDoubles")
    val results = countMinSketch(df, "numbers", 1000, 10).collect()

    results.foreach{ row =>
      if (row.getInt(0) == 1) {
        assert(row.getLong(1) > numOfOne * 0.8) 
      } else {
        assert(row.getLong(1) === 1L)
      }
    }    
  }
}
