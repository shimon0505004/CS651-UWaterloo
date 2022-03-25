package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.Map
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.rogach.scallop._
import java.io._  
import math._

class Q1Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, date, text, parquet)
    val input = opt[String]("input", descr = "Input path", required = true)
    val date = opt[String]("date", descr = "Date in YYYY-MM-DD format", required = true)
    val text = opt[Boolean]("text", descr = "text command processes input as text file", required = false)
    val parquet = opt[Boolean]("parquet", descr = "parquet command processes input as parquet file", required = false)
    verify()
}

object Q1{
    val log = Logger.getLogger(getClass().getName())

    def main(argv: Array[String]){
        val args = new Q1Conf(argv)

        log.info("Input: " + args.input())
        log.info("Date: " + args.date())
        log.info("Is input processed as text file: " + args.text())
        log.info("Is input processed as parquet file: " + args.parquet())

        val sparkSession = SparkSession.builder.appName("A6Q1").getOrCreate
        val date:String = args.date()

        val isParquet:Boolean = args.parquet()

        val l_shipdatePos = 10
        val l_quantityPos = 4

        val queryResult  = if(!isParquet){
            //Process as TXT file
            val lineitemRDD = sparkSession.sparkContext.textFile(args.input()+"/lineitem.tbl")
            lineitemRDD.filter(row => row.split('|').apply(l_shipdatePos).equals(date))
                        .map(row => {
                            val arr = row.split('|')
                            val l_quantity = arr.apply(l_quantityPos).toLong
                            l_quantity
                        })
                        .count()
        }else{
            val lineitemRDD = sparkSession.read.parquet(args.input()+"/lineitem").rdd
            lineitemRDD.filter(row => row.getString(l_shipdatePos).equals(date))
                        .map(row => {
                            val l_quantity = row.getDouble(l_quantityPos).toLong
                            l_quantity
                        })
                        .count()
        }

        println(s"ANSWER=${queryResult}")

        //For verifying results of Q3
        if(isParquet){
            val lineitemDF = sparkSession.read.parquet(args.input()+"/lineitem")
            lineitemDF.createOrReplaceTempView("lineitem")
            val result = sparkSession.sql("select count(*) from lineitem where l_shipdate = '" + date + "\'").show()
        }

    }
} 