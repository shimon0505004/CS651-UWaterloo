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

        val queryResult  = if(!isParquet){
            //Process as TXT file
            val lineitemRDD = sparkSession.sparkContext.textFile(args.input()+"/lineitem.tbl")
            lineitemRDD.map(row => row.split('|').apply(10)).filter(_.equals(date)).count()
        }else{
            val lineitemRDD = sparkSession.read.parquet(args.input()+"/lineitem").rdd
            lineitemRDD.map(row => row.getString(10)).filter(_.equals(date)).count()
        }

        println(s"ANSWER=${queryResult}")
    }
} 