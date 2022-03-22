package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.Map
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.rdd.PairRDDFunctions
import org.rogach.scallop._
import java.io._  
import math._

class Q2Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, date, text, parquet)
    val input = opt[String]("input", descr = "Input path", required = true)
    val date = opt[String]("date", descr = "Date in YYYY-MM-DD format", required = true)
    val text = opt[Boolean]("text", descr = "text command processes input as text file", required = false)
    val parquet = opt[Boolean]("parquet", descr = "parquet command processes input as parquet file", required = false)
    verify()
}

object Q2{
    val log = Logger.getLogger(getClass().getName())

    def main(argv: Array[String]){
        val args = new Q2Conf(argv)

        log.info("Input: " + args.input())
        log.info("Date: " + args.date())
        log.info("Is input processed as text file: " + args.text())
        log.info("Is input processed as parquet file: " + args.parquet())

        val sparkSession = SparkSession.builder.appName("A6Q2").getOrCreate
        val date:String = args.date()

        val isParquet:Boolean = args.parquet()
        val limit = 20

        val queryResult  = if(!isParquet){
            //Process as TXT file
            val lineitemRDD = sparkSession.sparkContext.textFile(args.input()+"/lineitem.tbl")
            val ordersRDD = sparkSession.sparkContext.textFile(args.input()+"/orders.tbl")
            
            val lineItemProjection = lineitemRDD.map(row => row.split('|'))
                .filter(_.apply(10).equals(date))
                .map(row => (row.apply(0), row.apply(10)))

            val ordersProjection = ordersRDD.map(row => {
                val arr = row.split('|')
                (arr.apply(0), arr.apply(5))
            })

            lineItemProjection.cogroup(ordersProjection)
                .filter(_._2._1.size > 0)
                .filter(_._2._2.size > 0)
                .map{case (o_clerk,value) => {
                    val o_orderkey = value._2.toList
                    (o_orderkey, o_clerk)
                }}
                .sortBy(_._2)
                .take(limit)
            
        }else{
            val lineitemRDD = sparkSession.read.parquet(args.input()+"/lineitem").rdd
            val ordersRDD = sparkSession.read.parquet(args.input()+"/orders").rdd
            val lineItemProjection:RDD[(Int, String)] = lineitemRDD.filter(_.getString(10).equals(date)).map(row => (row.apply(0), row.apply(10)))
            val ordersProjection:RDD[(Int, String)] = ordersRDD.map(row => (row.apply(0), row.apply(5)))

            lineItemProjection.cogroup(ordersProjection)
                .filter(_._2._1.size > 0)
                .filter(_._2._2.size > 0)
                .map{case (o_clerk,value) => {
                    val o_orderkey = value._2.toList
                    (o_orderkey, o_clerk)
                }}
                .sortBy(_._2)
                .take(limit)
        }

        println(s"ANSWER=${queryResult}")
    }
} 