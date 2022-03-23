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

class Q4Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, date, text, parquet)
    val input = opt[String]("input", descr = "Input path", required = true)
    val date = opt[String]("date", descr = "Date in YYYY-MM-DD format", required = true)
    val text = opt[Boolean]("text", descr = "text command processes input as text file", required = false)
    val parquet = opt[Boolean]("parquet", descr = "parquet command processes input as parquet file", required = false)
    verify()
}

object Q4{
    val log = Logger.getLogger(getClass().getName())

    def main(argv: Array[String]){
        val args = new Q4Conf(argv)

        log.info("Input: " + args.input())
        log.info("Date: " + args.date())
        log.info("Is input processed as text file: " + args.text())
        log.info("Is input processed as parquet file: " + args.parquet())

        val sparkSession = SparkSession.builder.appName("A6Q2").getOrCreate
        val date:String = args.date()

        val isParquet:Boolean = args.parquet()
        val limit = 20

        val o_custkeyPos = 1
        val o_orderkeyPos = 0
        
        val l_orderkeyPos = 0
        val l_shipdatePos = 10

        val c_custkeyPos = 0
        val c_nationkeyPos = 3

        val n_nationkeyPos = 0
        val n_namePos = 1

        val queryResult  = if(!isParquet){
            //Process as TXT file
            val lineitemRDD = sparkSession.sparkContext.textFile(args.input()+"/lineitem.tbl")
            val ordersRDD = sparkSession.sparkContext.textFile(args.input()+"/orders.tbl")
            val customerRDD = sparkSession.sparkContext.textFile(args.input()+"/customer.tbl")
            val nationRDD = sparkSession.sparkContext.textFile(args.input()+"/nation.tbl")

            val lineItemProjection = lineitemRDD.map(line => line.split('|'))
                .filter(_.apply(l_shipdatePos).equals(date))
                .map(line => (line.apply(l_orderkeyPos).toInt, line.apply(l_shipdatePos)))

            val ordersProjection = ordersRDD.map(line => {
                val row = line.split('|')
                (row.apply(o_orderkeyPos).toInt, row.apply(o_custkeyPos).toInt)
            })

            val customerProjection = customerRDD.map(line => {
                val row = line.split('|')
                (row.apply(c_custkeyPos).toInt, row.apply(c_nationkeyPos).toInt)
            })

            val nationProjection = nationRDD.map(line => {
                val row = line.split('|')
                (row.apply(n_nationkeyPos).toInt, row.apply(n_namePos))
            })

            val customerMap = customerProjection.collectAsMap()
            val nationMap = nationProjection.collectAsMap()

            lineItemProjection.cogroup(ordersProjection)
                .filter(_._2._1.size > 0)
                .filter(_._2._2.size > 0)
                .filter{case (key,value) => {
                    val o_custkey = value._2.toList.apply(0)
                    customerMap.contains(o_custkey)        
                }}
                .map{case (key,value) => {
                    val o_custkey = value._2.toList.apply(0)
                    customerMap.getOrElse(o_custkey, 0)
                }}
                .filter(_ <= 0)
                .filter{case (c_nationkey) => nationMap.contains(c_nationkey)}
                .map(n_nationkey => (n_nationkey, 1))
                .reduceByKey(_ + _)
                .sortBy(_._1)
                .map{case (n_nationkey, count) =>{
                    val n_name = nationMap.getOrElse(n_nationkey,"")
                    ((n_nationkey, n_name), count)
                }}
            
        }else{            
            val lineitemRDD = sparkSession.read.parquet(args.input()+"/lineitem").rdd
            val ordersRDD = sparkSession.read.parquet(args.input()+"/orders").rdd
            val customerRDD = sparkSession.read.parquet(args.input()+"/customer").rdd
            val nationRDD = sparkSession.read.parquet(args.input()+"/nation").rdd

            val lineItemProjection = lineitemRDD.filter(_.getString(l_shipdatePos).equals(date))
                .map(row => (row.getInt(l_orderkeyPos), row.getString(l_shipdatePos)))

            val ordersProjection = ordersRDD.map(row => (row.getInt(o_orderkeyPos), row.getInt(o_custkeyPos)))
            val customerProjection = customerRDD.map(row => (row.getInt(c_custkeyPos), row.getInt(c_nationkeyPos)))
            val nationProjection = nationRDD.map(row => (row.getInt(n_nationkeyPos), row.getString(n_namePos)))

            val customerMap = customerProjection.collectAsMap()
            val nationMap = nationProjection.collectAsMap()

            lineItemProjection.cogroup(ordersProjection)
                .filter(_._2._1.size > 0)
                .filter(_._2._2.size > 0)
                .filter{case (key,value) => {
                    val o_custkey = value._2.toList.apply(0)
                    customerMap.contains(o_custkey)        
                }}
                .map{case (key,value) => {
                    val o_custkey = value._2.toList.apply(0)
                    customerMap.getOrElse(o_custkey, 0)
                }}
                .filter(_ <= 0)
                .filter{case (c_nationkey) => nationMap.contains(c_nationkey)}
                .map(n_nationkey => (n_nationkey, 1))
                .reduceByKey(_ + _)
                .sortBy(_._1)
                .map{case (n_nationkey, count) =>{
                    val n_name = nationMap.getOrElse(n_nationkey,"")
                    ((n_nationkey, n_name), count)
                }}
        }

        queryResult.foreach{case (key,value) => {
            val n_nationkey = key._1
            val n_name = key._2
            val count = value
            println("("+n_nationkey+","+n_name+","+count+")")
        }}
        
    }
} 