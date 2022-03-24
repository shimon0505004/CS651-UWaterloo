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

class Q5Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, date, text, parquet)
    val input = opt[String]("input", descr = "Input path", required = true)
    val date = opt[String]("date", descr = "Date in YYYY-MM-DD format", required = true)
    val text = opt[Boolean]("text", descr = "text command processes input as text file", required = false)
    val parquet = opt[Boolean]("parquet", descr = "parquet command processes input as parquet file", required = false)
    verify()
}

object Q5{
    val log = Logger.getLogger(getClass().getName())

    def main(argv: Array[String]){
        val args = new Q5Conf(argv)

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
        val l_quantityPos = 4
        val l_shipdatePos = 10

        val c_custkeyPos = 0
        val c_nationkeyPos = 3

        val n_nationkeyPos = 0
        val n_namePos = 1


        val lineItemProjection = if(!isParquet){
            sparkSession.sparkContext.textFile(args.input()+"/lineitem.tbl")
                        .map(line => line.split('|'))
                        .filter(_.apply(l_shipdatePos).equals(date))
                        .map(line => (line.apply(l_orderkeyPos).toInt, line.apply(l_quantityPos).toLong))
        }else{
            sparkSession.read.parquet(args.input()+"/lineitem").rdd
                        .filter(_.getString(l_shipdatePos).equals(date))
                        .map(row => (row.getInt(l_orderkeyPos), row.getDouble(l_quantityPos).toLong))
        }

        val ordersProjection = if(!isParquet){
            sparkSession.sparkContext.textFile(args.input()+"/orders.tbl")
                        .map(line => {
                            val row = line.split('|')
                            (row.apply(o_orderkeyPos).toInt, row.apply(o_custkeyPos).toInt)
                        })
        }else{
            sparkSession.read.parquet(args.input()+"/orders").rdd
                        .map(row => (row.getInt(o_orderkeyPos), row.getInt(o_custkeyPos)))
        }

        val customerProjection = if(!isParquet){
            sparkSession.sparkContext.textFile(args.input()+"/customer.tbl")
                        .map(line => {
                            val row = line.split('|')
                            (row.apply(c_custkeyPos).toInt, row.apply(c_nationkeyPos).toInt)
                        })
        }else{
            sparkSession.read.parquet(args.input()+"/customer").rdd
                        .map(row => (row.getInt(c_custkeyPos), row.getInt(c_nationkeyPos)))
        }

        val nationProjection = if(!isParquet){
            sparkSession.sparkContext.textFile(args.input()+"/nation.tbl")
                        .map(line => {
                            val row = line.split('|')
                            (row.apply(n_nationkeyPos).toInt, row.apply(n_namePos))
                        })
        }else{
            sparkSession.read.parquet(args.input()+"/nation").rdd
                        .map(row => (row.getInt(n_nationkeyPos), row.getString(n_namePos)))
        }

        val customerMap = customerProjection.collectAsMap()
        val nationMap = nationProjection.collectAsMap()

        val broadcastcustomerMap = sparkSession.sparkContext.broadcast(customerMap)
        val broadcastnationMap = sparkSession.sparkContext.broadcast(nationMap)

        val queryResult = lineItemProjection.cogroup(ordersProjection)
                                            .filter(_._2._1.size > 0)
                                            .filter(_._2._2.size > 0)
                                            .filter{case (key,value) => {
                                                val o_custkey = value._2.toList.apply(0)
                                                broadcastcustomerMap.value.contains(o_custkey)        
                                            }}
                                            .map{case (key,value) => {
                                                val o_custkey = value._2.toList.apply(0)
                                                val n_nationkey = broadcastcustomerMap.value.getOrElse(o_custkey, -1)                                                
                                                n_nationkey
                                            }}
                                            .filter{n_nationkey => broadcastnationMap.value.contains(n_nationkey)}
                                            .map(n_nationkey => (n_nationkey, 1))
                                            .reduceByKey(_ + _)
                                            .sortByKey()
                                            .map{case (n_nationkey, count) => ((n_nationkey, broadcastnationMap.value.getOrElse(n_nationkey,"")), count)}
                                            .collect

        queryResult.foreach{case ((n_nationkey, n_name), count) => {
            println("("+n_nationkey+","+n_name+","+count+")")
        }}
        
        /*
        if(isParquet){
            val lineitemDF = sparkSession.read.parquet(args.input()+"/lineitem")
            val ordersDF = sparkSession.read.parquet(args.input()+"/orders")
            val customreDF = sparkSession.read.parquet(args.input()+"/customer")
            val nationDF = sparkSession.read.parquet(args.input()+"/nation")

            lineitemDF.createOrReplaceTempView("lineitem")
            ordersDF.createOrReplaceTempView("orders")
            customreDF.createOrReplaceTempView("customer")
            nationDF.createOrReplaceTempView("nation")
            val result = sparkSession.sql("select n_nationkey, n_name, count(*) from lineitem, orders, customer, nation where l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n_nationkey and l_shipdate = '"+date+"' group by n_nationkey, n_name order by n_nationkey asc").show()

        }
        */
    }
} 