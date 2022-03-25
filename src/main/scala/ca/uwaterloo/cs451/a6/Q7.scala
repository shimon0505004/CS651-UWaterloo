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
import scala.math.Ordering.Implicits
import org.rogach.scallop._
import java.io._  
import math._

class Q7Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, date, text, parquet)
    val input = opt[String]("input", descr = "Input path", required = true)
    val date = opt[String]("date", descr = "Date in YYYY-MM-DD format", required = true)
    val text = opt[Boolean]("text", descr = "text command processes input as text file", required = false)
    val parquet = opt[Boolean]("parquet", descr = "parquet command processes input as parquet file", required = false)
    verify()
}

object Q7{
    val log = Logger.getLogger(getClass().getName())

    def main(argv: Array[String]){
        val args = new Q7Conf(argv)

        log.info("Input: " + args.input())
        log.info("Date: " + args.date())
        log.info("Is input processed as text file: " + args.text())
        log.info("Is input processed as parquet file: " + args.parquet())

        val sparkSession = SparkSession.builder.appName("A6Q7").getOrCreate
        val date:String = args.date()

        val isParquet:Boolean = args.parquet()

        val limit = 5

        val l_orderkeyPos       = 0
        val l_extendedpricePos  = 5
        val l_discountPos       = 6
        val l_shipdatePos       = 10

        val lineItemProjection = if(!isParquet){
            sparkSession.sparkContext.textFile(args.input()+"/lineitem.tbl")
                        .map(line => line.split('|'))
                        .filter(_.apply(l_shipdatePos).compareTo(date) > 0)
                        .map(row => {
                            val l_orderKey = row.apply(l_orderkeyPos).toInt
                            val l_extendedprice = row.apply(l_extendedpricePos).toDouble
                            val l_discount = row.apply(l_discountPos).toDouble
                            val l_revenue =  l_extendedprice * (1 - l_discount)
                            (l_orderKey,l_revenue)
                        })
        }else{
            sparkSession.read.parquet(args.input()+"/lineitem").rdd
                        .filter(_.getString(l_shipdatePos).compareTo(date) > 0)
                        .map(row => {
                            val l_orderKey = row.getInt(l_orderkeyPos)
                            val l_extendedprice = row.getDouble(l_extendedpricePos)
                            val l_discount = row.getDouble(l_discountPos)
                            val l_revenue =  l_extendedprice * (1 - l_discount)
                            (l_orderKey, l_revenue)
                        })
        }
        
        val o_orderkeyPos       = 0
        val o_custkeyPos        = 1
        val o_orderdatePos      = 4
        val o_shippriorityPos   = 7

        val ordersProjection = if(!isParquet){
            sparkSession.sparkContext.textFile(args.input()+"/orders.tbl")
                        .map(line => line.split('|'))
                        .filter(_.apply(o_orderdatePos).compareTo(date) < 0 )
                        .map(row => {
                            val o_orderkey = row.apply(o_orderkeyPos).toInt
                            val o_custkey = row.apply(o_custkeyPos).toInt
                            val o_orderdate = row.apply(o_orderdatePos)
                            val o_shippriority = row.apply(o_shippriorityPos).toInt
                            (o_orderkey, (o_custkey, o_orderdate, o_shippriority))
                        })
        }else{
            sparkSession.read.parquet(args.input()+"/orders").rdd
                        .filter(_.getString(o_orderdatePos).compareTo(date) < 0)
                        .map(row => {
                            val o_orderkey = row.getInt(o_orderkeyPos)
                            val o_custkey = row.getInt(o_custkeyPos)
                            val o_orderdate = row.getString(o_orderdatePos)
                            val o_shippriority = row.getInt(o_shippriorityPos)
                            (o_orderkey, (o_custkey, o_orderdate, o_shippriority))
                        })
        }


        val c_custkeyPos = 0
        val c_namePos    = 1

        val customerProjection = if(!isParquet){
            sparkSession.sparkContext.textFile(args.input()+"/customer.tbl")
                        .map(line => line.split('|'))
                        .map(row => {
                            val c_custkey = row.apply(c_custkeyPos).toInt
                            val c_name = row.apply(c_namePos)
                            (c_custkey, c_name)
                        })
        }else{
            sparkSession.read.parquet(args.input()+"/customer").rdd
                        .map(row => {
                            val c_custkey = row.getInt(c_custkeyPos)
                            val c_name = row.getString(c_namePos)
                            (c_custkey, c_name)
                        })
        }

        val customerMap = customerProjection.collectAsMap()
        val broadcastcustomerMap = sparkSession.sparkContext.broadcast(customerMap)

        val queryResult = lineItemProjection.cogroup(ordersProjection)
                                            .filter{case (orderkey,(lineitemValues, ordersValues)) => {                                                
                                                (lineitemValues.size) > 0 && (ordersValues.size > 0)  
                                            }}
                                            .filter{case (orderkey,(lineitemValues, ordersValues)) => {                                                
                                                val o_custkey = ordersValues.toList.apply(0)._1
                                                broadcastcustomerMap.value.contains(o_custkey)
                                            }}
                                            .map{case (l_orderkey,(lineitemValues, ordersValues)) => {     
                                                val o_custkey = ordersValues.toList.apply(0)._1                                           
                                                val o_orderdate = ordersValues.toList.apply(0)._2
                                                val o_shippriority = ordersValues.toList.apply(0)._3
                                                val c_name = broadcastcustomerMap.value.getOrElse(o_custkey, "")  
                                                val l_revenue = lineitemValues.toList.apply(0)
                                                ((c_name, l_orderkey, o_orderdate, o_shippriority), l_revenue)
                                            }}
                                            .reduceByKey(_ + _)
                                            .sortBy{case((c_name, l_orderkey, o_orderdate, o_shippriority), revenue) => -revenue}
                                            .collect
                                            .take(limit)
                                            

        queryResult.foreach{case((c_name, l_orderkey, o_orderdate, o_shippriority), revenue) => {
             println("(" + c_name + "," + l_orderkey + "," + revenue + "," + o_orderdate + "," + o_shippriority + ")")
        }}
        
        
        /*
        if(isParquet){
            val lineitemDF = sparkSession.read.parquet(args.input()+"/lineitem")
            val ordersDF = sparkSession.read.parquet(args.input()+"/orders")
            val customreDF = sparkSession.read.parquet(args.input()+"/customer")

            lineitemDF.createOrReplaceTempView("lineitem")
            ordersDF.createOrReplaceTempView("orders")
            customreDF.createOrReplaceTempView("customer")

            val result = sparkSession.sql("""select
                                                c_name,
                                                l_orderkey,
                                                sum(l_extendedprice*(1-l_discount)) as revenue,
                                                o_orderdate,
                                                o_shippriority
                                            from customer, orders, lineitem
                                            where
                                                c_custkey = o_custkey and
                                                l_orderkey = o_orderkey and
                                                o_orderdate < """"+ date +"""" and
                                                l_shipdate > """"+ date +""""
                                            group by
                                                c_name,
                                                l_orderkey,
                                                o_orderdate,
                                                o_shippriority
                                            order by
                                                revenue desc
                                            limit 5""")
                                     .show(200, false)

        }
        */
        
        
    }
} 