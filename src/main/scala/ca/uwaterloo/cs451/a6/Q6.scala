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

        
        val l_orderkeyPos =         0
        val l_partkeyPos =          1
        val l_suppkeyPos =          2
        val l_linenumberPos =       3
        val l_quantityPos =         4
        val l_extendedpricePos =    5
        val l_discountPos =         6
        val l_taxPos =              7
        val l_returnflagPos =       8        
        val l_linestatusPos =       9
        val l_shipdatePos =         10
        val l_commitdatePos =       11
        val l_receiptdatePos =      12
        val l_shipinstructPos =     13
        val l_shipmodePos =         14
        val l_commentPos =          15        


        val lineItemProjection = if(!isParquet){
            sparkSession.sparkContext.textFile(args.input()+"/lineitem.tbl")
                        .map(line => line.split('|'))
                        .filter(_.apply(l_shipdatePos).equals(date))
                        .map(line => (line.apply(l_orderkeyPos).toInt, line.apply(l_quantityPos).toLong))
                        .map(row => {
                            val l_returnflag = row.apply(l_returnflagPos).toInt
                            val l_linestatus = row.apply(l_linestatusPos)
                            val l_quantity = row.apply(l_quantityPos).toDouble
                            val l_extendedprice = row.apply(l_extendedpricePos).toDouble
                            val disc_price = l_extendedprice * (1 - row.apply(l_discountPos).toDouble)
                            val sum_charge = l_extendedprice * (1 - row.apply(l_discountPos).toDouble) * (1 + row.apply(l_taxPos).toDouble)
                            val l_discount = row.apply(l_discountPos).toDouble
                            ((l_returnflag, l_linestatus), (l_quantity, l_extendedprice, disc_price, sum_charge, l_discount, 1))
                        })
        }else{
            sparkSession.read.parquet(args.input()+"/lineitem").rdd
                        .filter(_.getString(l_shipdatePos).equals(date))
                        .map(row => {
                            val l_returnflag = row.getInt(l_returnflagPos)
                            val l_linestatus = row.getString(l_linestatusPos)
                            val l_quantity = row.getDouble(l_quantityPos)
                            val l_extendedprice = row.getDouble(l_extendedpricePos)
                            val disc_price = l_extendedprice * (1 - row.getDouble(l_discountPos))
                            val sum_charge = l_extendedprice * (1 - row.getDouble(l_discountPos)) * (1 + row.getDouble(l_taxPos))
                            val l_discount = row.getDouble(l_discountPos)
                            ((l_returnflag, l_linestatus), (l_quantity, l_extendedprice, disc_price, sum_charge, l_discount, 1))
                        })
         }

        val queryResult = lineItemProjection.reduceByKey(((l_quantity1, l_extendedprice1, disc_price1, sum_charge1, l_discount1, count1),
                                                            (l_quantity2, l_extendedprice2, disc_price2, sum_charge2, l_discount2, count2)) => ((l_quantity1+l_quantity2), 
                                                                                                                                                (l_extendedprice1+l_extendedprice2), 
                                                                                                                                                (disc_price1+disc_price2), 
                                                                                                                                                (sum_charge1+sum_charge2), 
                                                                                                                                                (l_discount1+l_discount2), 
                                                                                                                                                (count1+count2)))
                                            .map(((l_returnflag, l_linestatus), (sum_qty, sum_base_price, sum_disc_price, sum_charge, sum_discount, count)) => ((l_returnflag, l_linestatus), (sum_qty, sum_base_price, sum_disc_price, sum_charge, sum_qty/count, sum_base_price/count , sum_discount/count, count)))
                                            .collect

        queryResult.foreach{case ((l_returnflag, l_linestatus), (sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_base_price , avg_discount, count_order)) => {
            println("("l_returnflag +","+ l_linestatus +","+ sum_qty +","+ sum_base_price +","+ sum_disc_price +","+ sum_charge +","+ avg_qty +","+ avg_base_price +","+ avg_discount +","+ count_order +")")
        }}
        
        
        if(isParquet){
            val lineitemDF = sparkSession.read.parquet(args.input()+"/lineitem")

            updatedlineitemDF.createOrReplaceTempView("lineitem")
            val result = sparkSession.sql("""select
                                                l_returnflag,
                                                l_linestatus,
                                                sum(l_quantity) as sum_qty,
                                                sum(l_extendedprice) as sum_base_price,
                                                sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
                                                sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
                                                avg(l_quantity) as avg_qty,
                                                avg(l_extendedprice) as avg_price,
                                                avg(l_discount) as avg_disc,
                                                count(*) as count_order
                                            from lineitem
                                            where
                                                l_shipdate = '"""+date+""""'
                                            group by l_returnflag, l_linestatus""")
                                     .show(200, false)

        }
        
    }
} 