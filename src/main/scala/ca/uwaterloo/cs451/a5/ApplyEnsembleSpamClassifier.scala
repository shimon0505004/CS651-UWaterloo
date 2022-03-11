


package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import math._

class ApplyEnsembleSpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, output, model, method)
    val input = opt[String]("input", descr = "input path", required = true)
    val output = opt[String]("output", descr = "output path", required = true)
    val model = opt[String]("model", descr = "model", required = true)
    val method = opt[String]("method", descr = "method : average/vote", required = true)
    verify()
}