


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

object ApplyEnsembleSpamClassifier {

    val log = Logger.getLogger(getClass().getName())

    def main(argv: Array[String]) {
        val args = new ApplyEnsembleSpamClassifierConf(argv)

        log.info("Input: " + args.input())
        log.info("Output: " + args.output())
        log.info("Previously Saved Model Path: " + args.model())
        log.info("method : average/vote: " + args.method())

        val conf = new SparkConf().setAppName("Apply Ensamble Spam Classifier")
        val sc = new SparkContext(conf)

        val outputPath = new Path(args.output())
        FileSystem.get(sc.hadoopConfiguration).delete(outputPath, true)

        val textFile = sc.textFile(args.input())

    }

}