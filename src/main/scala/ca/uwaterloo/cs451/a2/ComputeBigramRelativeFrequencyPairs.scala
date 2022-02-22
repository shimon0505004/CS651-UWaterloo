/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class PairsConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers, textOutput)
  val input = opt[String]("input", descr = "input path", required = true)
  val output = opt[String]("output", descr = "output path", required = true)
  val reducers = opt[Int]("reducers", descr = "number of reducers", required = false, default = Some(1))
  val textOutput = opt[Boolean]("textOutput", descr = "use TextOutputFormat (otherwise, SequenceFileOutputFormat)", required = false, default = Some(true))
  val executors = opt[Int]("num-executors", descr = "number of executors", required = false, default = Some(1))
  val executorCores = opt[Int]("executor-cores", descr = "number of executor cores", required = false, default = Some(1))
  verify()
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new PairsConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Text output: " + args.textOutput())

    val conf = new SparkConf().setAppName("Compute Bigram Relative Frequency Pairs")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    var sum = 1
    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) tokens.sliding(2).map(p => p.mkString(",")).flatMap(p => List(p, p.split(",")(0)+",*")).toList else List()
      })
      .map(bigram => (bigram, 1))
      .reduceByKey(_ + _, args.reducers())
      .sortByKey()
      .map(count => {
        if(count._1.endsWith(",*")){
          sum = count._2
          ("("+count._1+")", ((count._2*1.0f)))
        }else{
          ("("+count._1+")", ((count._2*1.0f)/sum))
        } 
        })
      
    
    

    if(args.textOutput()){
      counts.saveAsTextFile(args.output())
    }else{
      counts.saveAsObjectFile(args.output())
    }

    	
  }
}
