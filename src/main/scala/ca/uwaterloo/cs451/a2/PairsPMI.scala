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

class PairsPMIConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String]("input", descr = "input path", required = true)
  val output = opt[String]("output", descr = "output path", required = true)
  val reducers = opt[Int]("reducers", descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int]("threshold", descr = "threshold", required = false, default = Some(1))
  verify()
}

object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new PairsPMIConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("Compute Pairs PMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    /*
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
      
    counts.saveAsTextFile(args.output())
    */

    var numberOfLines = 0
    val uniqueWordCounts = textFile
      .flatMap(line => {
        numberOfLines +=1
        var uniquetokens: Set[String] = Set()
        tokenize(line).foreach(uniquetokens += _)

        if (uniquetokens.size > 1) uniquetokens.toList else List()
      })
      .map(token => (token, 1))
      .reduceByKey(_ + _, args.reducers())

    val wordCountMap : scala.collection.mutable.HashMap[String,Int] = scala.collection.mutable.HashMap()
    uniqueWordCounts.foreach(wordCountMap += (_1 -> _2) )

    val uniquePairCounts = textFile
      .flatMap(line =>{
        var uniquetokens: Set[String] = Set()
        tokenize(line).foreach(uniquetokens += _)

        if (uniquetokens.size > 1) uniquetokens.toList.combinations(2).toList.flatMap(p => p.permutations.toList).map(l => (l.head, l.last)).toList else List()
      })
      .map(token => (token, 1))
      .reduceByKey(_ + _, args.reducers())
      .map(p =>{
        val key = p._1
        val c_x_y = p._2
        val c_x = wordCountMap(key._1) * 1.0f
        val c_y = wordCountMap(key._2) * 1.0f
        val pmi = (c_x_y * 1.0f * numberOfLines / (c_x * c_y))
        (key, (pmi, c_x_y))
      })

    uniquePairCounts.saveAsTextFile(args.output())

  }
}
