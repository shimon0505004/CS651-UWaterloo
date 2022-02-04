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

package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfFloatInt;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.map.HMapKIW;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * <p>
 * Implementation of the "pairs" algorithm for computing co-occurrence matrices from a large text
 * collection. This algorithm is described in Chapter 3 of "Data-Intensive Text Processing with 
 * MapReduce" by Lin &amp; Dyer, as well as the following paper:
 * </p>
 *
 * <blockquote>Jimmy Lin. <b>Scalable Language Processing Algorithms for the Masses: A Case Study in
 * Computing Word Co-occurrence Matrices with MapReduce.</b> <i>Proceedings of the 2008 Conference
 * on Empirical Methods in Natural Language Processing (EMNLP 2008)</i>, pages 419-428.</blockquote>
 *
 * @author Jimmy Lin
 */
public class PairsPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);

  private static final class FirstMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
    private static final PairOfStrings PAIR = new PairOfStrings();
    private static final IntWritable ONE = new IntWritable(1);
    private static final HMapKIW<PairOfStrings> DUPLICATECHECKERSET = new HMapKIW<PairOfStrings>();

    //private int window = 2;

    @Override
    public void setup(Context context) {
      //window = context.getConfiguration().getInt("window", 2);
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(value.toString());
      DUPLICATECHECKERSET.clear();

      //Used for counting number of lines
      PAIR.set("*", "*");
      context.write(PAIR, ONE);                                           //So, (*,*) will contain the total number of lines and will be available at the beginning of sorted order
      DUPLICATECHECKERSET.put(PAIR, ONE);     

      for (int i = 0; i < Math.min(tokens.size(), 40); i++) {

        PAIR.set(tokens.get(i), "*");  
        if(!DUPLICATECHECKERSET.containsKey(PAIRS)){
          context.write(PAIR, ONE);                                      
          DUPLICATECHECKERSET.put(PAIR, ONE);                         // When Line is like A B C A B C, Take co-occuring pair (A,B) only once, which will indicate line containing event A  
        }
        
        PAIR.set("*", tokens.get(i));  
        if(!DUPLICATECHECKERSET.containsKey(PAIRS)){
          context.write(PAIR, ONE);                                      
          DUPLICATECHECKERSET.put(PAIR, ONE);                         // When Line is like A B C A B C, Take co-occuring pair (A,B) only once, which will indicate line containing event A  
        }  

        for(int j = i+1; j < Math.min(tokens.size(), 40); j++)  {           // Ensure only the first 40 words in each line

          PAIR.set(tokens.get(j), "*");  
          if(!DUPLICATECHECKERSET.containsKey(PAIRS)){
            context.write(PAIR, ONE);                                   // When Line is like A B C A B C, Take co-occuring pair (A,B) only once, which will indicate line containing event B     
            DUPLICATECHECKERSET.put(PAIR, ONE);
          }

          PAIR.set("*", tokens.get(j));  
          if(!DUPLICATECHECKERSET.containsKey(PAIRS)){
            context.write(PAIR, ONE);                                      
            DUPLICATECHECKERSET.put(PAIR, ONE);                         // When Line is like A B C A B C, Take co-occuring pair (A,B) only once, which will indicate line containing event A  
          }  

          if (tokens.get(i).compareTo(tokens.get(j) == 0) continue;       // When Line is like A B C A B C, avoid counting co-occuring pairs like (A A)

          PAIR.set(tokens.get(i), tokens.get(j));
          if(!DUPLICATECHECKERSET.containsKey(PAIRS)){
            context.write(PAIR, ONE);                                    // When Line is like A B C A B C, Take co-occuring pair (A,B) only once  
            DUPLICATECHECKERSET.put(PAIR, ONE);
          }

          PAIR.set(tokens.get(j), tokens.get(i));
          if(!DUPLICATECHECKERSET.containsKey(PAIRS)){
            context.write(PAIR, ONE);                                    // When Line is like A B C A B C, Take co-occuring pair (B,A) only once  
            DUPLICATECHECKERSET.put(PAIR, ONE);
          }
        }
      }

      DUPLICATECHECKERSET.clear();

    }
  }

  private static final class FirstCombiner extends
      Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  private static final class FirstReducer extends
      Reducer<PairOfStrings, IntWritable, PairOfStrings, PairOfInts> {
    private static final PairOfInts RESULT = new PairOfInts();
    private static final PairOfStrings PAIR = new PairOfStrings();
    private int denominator = 0;
    private int numerator = 0;

    private int threshold = 1;

    @Override
    public void setup(Context context) {
      threshold = context.getConfiguration().getInt("threshold", 1);
    }

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      if (key.getRightElement().equals("*")) {
        denominator = sum;
      } else {
        if(sum > threshold){
          // For PMI(x,y), if count of x/y is less than threshold, then count of (x,y) will definitely be less than threshold.
          numerator = sum;
          RESULT.set(numerator, denominator);
          PAIR.set(key.getRightElement(), key.getLeftElement())
          context.write(PAIR, RESULT);
        }
      }
    }
  }

  private static final class FirstPartitioner extends Partitioner<PairOfStrings, IntWritable> {
    @Override
    public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }



  /**
   * Creates an instance of this tool.
   */
  private PairsPMI() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-threshold", metaVar = "[num]", usage = "threshold of co-occurrence")
    int threshold = 1;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - threshold : " + args.threshold);
    LOG.info(" - number of reducers: " + args.numReducers);

    Job job1 = Job.getInstance(getConf());
    job1.setJobName("Job1 - Count Lines, Words and Co-Occurance Pairs");
    job1.setJarByClass(PairsPMI.class);

    
    // Delete the Intermediate output directory if it exists already.
    String tempOutput = args.output + "_temp";
    Path tempOutputDir = new Path(tempOutput);
    FileSystem.get(getConf()).delete(tempOutputDir, true);

    job1.getConfiguration().setInt("threshold", args.threshold);

    job1.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job1, new Path(args.input));
    FileOutputFormat.setOutputPath(job1, new Path(tempOutput));

    job1.setMapOutputKeyClass(PairOfStrings.class);
    job1.setMapOutputValueClass(IntWritable.class);
    job1.setOutputKeyClass(PairOfStrings.class);
    job1.setOutputValueClass(PairOfInts.class);

    job1.setMapperClass(FirstMapper.class);
    job1.setCombinerClass(FirstCombiner.class);
    job1.setReducerClass(FirstReducer.class);
    job1.setPartitionerClass(FirstPartitioner.class);

    long startTime = System.currentTimeMillis();
    job1.waitForCompletion(true);
    System.out.println("Job1 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PairsPMI(), args);
  }
}