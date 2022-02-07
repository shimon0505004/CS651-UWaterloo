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
import org.apache.hadoop.io.FloatWritable; 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.map.HMapStIW;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.HashSet;
import java.util.Set;

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
public class StripesPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);

    // Mapper: emits (token, 1) for every unique word occurrence in every word.
    public static final class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
      // Reuse objects to save overhead of object creation.
      private static final IntWritable ONE = new IntWritable(1);
      private static final Text WORD = new Text();
      private static final Set<String> wordSet = new HashSet();
  
      @Override
      public void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException {

        wordSet.clear();

        WORD.set("*");                  //For Counting lines
        context.write(WORD, ONE);

        for (String word : Tokenizer.tokenize(value.toString())) {
          if(!wordSet.containsKey(word)){
            wordSet.add(word);
            WORD.set(word);
            context.write(WORD, ONE);
          }
        }
      }
    }
  
  
    // Reducer: sums up all the counts.
    public static final class FirstCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
      // Reuse objects.
      private static final IntWritable SUM = new IntWritable();
  
      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
          throws IOException, InterruptedException {
        // Sum up values.
        Iterator<IntWritable> iter = values.iterator();
        int sum = 0;
        while (iter.hasNext()) {
          sum += iter.next().get();
        }
        SUM.set(sum);
        context.write(key, SUM);
      }
    }

    
    // Reducer: Calculating marginal of P(y).
    public static final class FirstReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
      // Reuse objects.
      private static final FloatWritable P_y = new FloatWritable(0.0f);
      private int lineCount = 0;
      private int threshold = 1;
  
      @Override
      public void setup(Context context) {
        threshold = context.getConfiguration().getInt("threshold", 1);
      }

      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
          throws IOException, InterruptedException {
        // Sum up values.
        Iterator<IntWritable> iter = values.iterator();
        int sum = 0;
        while (iter.hasNext()) {
          sum += iter.next().get();
        }

        if(text.toString().equals("*")){
          lineCount = sum;
        }else{
          if(sum >= threshold){
            P_y.set((sum*1.0f)/(lineCount));
            context.write(key, P_y);
          }
        }
      }
    }


  private static final class SecondMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final HMapStIW MAP = new HMapStIW();
    private static final Text KEY = new Text();
    

    @Override
    public void setup(Context context) {

    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(value.toString());

      for (int i = 0; i < Math.min(tokens.size(), 40); i++) {
        MAP.clear();
        MAP.increment(tokens.get(i));                 //Used for keeping count of individual words A as (A,A)  
        for (int j = 0; j < Math.min(tokens.size(), 40); j++) {
          if (i == j) continue;
          
          if((tokens.get(i).compareTo(tokens.get(j))) == 0) continue;   // Skip cases like (A,A)
          
          if(!MAP.containsKey(tokens.get(j)))         //Take pairs like (A,B) only once. Say line is A, B, C, B. This will ensure if (B,1) is already in, then no more entries are made.
            MAP.increment(tokens.get(j));
        }

        KEY.set(tokens.get(i));
        context.write(KEY, MAP);
      }
    }
  }

  private static final class SecondCombiner extends Reducer<Text, HMapStIW, Text, HMapStIW> {
    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW map = new HMapStIW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      context.write(key, map);
    }
  }


  private static final class SecondReducer extends Reducer<Text, HMapStIW, Text, HashMapWritable> {

    private static final PairOfFloatInt CO_OCCURANCE_PAIR_PMI_AND_COUNT = new PairOfFloatInt();
    private static final HashMapWritable<String, PairOfFloatInt> OUTPUT_MAP_VALUE = new HashMapWritable();
    private Map<String, float> p_yMapper = new HashMap<>();
    private int threshold = 1;

    @Override
    public void setup(Context context) {
      threshold = context.getConfiguration().getInt("threshold", 1);
      String sidedata_dir = context.getConfiguration().get("sidedata_dir");

      FileSystem fs = FileSystem.get(context.getConfiguration());
      PathFilter filter = new PathFilter() {

        @Override
        public boolean accept(Path path) {
            return path.getName().startsWith("part-r-");
        }
      };

      FileStatus[] fileStatuses = fs.listStatus(sidedata_dir, filter);
      for(FileStatus file: fileStatuses){
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file)));
        String line = null;
        while((line = reader.readLine()) != null) {
          String[] words = Tokenizer.tokenize(line);
          if(words.length == 2){
            String yKey = words[0];
            float p_y = Float.parseFloat(words[1]);
            p_yMapper.put(yKey, p_y);
          }
        }
        reader.close();
      }
    }

    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW map = new HMapStIW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      float c_X = map.get(key.toString());
      OUTPUT_MAP_VALUE.clear();
      for(String yKey: map.keySet()){
        if(yKey.compareTo(key.toString())!=0){

          int c_X_Y = map.get(yKey);

          if(c_X_Y >= threshold){
            float p_Y_bar_X = ((c_X_Y * 1.0)/c_X);
            float p_y = p_yMapper.get(yKey);
            float pmi_x_y = (float)(java.lang.Math.log10(p_Y_bar_X / p_y));
            CO_OCCURANCE_PAIR_PMI_AND_COUNT.set(pmi_x_y, c_X_Y);
            OUTPUT_MAP_VALUE.put(yKey, CO_OCCURANCE_PAIR_PMI_AND_COUNT);  
          }

        }
      }

      context.write(key, OUTPUT_MAP_VALUE);
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private StripesPMI() {}

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
  public int run(String[] argv) throws Exception {
    Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + ComputeCooccurrenceMatrixPairs.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - threshold: " + args.threshold);
    LOG.info(" - number of reducers: " + args.numReducers);

    Job job1 = Job.getInstance(getConf());
    job1.setJobName(StripesPMI.class.getSimpleName() + " Job1 : Calculate p(y) = c(y)/number of lines.");
    job1.setJarByClass(StripesPMI.class);

    // Delete the Intermediate output directory if it exists already.
    String tempOutput = args.output + "_temp";
    Path tempOutputDir = new Path(tempOutput);
    FileSystem.get(getConf()).delete(tempOutputDir, true);
    

    job1.getConfiguration().setInt("threshold", args.window);
    job1.getConfiguration().setInt("sidedata_dir", tempOutput);

    job1.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job1, new Path(args.input));
    FileOutputFormat.setOutputPath(job1, new Path(tempOutputDir));


    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(HMapStIW.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(HMapStIW.class);

    job1.setMapperClass(FirstMapper.class);
    job1.setCombinerClass(FirstCombiner.class);
    job1.setReducerClass(FirstReducer.class);

    job1.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job1.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job1.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job1.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job1.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    long startTime = System.currentTimeMillis();
    boolean successAtJob1 = job1.waitForCompletion(true);
    System.out.println("Job2 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    if(successAtJob1){
      Job job2 = Job.getInstance(getConf());
      job2.setJobName(StripesPMI.class.getSimpleName() + " Job2 : Calculate PMI(x,y) = p(y|x)/p(y)");
      job2.setJarByClass(StripesPMI.class);

      // Delete the output directory if it exists already.
      Path outputDir = new Path(args.output);
      FileSystem.get(getConf()).delete(outputDir, true);

      job2.getConfiguration().setInt("threshold", args.threshold);
      job2.setNumReduceTasks(args.numReducers);

      FileInputFormat.setInputPaths(job2, new Path(args.input));
      FileOutputFormat.setOutputPath(job2, new Path(args.output));
  
      job2.setMapOutputKeyClass(Text.class);
      job2.setMapOutputValueClass(IntWritable.class);
      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(HashMapWritable.class);
      job2.setOutputFormatClass(TextOutputFormat.class);

      job2.setMapperClass(SecondMapper.class);
      job2.setCombinerClass(SecondCombiner.class);
      job2.setReducerClass(SecondReducer.class);

      job2.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
      job2.getConfiguration().set("mapreduce.map.memory.mb", "3072");
      job2.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
      job2.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
      job2.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

      long startTime = System.currentTimeMillis();
      boolean successAtJob2 = job2.waitForCompletion(true);
      System.out.println("Job2 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

      if(successAtJob2){
        //Delete temporary directory once job 2 is successfully completed.
        FileSystem.get(getConf()).delete(tempOutputDir, true);
      }


    }

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI(), args);
  }
}
