package fr.eurecom.dsg.mapreduce;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Word Count example of MapReduce job. Given a plain text in input, this job
 * counts how many occurrences of each word there are in that text and writes
 * the result on HDFS.
 * 
 */
public class WordCountIMC extends Configured implements Tool {

    private int numReducers;
    private Path inputPath;
    private Path outputDir;

    @Override
    public int run(String[] args) throws Exception {

        //Job job = null; // TODO: define new job instead of null using conf e setting

        // TODO: set job input format
        // TODO: set map class and the map output key and value classes
        // TODO: set reduce class and the reduce output key and value classes
        // TODO: set job output format
        // TODO: add the input file as job input (from HDFS)
        // TODO: set the output path for the job results (to HDFS)
        // TODO: set the number of reducers. This is optional and by default is 1
        // TODO: set the jar class

        Configuration conf = this.getConf();

        Job job = new Job(conf, "Word Count");

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(WCIMCMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(WCIMCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setNumReduceTasks(Integer.parseInt(args[0]));

        job.setJarByClass(WordCount.class);

        job.waitForCompletion(true);

        return 0;
        //return job.waitForCompletion(true) ? 0 : 1; // this will execute the job
    }

    public WordCountIMC(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: WordCountIMC <num_reducers> <input_path> <output_path>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCountIMC(args), args);
        System.exit(res);
    }


    class WCIMCMapper extends Mapper<LongWritable, Text, Text, IntWritable> { // TODO: change Object to output value type

        //    private IntWritable ONE = new IntWritable(1);
        private Text textValue = new Text();

        HashMap<String, Integer> map = new HashMap<String, Integer>();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] words = line.split("\\s+"); //split string to tokens

            int length = words.length;

            for (String word : words) {
                if (map.containsKey(word)) {
                    int new_value = map.get(word);
                    new_value++;
                    map.put(word, new_value);
                } else
                    map.put(word, 1);
            }// in-memory combine

            for (String word : map.keySet()) {
                textValue.set(word);
                context.write(textValue, new IntWritable(map.get(word)));
            }
        }
        //*TODO: implement the map method (use context.write to emit results). Use
        // the in-memory combiner technique
    }

    class WCIMCReducer extends Reducer<Text, // TODO: change Object to input key
            IntWritable, // TODO: change Object to input value type
            Text, // TODO: change Object to output key type
            IntWritable> { // TODO: change Object to output value type

        @Override
        protected void reduce(Text word, // TODO: change Object to input key type
                              Iterable<IntWritable> values, // TODO: change Object to input value type
                              Context context) throws IOException, InterruptedException {

            int accumulator = 0;
            for (IntWritable value : values) { //comupte the sum
                accumulator += value.get();
            }
            context.write(word, new IntWritable(accumulator));
        }
    }
}
