package edu.wpi.project1;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query5 {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{

    private Text word = new Text();
    private Text information = new Text();

    static  HashMap<String, String> customerId = new HashMap<String, String>();

    static HashMap<Integer, String> agemap = new HashMap<Integer, String>();

    public void setup(Context context) throws IOException {

      agemap.put(1, "[10,20)");
      agemap.put(2, "[20,30)");
      agemap.put(3, "[30,40)");
      agemap.put(4, "[40,50)");
      agemap.put(5, "[50,60)");
      agemap.put(6, "[60,70]");


      Configuration conf = context.getConfiguration();
      Path path = new Path("data/Customers.txt");
      FileSystem fs = FileSystem.get(path.toUri(), conf);
      BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
      try {
        String line;
        line=br.readLine();
        while (line != null){
          String[] arrOfStr = line.split(",");

          // Customer ID : Age Range, Gender
          customerId.put( String.valueOf(arrOfStr[0]), agemap.get((Integer.valueOf(arrOfStr[2]) + 1) / 10 )  +","+ arrOfStr[3]);

          line = br.readLine();
        }
      } finally {
        br.close();
      }

    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      String[] arrOfStr = value.toString().split(",");
      information.set("1," + String.valueOf(arrOfStr[2]));
      word.set( customerId.get(String.valueOf(arrOfStr[1])) );
      context.write(word, information);

    }
  }

  public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {

    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      float count = 0;
      float sum = 0;
      int min = Integer.MAX_VALUE;
      int max = Integer.MIN_VALUE;

      for (Text val : values) {
        String[] field = val.toString().split(",");
        count += Float.valueOf(field[0]);
        sum += Float.valueOf(field[1])*Float.valueOf(field[0]);

        if (Integer.valueOf(field[1]) < min) {
          min = Integer.valueOf(field[1]);
        }
        if (Integer.valueOf(field[1]) > max) {
          max = Integer.valueOf(field[1]);
        }
      }

      String o = min +","+ max +","+ sum/count;
      result.set(o);
      context.write(key, result);
    }
  }

/*
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();


    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(Query5.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumCombiner.class);
    //job.setNumReduceTasks(1);
    job.setReducerClass(IntSumReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  */
}
