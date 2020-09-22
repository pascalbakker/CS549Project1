package edu.wpi.project1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.wpi.project1.Query2.ReduceJoinReducer;
/**
 * Project 1
 *
 */
public class App {
    public static void main( String[] args ) throws Exception
    {
        org.apache.log4j.BasicConfigurator.configure(); // log output
        mr_query2("data/Customers.txt","data/Transactions.txt","data/result");
    }

    // TODO Creating Transaction
    public static void createTransaction(){}

    // TODO Create Customer
    public static void createCustomer(){}


    /**
        MAP REDUCE JOBS
    */

    // TODO Query 2
    public static void mr_query2(String customer_csv_path, String transcation_csv_path,String output_string_path) throws Exception {
        System.out.println("Query 2: Find customer transcation totals");
        Configuration conf = new Configuration();
        // Setup job
        Job job = Job.getInstance(conf, "query2: find customer transaction total");
        job.setJarByClass(Query2.class);
        // Set custom functions
        job.setMapperClass(Query2.CustomerMapper.class);
        job.setCombinerClass(Query2.CustomerCombiner.class);
        job.setReducerClass(ReduceJoinReducer.class);
        // set input/output data types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // set input file
        FileInputFormat.addInputPath(job, new Path("data/Transactions.txt"));
        job.addCacheFile(new Path("data/Customers.txt").toUri());
        // write outputs
        Path outputPath = new Path(output_string_path);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // TODO Query 3
    public static void mr_query3(){}

    // TODO Query 4
    public static void mr_query4(){}

    // TODO Query 5
    public static void mr_query5(){}

    /**
        APACHE PIG JOBS
    */

    // TODO Query 2
    public static void pig_query2(){}

    // TODO Query 3
    public static void pig_query3(){}

    // TODO Query 4
    public static void pig_query4(){}

    // TODO Query 5
    public static void pig_query5(){}
}
