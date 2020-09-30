package edu.wpi.project1;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

import edu.wpi.project1.*;
import edu.wpi.project1.Query5.*;
import edu.wpi.project1.Query3.*;

/**
 * Project 1
 *
 */
public class App {
    public static void main( String[] args ) throws Exception
    {
        // properties set so that pig scripts can be run
        Properties props = new Properties();
        props.setProperty("fs.default.name", "hdfs://localhost:9000");
        //props.setProperty("fs.default.name", "hdfs://<namenode-hostname>:<port>");
        //props.setProperty("mapred.job.tracker", "<jobtracker-hostname>:<port>");
        org.apache.log4j.BasicConfigurator.configure(); // log output
        // RUN QUERIES
        String customerPath = "data/Customers.txt";
        String transactionPath = "data/Transactions.txt";
        if(args.length==0) return;
        switch(args[0]){
            case "2": mr_query2(customerPath,transactionPath,"data/result/query2");
            case "3": mr_query3(transactionPath,"data/result/query3");
            case "4": mr_query4(customerPath,transactionPath,"data/result/query4");
            case "5": mr_query5(transactionPath,"data/result/query5");
            default: System.out.println("Arguements not valid. Please pass a number between 2 and 5");
        }
    }

    public static void deleteResults(String result_path){
            File resultsFolder = new File(result_path);
            resultsFolder.delete();
    }
    /**
        MAP REDUCE JOBS
    */

    /** 3.1
     * Joins the customer and transaction tables and finds the total tranaction amount for each customer. Results are formatted as customer_id,customer_name,total_transaction_amount.
     * @author Pascal Bakker
     * @param customer_csv_path Customer.txt file location
     * @param transcation_csv_path Transaction.txt file location
     * @param output_string_path  Hadoop results output folder
     * @throws Exception
     */
    public static void mr_query2(String customer_csv_path, String transcation_csv_path,String output_string_path) throws Exception {
        deleteResults(output_string_path);
        Configuration conf = new Configuration();
        // Setup job
        Job job = Job.getInstance(conf, "Query 2: Find customer transcation totals");
        job.setJarByClass(Query2.class);
        // Set custom functions
        job.setMapperClass(Query2.CustomMapper.class);
        job.setCombinerClass(Query2.CustomCombiner.class);
        job.setReducerClass(Query2.CustomReducer.class);
        // set input/output data types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // set input file
        FileInputFormat.addInputPath(job, new Path("data/Transactions.txt"));
        job.addCacheFile(new Path("data/Customers.txt").toUri());
        // write outputs
        Path outputPath = new Path(output_string_path);
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /** 3.2
    * @author Mario Marduz
    * @throws Exception
    */
public static void mr_query3(String transcation_csv_path, String output_string_path) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Query3.class);
        job.setMapperClass(Query3.TokenizerMapper.class);
        job.setCombinerClass(Query3.IntSumReducer.class);
        //job.setNumReduceTasks(1);
        job.setReducerClass(Query3.IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(transcation_csv_path));
        FileOutputFormat.setOutputPath(job, new Path(output_string_path));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /** 3.3
     * Find the total amount of customers per country, and the minimum and maximum amount spent by a customer in a country
     * @author Pascal Bakker
     * @param customer_csv_path Customer.txt file location
     * @param transcation_csv_path Transaction.txt file location
     * @param output_string_path  Hadoop results output folder
     * @throws Exception
     */
public static void mr_query4(String customer_csv_path, String transcation_csv_path,String output_string_path) throws Exception {
        // delete old results
        deleteResults(output_string_path);
        Configuration conf = new Configuration();
        // Setup job
        Job job = Job.getInstance(conf, "Query 4: find country customer amount");
        job.setJarByClass(Query4.class);
        // Set custom functions
        job.setMapperClass(Query4.CustomMapper.class);
        //job.setCombinerClass(Query4.CustomCombiner.class);
        job.setReducerClass(Query4.CustomReducer.class);
        // set input/output data types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // set input file
        FileInputFormat.addInputPath(job, new Path(transcation_csv_path));
        job.addCacheFile(new Path(customer_csv_path).toUri());
        // write outputs
        Path outputPath = new Path(output_string_path);
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /** 3.4
    * @author Mario Marduz
    * @throws Exception
    */
    public static void mr_query5(String transcation_csv_path, String output_string_path) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Query5.class);
        job.setMapperClass(Query5.TokenizerMapper.class);
        //job.setCombinerClass(IntSumCombiner.class);
        //job.setNumReduceTasks(1);
        job.setReducerClass(Query5.IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(transcation_csv_path));
        FileOutputFormat.setOutputPath(job, new Path(output_string_path));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
