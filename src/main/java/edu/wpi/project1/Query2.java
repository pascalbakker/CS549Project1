package edu.wpi.project1;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Query2{

	// Customer id,name,age,gender,country,salary
	// Transactions transid, customer_id, total, num_items, desc

	// Mapper for customers
	public static class CustomerMapper extends Mapper<Object, Text, Text, Text>
	{
		  // Parse customer input
		  // Get Customer_id, Name
	      public void map(Object key,
							Text value,
							Context context) throws IOException, InterruptedException {
	        String[] data = value.toString().split(",");
			context.write(new Text("cust "+data[0]),new Text(data[1]));
	      }

	}

	// Mapper for transactions
	public static class TransactionMapper extends Mapper<Object, Text, Text, Text>
		{
			  // Parse transaction input
			  // Get customer_id, total
			  public void map(Object key,
								Text value,
								Context context) throws IOException, InterruptedException {
				String[] data = value.toString().split(",");
				context.write(new Text(data[1]),new Text("tnxn " + data[2]));
			  }

		}

	// Reducer: Join customers and transactions
	public static class ReduceJoinReducer extends Reducer<Text,Text,Text,Text>{
		// values: { customer_id - [customer_name, amount1, amount2 ...] }
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String name = "";
			double total = 0.0;
			int count = 0;

			for(Text t: values){
				String parts[] = t.toString().split(" ");
				if(parts[0].equals("tnxn")){
					count++;
					total += Float.parseFloat(parts[1]);
				} else if(parts[0].equals("cust")){
						name = parts[1];
				}
			}
			String str = String.format("%d %f",count, total);
			System.out.println(str+" name "+name);
			context.write(new Text(name), new Text(str));
		}
	}
}
