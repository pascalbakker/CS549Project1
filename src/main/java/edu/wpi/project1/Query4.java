package edu.wpi.project1;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

// TODO fix names, clean up code
public class Query4{

	// Customer id,name,age,gender,country,salary
	// Transactions transid, customer_id, total, num_items, desc

	// Mapper for customers
	public static class CustomMapper extends Mapper<Object, Text, Text, Text>
	{
		// Key: customer id
		// value: custome country
		private static HashMap<Integer, String> customerHashpMap = new HashMap<>();

		// Retrieve customer data
		public void setup(Context context) throws IOException, InterruptedException {
			try{
					URI[] customerFile = context.getCacheFiles();
					if (customerFile != null && customerFile.length > 0) {
						for (URI file : customerFile){
							// Read Customers
							FileSystem f = FileSystem.get(context.getConfiguration());
							Path p = new Path(file.toString());
							BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(f.open(p)));
							String value;
							String[] customerData;
							while ((value = bufferedReader.readLine()) != null) {
								customerData = value.split(",");
								customerHashpMap.put(Integer.parseInt(customerData[0]), customerData[4]);
							}
						}
					}
			} catch (IOException ex){
						System.exit(1);
				}
		}


		  // input: Customer JOIN Transactions
		  // output: key customer_country values "customerID, transValue"
	      public void map(Object key,
							Text value,
							Context context) throws IOException, InterruptedException {
	        String[] transData = value.toString().split(",");
			String customerCountry = customerHashpMap.get(Integer.parseInt(transData[1]));
			Text mapkey = new Text(customerHashpMap.get(Integer.parseInt(transData[1]))); // customer id
			Text mapvalue = new Text(transData[1]+","+transData[2]);
			context.write(mapkey, mapvalue);
	      }

	}

	// input: key country, values "customerId, transValue"
	// output: key country, values "customerId,totalCount"
	public static class CustomCombiner extends Reducer <Text,Text,Text,Text>{
			public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				double sum = 0;
				String customerId = "";
				for (Text t: values){
					//System.out.println(t.toString());
					String[] data = t.toString().split(",");
					customerId = data[0];
					sum += Double.parseDouble(data[1]);
				}
				Text combinerValue = new Text(customerId + ","+sum);
				System.out.println("combiner "+combinerValue.toString());
				context.write(key, combinerValue);
			}
	}



	// Recives customers in country
	// input: key country_id values: ["customerId,totalSpending"]
	// output: key customer_id, values: ["numberOfCustomers, MinTransTotal, MaxTransTotal"]
	public static class CustomReducer extends Reducer<Text,Text,Text,Text>{
		// values: { customer_id - [customer_name, amount1, amount2 ...] }
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				double max = Double.MIN_VALUE;
				double min = Double.MAX_VALUE;
                int count = 0;
                for (Text t : values) {
					//System.out.println(val.toString());
                    String[] data = t.toString().split(",");
					// Find min and max. data[1]
					double customerTotalSpending = Double.parseDouble(data[1]);
					if(customerTotalSpending > max) max = customerTotalSpending;
					if(customerTotalSpending < min) min = customerTotalSpending;
					count++;
                }
                context.write(key, new Text(count + "," + min + "," + max));
		}
	}
}
