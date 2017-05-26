package real;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Re3 {

	/**
	 * 3.Which is the cheapest Condo in CA.
	 *  name the city,street and price for the Condo.
	 */
	public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			
			
			//if(arr[3].toString().equals("CA")){
			if(arr[7].toString().equals("Condo"))
			
				
			context.write(new Text(arr[7]), new Text(arr[9]+","+arr[0]+","+arr[1]));
		//}
		
		
	}}
	public static class MyReducer extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
			
			
			
			String sd="";
			
			int min=Integer.MAX_VALUE;
				
				
				for(Text num:value)
				{
					
					String arr[]=num.toString().split(",");
				
					int pr=Integer.parseInt(arr[0]);
					
					//min=Math.min(min, pr);
					if(pr<min){
						min=pr;
					}
					 sd=min+","+arr[2]+","+arr[1];
					
				}
				
				
				context.write(key, new Text(sd));
				
				
			}
			
		}


	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
			Configuration obj = new Configuration();
			Job job = Job.getInstance(obj, "pppp rr11.....");
			job.setJarByClass(Re3.class);
			job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			FileSystem.get(obj).delete(new Path(args[1]), true);
			System.exit(job.waitForCompletion(true) ? 0 : 1);

		

	}



}
