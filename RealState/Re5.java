package real;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Re5 {

	/**
	 * 5.separate list of residential apartments with more than 2 beds. 
	 * Also include columns in following order City,
	 * Baths,Sq_feet,Price,flat_type,Beds respectively.
	 */
	public static class MyMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			int bd=Integer.parseInt(arr[4]);
			
			
			if(arr[7].toString().equals("Residential")  && bd>2 )
			
				
			context.write(new Text(arr[2]+","+arr[5]+","+arr[6]+","+arr[9]+","+arr[7]+","+arr[4]), NullWritable.get());
		
		
		
	}}
		
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
			Configuration obj = new Configuration();
			Job job = Job.getInstance(obj, "pppp rr11.....");
			job.setJarByClass(Re5.class);
			job.setMapperClass(MyMapper.class);
		//job.setReducerClass(MyReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			FileSystem.get(obj).delete(new Path(args[1]), true);
			System.exit(job.waitForCompletion(true) ? 0 : 1);

		

	}



}
