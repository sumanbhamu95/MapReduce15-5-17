package ibm;

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

public class Pr3 {

	/**
	 * 3.Find out the list of employee whose income is more 
	 * than the average income of all the employees 
	 * present in the same department?s
	 */
	public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			
			
			context.write(new Text(arr[4]), new Text(arr[9]+","+arr[18]));//dept,emplo no.   and income
		}
	}
		
	
	public static class MyReducer extends Reducer<Text,Text,Text,Text>{
			public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
		
		int c=0,sum=0;
		float avgde=0.0f;
		String sumu="";
		int inc=0;
		for(Text num:value){
			String str[]=num.toString().split(",");
			 inc=Integer.parseInt(str[1]);
		sum=sum+inc;
			sumu=str[0];
			c++;
		}
		avgde=sum/c;
		int abc = 0;
		if(inc>avgde)
			abc=inc;
		
			context.write(new Text(key), new Text(String.valueOf(abc)));
		
		
	}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
			Configuration obj = new Configuration();
			Job job = Job.getInstance(obj, "pppp rr11.....");
			job.setJarByClass(Pr3.class);
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
