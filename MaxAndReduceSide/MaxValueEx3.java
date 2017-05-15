package maxandred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxValueEx3 {

	/**
	 * @param args
	 */
public static class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String a="max values";			
			String arr[]=value.toString().split(",");
		  context.write(new Text(a),new IntWritable(Integer.parseInt(arr[2])));
		}
	}
	public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		
		public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException{
			
			
			int max=0;
			for(IntWritable num:value)
			{
			  if(num.get()>max){
				  max=num.get();
			  }
			}
			
			context.write(key, new IntWritable(max));
			
		}
	}



	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration c=new Configuration();
		Job job=Job.getInstance(c,"sss");
		job.setJarByClass(MaxValueEx3.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		//job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileSystem.get(c).delete(new Path(args[1]),true);
		FileInputFormat.addInputPath(job, new Path(args[0]));	
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);

	}

}
