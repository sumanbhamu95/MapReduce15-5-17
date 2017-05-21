package m1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ListYr {

	/**
	 * Find the list of years and number of movies released each year
	 */
	public static class MyMapper extends Mapper<LongWritable,Text,IntWritable,IntWritable>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			
			int i=1;
			String arr[]=value.toString().split(",");
			context.write(new IntWritable(Integer.parseInt(arr[2])),new IntWritable(i) );
		}
	}
	public static class MyReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
		public void reduce(IntWritable key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException{
			
			int c=0;
			for(IntWritable x:value){
				c=c+x.get();
				
			}
			context.write(key, new IntWritable(c));
			
			
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration c=new Configuration();
		Job job=Job.getInstance(c,"listY");
		job.setJarByClass(ListYr.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		 FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        System.exit(job.waitForCompletion(true) ? 0 : 1);
		

	}

}
