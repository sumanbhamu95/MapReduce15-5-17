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

public class TopMovie {

	//Count the total number of movies in the list
	
	
	public static class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			
			int c=1;
			String arr[]=value.toString().split(",");
			String s="totla no. of movies in the list";
			/*for(String y:arr){
				c++;
			}*/
			//arr.length;
			/*if(Integer.parseInt(arr[0])!=0){
				c++;
			}*/
			
			context.write(new Text(s), new IntWritable(c));
			
		}
		
	}
	public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		
		public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException{
			int cou=0;
			for(IntWritable i:value){
				
				cou++;
				
			}
			context.write(key, new IntWritable(cou));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration c=new Configuration();
		Job job=Job.getInstance(c,"TotalMovie");
		job.setJarByClass(TopMovie.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
	//	job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
