package m1;

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

public class BadMovie {

	/**
	 * list Bad movie
	 */
	public static class MyMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			context.write( new IntWritable(0),new Text(arr[1]));
		}
	}
	public static class MyReducer extends Reducer<IntWritable,Text,IntWritable,Text>{
		public void reduce(IntWritable key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
			String[] bd={"The","Big"};
			int c=1;
			for(Text s:value){
				
			for(String a:bd){
				if(s.toString().contains(a)){
				//	c++;
					context.write( new IntWritable(c),new Text(s));
			}
				
			}
			
			}
			/*if(c>0){
			context.write( new IntWritable(c),(Text) value);
		}*/
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration c=new Configuration();
		Job job=Job.getInstance(c,"badMov");
		job.setJarByClass(BadMovie.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileSystem.get(c).delete(new Path(args[1]),true);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);

	}

}
