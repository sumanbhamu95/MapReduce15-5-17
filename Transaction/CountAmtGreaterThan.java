package transTask;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountAmtGreaterThan {

	/**
	 * 
	Count all the transaction where amt>160
	 */
	public static class MyMapper extends Mapper<LongWritable,Text,Text,FloatWritable>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			
			String a="count is";
			//arr[0]+" , "+arr[4];
			
			context.write(new Text(a), new FloatWritable(Float.parseFloat(arr[3])));
			
		}
	}
	public static class MyReducer extends Reducer<Text,FloatWritable,Text,IntWritable>{
		public void reduce(Text key,Iterable<FloatWritable> value,Context context) throws IOException, InterruptedException{
			
			int c=0;
			for(FloatWritable x:value){
				if(x.get()>160){
					c++;
				}
			}context.write(key, new IntWritable(c));
			
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration c=new Configuration();
		Job job=Job.getInstance(c,"Count Transac where amt >160");
		job.setJarByClass(CountAmtGreaterThan.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileSystem.get(c).delete(new Path(args[1]),true);//to delete automatically
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);

	}

}
