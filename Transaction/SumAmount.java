package transx;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SumAmount {

	/**
	 * Sum of amount
	 */
	public static class MyMapper extends Mapper<LongWritable,Text,Text,FloatWritable>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			String a="the sum of amount";
			context.write(new Text(a), new FloatWritable(Float.parseFloat(arr[3])));
		}
	}
	public static class MyReducer extends Reducer<Text,FloatWritable,Text,FloatWritable>{
		public void reduce(Text key,Iterable<FloatWritable> value,Context context) throws IOException, InterruptedException{
			float sum=0;
			for(FloatWritable i:value){
				sum=sum+i.get();
			}
			context.write(key, new FloatWritable(sum));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration c=new Configuration();
		Job job=Job.getInstance(c,"sumAmt");
		job.setJarByClass(SumAmount.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileSystem.get(c).delete(new Path(args[1]),true);//to delete automatically
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);

	}

}
