package petrol;

import java.io.IOException;
import java.util.TreeMap;

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

public class SoldPetrolInLeastAmount {

	/**
	 * 3)Find real life 10 distributor name who sold petrol in the least amount.

	 */
	public static class MyMapper extends Mapper<LongWritable,Text,Text,FloatWritable>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			
			//String sd[]=arr[3].split("$");
			context.write(new Text(arr[1]),new FloatWritable(Float.parseFloat(arr[3])) );
		}
	}
public static class MyReducer extends Reducer<Text,FloatWritable,Text,Text>{
	String ss;
		TreeMap<Float,String> tm=new TreeMap<Float,String>();
		public void reduce(Text key,Iterable<FloatWritable> value,Context context) throws IOException, InterruptedException{
			
			
			
			for(FloatWritable num:value)
			{
				
				tm.put(num.get(),key.toString());
				
				if(tm.size()>2){
					tm.remove(tm.lastKey());
				}
			}
			
			
		}
		public void cleanup(Context context) throws IOException, InterruptedException{
			context.write(new Text("min..10.."), new Text(tm.toString()));
		}
	}


	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration c=new Configuration();
		Job job=Job.getInstance(c,"distributers who sold petrol at least amt");
		job.setJarByClass(SoldPetrolInLeastAmount.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		//job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileSystem.get(c).delete(new Path(args[1]),true);
		FileInputFormat.addInputPath(job, new Path(args[0]));	
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);


	}



}
