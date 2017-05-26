package petrol;

import java.io.IOException;
import java.util.TreeMap;

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

public class TopTenVolumeSold {

	/**
	 * 2)Which are the top 10 distributors ID s
	 *  for selling petrol and also display the amount
	 *   of petrol sold in volume by them individually?

	 */
	
	public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			
			context.write(new Text("top 10"),new Text(arr[3]+","+arr[5]+","+arr[0]));
		}
	}
public static class MyReducer extends Reducer<Text,Text,Text,Text>{
	String ss;
		TreeMap tm=new TreeMap();
		public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
			
			
			
			for(Text num:value)
			{
				
				
				String str[]=num.toString().split(",");
				
				float sp=Float.parseFloat(str[0]);
				int vol=Integer.parseInt(str[1]);
				
				String id=str[2];
				
				ss=key.toString();
				String sd=id+","+vol;
				tm.put(sp, sd);
				
				if(tm.size()>10){
					tm.remove(tm.firstKey());
				}
			}
			
			
			
		}
		public void cleanup(Context context) throws IOException, InterruptedException{
			context.write(new Text("top..10.."), new Text(tm.toString()));
		}
	}


	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration c=new Configuration();
		Job job=Job.getInstance(c,"sss");
		job.setJarByClass(TopTenVolumeSold.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		//job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileSystem.get(c).delete(new Path(args[1]),true);
		FileInputFormat.addInputPath(job, new Path(args[0]));	
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);


	}

}
