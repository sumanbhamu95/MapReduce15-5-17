package maxandred;

import java.io.IOException;
import java.util.TreeMap;

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

public class TreeMapEx {

	/**
	 * @param args
	 */
public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
		
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String a="max values";			
			String arr[]=value.toString().split(",");
		  context.write(new Text(a),new Text(arr[0]+","+arr[2]));
		}
	}
	public static class MyReducer extends Reducer<Text,Text,Text,Text>{
		
		TreeMap<Integer,String> tm=new TreeMap<Integer,String>();
		public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
			
			
			
			for(Text num:value)
			{
				String ss[]=num.toString().split(",");
				tm.put(Integer.parseInt(ss[1]), ss[0]);
				
				if(tm.size()>1){
					tm.remove(tm.firstKey());
				}
				}
			
			context.write(key, new Text(tm.toString()));
			
		}
	}


	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration c=new Configuration();
		Job job=Job.getInstance(c,"sss");
		job.setJarByClass(TreeMapEx.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		//job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileSystem.get(c).delete(new Path(args[1]),true);
		FileInputFormat.addInputPath(job, new Path(args[0]));	
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);


	}

}
