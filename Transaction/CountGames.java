package transx;

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

public class CountGames {

	/**
	 * Count total games---played no of times---i.e display how many types of games
	 */
	public static class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			context.write(new Text(arr[4]), new IntWritable(1));
		}
	}
	public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	Text maxWord;
	int max=0;
		public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException{
			int c=0;
			for(IntWritable i:value){
				c=c+i.get();
			}
			if(c>max){
				max=c;
			}
			maxWord=key;
			context.write(maxWord, new IntWritable(max));
		}
		/* @Override
		  protected void cleanup(Context context) throws IOException, InterruptedException {
		      context.write(maxWord, new IntWritable(max));
		  }*/
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration c=new Configuration();
		Job job=Job.getInstance(c,"countGames");
		job.setJarByClass(CountGames.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileSystem.get(c).delete(new Path(args[1]),true);//to delete automatically
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);

	}

}
