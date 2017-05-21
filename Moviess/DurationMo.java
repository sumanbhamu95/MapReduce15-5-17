package m1;

import java.io.IOException;



import m1.DurationMo.MyMapper.MyReducer;

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

public class DurationMo {

	/**
	 *Find the number of movies with duration more than 1.5 hours
	 */
	public static class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			context.write(new Text(arr[1]),new IntWritable(Integer.parseInt(arr[4])) );
		}
		public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
			public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException{
				
				int s=0;
				for(IntWritable d:value){
					s=d.get()/60;
					if(s>90){
						context.write(key, new IntWritable(s));//in min
					}
				}
				
			}
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration c=new Configuration();
		Job job=Job.getInstance(c,"duratiion");
		job.setJarByClass(DurationMo.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		 FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
		

	}

}
