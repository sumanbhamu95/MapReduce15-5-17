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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class BetwTwoYr {

//Find the number of movies released between 1945 and 1959-
	
	
public static class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String a=null;
			
			String arr[]=value.toString().split(",");
			
			context.write(new Text(arr[1]), new IntWritable(Integer.parseInt(arr[2])));
			
		}
	}
	
	//reducer --hs only one mthd -reduce
	public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		
		public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException{
		
			for(IntWritable v:value){
				int su=0;
				if((v.get()>=1971)){
					if(v.get()<=1993){
						su=v.get();
					context.write(key, new IntWritable(su));
				}
				}
			}
			
			
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration c=new Configuration(); // like web.xml
		Job job=Job.getInstance(c,"wordcount");//c-reference of configuration and wordcount-userdefined var
		job.setJarByClass(BetwTwoYr.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		//job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


}
