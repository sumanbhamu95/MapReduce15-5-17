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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MonthlyTrans {

	/**
	 * Find Total trasaction amount for each month...
	 * Task 5 

Divide the file into 12 files, each file containing each month of data. 
For eg. file 1 
should contain data of january txn, file 2 should contain data of feb txn.
	 */
	public static class MyMapper extends Mapper<LongWritable,Text,Text,FloatWritable>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			
			//String as=arr[1].substring(0,2);
			context.write(new Text(arr[1]),new FloatWritable(Float.parseFloat(arr[3])));
		}
	}
	public static class MyPartitioner extends Partitioner<Text,FloatWritable>{

		@Override
		public int getPartition(Text key, FloatWritable value, int op) {
			String a=(key.toString().substring(0,2));
			
			if(a.toString().equals("01")){
				return 0;
			}
			if(a.toString().equals("02")){
				return 1;
			}
			if(a.toString().equals("03")){
				return 2;
			}
			if(a.toString().equals("04")){
				return 3;
			}
			if(a.toString().equals("05")){
				return 4;
			}
			if(a.toString().equals("06")){
				return 5;
			}
			if(a.toString().equals("07")){
				return 6;
			}
			if(a.toString().equals("08")){
				return 7;
			}
			
			if(a.toString().equals("09")){
				return 8;
			}
			if(a.toString().equals("10")){
				return 9;
			}
			if(a.toString().equals("11")){
				return 10;
			}
			if(a.toString().equals("12")){
				return 11;
			}
			else{
				return 12;
			}
			
			
			/*int x=0;String aa = null;
				for(int y=0;y<=9;y++){
					
					aa=x+""+y;
			}
				if(a.startsWith(aa)){
					return Integer.parseInt(aa);
				}
				else{
					return 1;
				}*/
			
	//	}
		
	}
	}
	public static class MyReducer extends Reducer<Text,FloatWritable,Text,FloatWritable>{
		public void reduce(Text key,Iterable<FloatWritable> value,Context context) throws IOException, InterruptedException{
			
			float sum=0;
			for(FloatWritable s:value){
				sum=sum+s.get();
			}
			context.write(key, new FloatWritable(sum));
				
			
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration c=new Configuration();
		Job job=Job.getInstance(c,"MonthlySAle");
		job.setJarByClass(MonthlyTrans.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setNumReduceTasks(13);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileSystem.get(c).delete(new Path(args[1]),true);//to delete automatically
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);

	}

}
