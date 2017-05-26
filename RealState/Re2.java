package real;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Re2 {

	/**
	 *2.In GALT city which residential type has more than 
	 *800sq__ft. Display their respective details street,sq__ft,sale_date,city.
	 */
	public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			
			
			if(arr[1].equals("GALT")){
			if(arr[7].equals("Residential"))
			{
				if(Integer.parseInt(arr[6])>800)
			context.write(new Text(arr[1]+","+arr[6]), new Text(arr[0]+","+arr[6]+","+arr[8]));
		}
		}//
		
	}}
		
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
			Configuration obj = new Configuration();
			Job job = Job.getInstance(obj, "pppp rr11.....");
			job.setJarByClass(Re2.class);
			job.setMapperClass(MyMapper.class);
		
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setNumReduceTasks(0);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			FileSystem.get(obj).delete(new Path(args[1]), true);
			System.exit(job.waitForCompletion(true) ? 0 : 1);

		

	}




}
