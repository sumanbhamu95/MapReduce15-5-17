package transTask;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortBasisAmt {

	/**
	 * Task 6 ::  Sort the whole file on the basis of amt.
	 */
	public static class MyMapper extends Mapper<LongWritable,Text,FloatWritable,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			
			
			TreeMap<Float,String> i1=new TreeMap<Float,String>();
			
			
			float f=Float.parseFloat(arr[3]);
			i1.put(f, arr[5]);
			
			
			for(Map.Entry<Float,String> m:i1.entrySet()){  
				   context.write(new FloatWritable(m.getKey()),new Text(m.getValue()));  
				  }  
			
			
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration c=new Configuration();
		Job job=Job.getInstance(c,"Sort on basis of amt");
		job.setJarByClass(SortBasisAmt.class);
		job.setMapperClass(MyMapper.class);
		job.setOutputKeyClass(FloatWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileSystem.get(c).delete(new Path(args[1]),true);//to delete automatically
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);


	}

}
