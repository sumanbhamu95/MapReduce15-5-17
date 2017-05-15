package maxandred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReducerSideJoinEx {

	/**
	 * ..
	 */
	public static class MyMapper1 extends
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// String a="max values";
			String arr[] = value.toString().split(",");
			context.write(new Text(arr[0]), new Text(arr[1] + "\ta"));
		}
	}

	public static class MyMapper2 extends
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String arr[] = value.toString().split(",");
			context.write(new Text(arr[0]), new Text(arr[1] + "," + arr[2]
					+ "\tb"));
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {

			String marks = "", xx = "";
			String sumu = "";
			int sum=0;
			for (Text num : value) {

				String ss[] = num.toString().split("\t");
				if (ss[1].equals("a")) {
					
					sum = sum+Integer.parseInt(ss[0]);

				} else if (ss[1].equals("b")) {
					xx = ss[0];
				}
				sumu = sum + "-" + xx;
			}

			context.write(key, new Text(sumu));

		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration c = new Configuration();
		Job job = Job.getInstance(c, "sss");
		job.setJarByClass(ReducerSideJoinEx.class);

		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(1);
		FileSystem.get(c).delete(new Path(args[2]), true);
		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class, MyMapper1.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, MyMapper2.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
