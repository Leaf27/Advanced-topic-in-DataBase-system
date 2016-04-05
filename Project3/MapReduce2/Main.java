package MapReduce2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Path inputPath = new Path("hdfs://localhost:8020/user/hadoop/input/Customer");
		Path outputPath = new Path("hdfs://localhost:8020/user/hadoop/output/Mapreduce2");
		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(conf);
	    Job job = new Job(conf, "customer");
	    job.setMapperClass(CustomerMap.class);
	    job.setCombinerClass(CustomerCombiner.class);
	    job.setReducerClass(CustomerReducer.class);
	    job.setJarByClass(Main.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setInputFormatClass(CustomerInputFormat.class);
	    CustomerInputFormat.setInputPaths(job, inputPath);

	    FileOutputFormat.setOutputPath(job, outputPath);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
