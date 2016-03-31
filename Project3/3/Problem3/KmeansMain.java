package KMeans;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KmeansMain {
	public enum UpdateCounter
	{
		UPDATE
	}
	
	final static String NO_NEED_MORE_CLUSTERING = "no_need_more_clustering";
	final static String NEED_MORE_CLUSTERING = "need_more_clustering";
	final static int DEPTHMAX = 6;
	static int depth =0;
	final static String SEPERATOR="\t";
	final static String OUTPUTSECTION = "/part-r-00000";
	
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException
	{			
		long counter =1;
		String in = args[0];
		String out = args[1].substring(0,args[1].length()-1);
		
		while(counter>0 &&depth<DEPTHMAX+1)
		{
			Configuration conf = new Configuration();
			conf.setInt("depth", depth);			
			conf.set("output", out);			
			conf.set("mapreduce.output.textoutputformat.separator", SEPERATOR);

			Job job = new Job(conf, "Kmeans");						
			job.setJarByClass(KmeansMain.class);
			job.setMapperClass(KmeansMap.class);
			job.setCombinerClass(KmeansCombiner.class);
			job.setReducerClass(KmeansReducer.class);
			
			if(depth==DEPTHMAX)
			{
				job.setNumReduceTasks(0);
			}
			FileInputFormat.addInputPath(job, new Path(in));
			FileSystem fs = FileSystem.get(conf);
			
			Path outPath = new Path(out+(depth+1));
			if(fs.exists(outPath))
			{
				fs.delete(outPath,true);
			}
	
			FileOutputFormat.setOutputPath(job, outPath);
			
			DistributedCache.addCacheFile(new URI(out+depth), conf);
			
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);	
			job.waitForCompletion(true);
			depth++;
			counter=job.getCounters().findCounter(UpdateCounter.UPDATE).getValue();
		}
		
	}
}
