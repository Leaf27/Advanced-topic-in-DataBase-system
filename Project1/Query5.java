package Query5;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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

public class Query5
{
  static HashMap<String, String> customer = new HashMap<String,String>();   
  static String path = "/user/hadoop/input/customers.db";

  public static void main(String[] args)
    throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException
  {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "customer detail");
    conf.setBoolean("mapreduce.map.output.compress", true);
    DistributedCache.addCacheFile(new URI(path), conf);   
    job.setJarByClass(Query5.class);
    job.setMapperClass(mapper.class);
    job.setReducerClass(reducer.class);
//    job.setCombinerClass(reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  

  public static class mapper extends Mapper<LongWritable, Text, Text, IntWritable>
  {	  
	IntWritable one = new IntWritable(1);
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
      String[] content = value.toString().split(",");
      String customerId = content[1];
      context.write(new Text(customerId), one);     
    }
  }

  public static class reducer extends Reducer<Text, IntWritable, Text, IntWritable>
  {
	  HashMap<String, Integer> results = new HashMap<String, Integer>();
	  
	  protected void setup(Context context) throws IOException, InterruptedException
	  {
		  Configuration conf = new Configuration();
		  FileSystem fs = FileSystem.get(conf);
		    try(BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path)))))
		    {
		    	String ID ="";
		    	String line;
		    	String name = "";
		    	while((line = br.readLine())!=null)
		    	{
		    		String[] content = line.split(",");
		    		ID= content[0];	
		    		name = content[1];    	
		    		customer.put(ID, name);
		    	}
		    }	  
	  }
		
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    { 
      int count =0;    
      for(IntWritable value:values)
      {
    	 count++;
      }   
      String name = customer.get(key.toString());    
      results.put(name, count);
      context.write(null, null);
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
    	String name ="";
    	int count =Integer.MAX_VALUE;
    	for(Map.Entry<String, Integer> entry:results.entrySet())
    	{
    		if(entry.getValue()<count)
    		{
    			name = entry.getKey();
    			count=entry.getValue();
    		}
    	}
    	context.write(new Text(name), new IntWritable(count));
    	
    }
    
  }
}
