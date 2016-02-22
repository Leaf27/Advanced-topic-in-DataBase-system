package Query4;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query4
{
  static HashMap<String, Integer> customer = new HashMap<String,Integer>();   
  static String path = "/user/hadoop/input/customers.db";

  public static void main(String[] args)
    throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException
  {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "customer detail");

//	    read the sustomer file as sidtributedCache and store it into a hashmap
    
    DistributedCache.addCacheFile(new URI(path), conf);
   
    job.setJarByClass(Query4.class);
    job.setMapperClass(mapper.class);
    job.setReducerClass(reducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  

  public static class mapper extends Mapper<LongWritable, Text, IntWritable, Text>
  {
	  protected void setup(Context context) throws IOException, InterruptedException
	  {
		  Configuration conf = new Configuration();
		  FileSystem fs = FileSystem.get(conf);
		    try(BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path)))))
		    {
		    	String ID =null;
		    	String line;
		    	int CountryCode = 0;
		    	while((line = br.readLine())!=null)
		    	{
		    		String[] content = line.split(",");
		    		ID= content[0];	
		    		CountryCode = Integer.parseInt(content[3]);    	
		    		customer.put(ID, CountryCode);
		    	}
		    }	  
	  }
	  
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
      String[] content = value.toString().split(",");
      String customerId = content[1];
      float transTotal = Float.parseFloat(content[2]);
      int CountryCode = customer.get(customerId);
      context.write(new IntWritable(CountryCode), new Text(customerId + "," + transTotal));
      
    }
  }

  public static class reducer extends Reducer<IntWritable, Text, IntWritable, Text>
  {
	
    public void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException
    {
      HashMap<String, Float> MoreInfo = new HashMap<String, Float>();
      for(Text value:values)
      {
    	  String[] content = value.toString().split(",");
    	  String Id = content[0];
    	  float transtotal = Float.parseFloat(content[1]);
    	  Float test = MoreInfo.get(Id);
    	  if(test!=null) MoreInfo.put(Id, test+transtotal);
    	  else MoreInfo.put(Id, transtotal);
      }
      int numberOfCustomer = MoreInfo.size();
      
      float max = Float.NEGATIVE_INFINITY, min = Float.POSITIVE_INFINITY;
      
      for(Map.Entry<String, Float> entry:MoreInfo.entrySet())
      {
    	  if(entry.getValue()>max) max = entry.getValue();
    	  if(entry.getValue()<min) min = entry.getValue();
      }
//      MoreInfo.clear();
      context.write(key, new Text(numberOfCustomer+","+min+","+max));
    }
  }
}
