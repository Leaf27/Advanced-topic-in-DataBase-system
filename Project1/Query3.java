package Query3;

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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query3
{
  static Map<Integer, String> customer = new HashMap<Integer,String>(); 
  static String path = "/user/hadoop/input/customers.db";
  
  public static void main(String[] args)
    throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException
  {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "customer detail");
//    read the sustomer file as sidtributedCache and store it into a hashmap
    DistributedCache.addCacheFile(new URI(path), conf);
    
    job.setJarByClass(Query3.class);
    job.setMapperClass(mapperTransaction.class);
    job.setReducerClass(reducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class mapperTransaction extends Mapper<LongWritable, Text, IntWritable, Text>
  {
	    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
      String[] content = value.toString().split(",");
      int customerId = Integer.parseInt(content[1]);
      float transTotal = Float.parseFloat(content[2]);
      int NumberItems = Integer.parseInt(content[3]);

        context.write(new IntWritable(customerId), new Text(transTotal + "," + NumberItems));
      
    }
  }

  public static class reducer extends Reducer<IntWritable, Text, IntWritable, Text>
  {
	  protected void setup(Context context) throws IOException, InterruptedException
	  {
		  Configuration conf = new Configuration();
		  FileSystem fs = FileSystem.get(conf);
		  try(BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(path)))))
		    {
		    	int ID =0;
		    	String line;
		    	String name = "";
		    	float salary = 0;
		    	while((line = br.readLine())!=null)
		    	{
		    		String[] content = line.split(",");
		    		ID= Integer.parseInt(content[0]);	
		    		name = content[1];
		    		salary = Float.parseFloat(content[4]);
		    	
		    		customer.put(ID, name+","+salary);
		    	}
		    }
	  }
    public void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException
    {
      int ID = key.get();
      int NumberofTrans = 0;
      float TotalSum = 0;
      int MinItem = Integer.MAX_VALUE;
      for (Text value : values)
      {
    	  String[] content = value.toString().split(",");
          NumberofTrans ++;
          TotalSum += Float.parseFloat(content[0]);
          MinItem = Math.min(MinItem, Integer.parseInt(content[1]));
      }
      String result = customer.get(ID)+"," + NumberofTrans + "," + TotalSum + "," + MinItem;
      context.write(key, new Text(result));
    }
  }
}