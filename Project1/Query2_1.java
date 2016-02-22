package Query2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query2
{
  public static void main(String[] args)
    throws ClassNotFoundException, IOException, InterruptedException
  {
	  test(args[0], args[1]);
   
  }

  public static void test(String input, String output) throws IOException, ClassNotFoundException, InterruptedException
  {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "count the trasaction and total sum");
    job.setJarByClass(Query2.class);
    job.setMapperClass(transactionCountMapper.class);
    job.setReducerClass(transactionReducer.class);
//    job.setCombinerClass(transactionReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class transactionCountMapper extends Mapper<Object, Text, IntWritable, Text>
  {
    public void map(Object key, Text value,Context context)
      throws IOException, InterruptedException
    {
      int customerID = 0;
      float transaction = 0;
        String[] content = value.toString().split(",");
        if (content[1] != null) customerID = Integer.parseInt(content[1]);
        if (content[2] != null) transaction = Float.parseFloat(content[2]);
        context.write(new IntWritable(customerID), new Text((1+","+ transaction).toString()));
    }
  }

  public static class transactionReducer extends Reducer<IntWritable, Text, IntWritable, Text>
  {
    public void reduce(IntWritable key, Iterable<Text> value, Context context)
      throws IOException, InterruptedException
    {
      int count = 0;
      float sum = 0;
      for (Text content : value)
      {
    	String[] detail = content.toString().split(",");
        count += Integer.parseInt(detail[0]);
        sum += Float.parseFloat(detail[1]);
      }
      context.write(key, new Text((count+","+sum).toString()));
    }
  }
}
