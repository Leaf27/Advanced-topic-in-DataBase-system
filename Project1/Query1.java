package Query1;

import java.io.IOException;
import java.io.PrintStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query1
{
  public static void main(String[] args)
    throws Exception
  {
    Configuration conf = new Configuration();
    if (args.length != 2)
    {
      System.err.println("Usage: wordcount <HDFS input file> <HDFS output file>");
      System.exit(2);
    }
    Job job = new Job(conf, "count customers");
    job.setJarByClass(Query1.class);
    job.setMapperClass(CountMapper.class);
    job.setNumReduceTasks(0);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class CountMapper extends Mapper<Object, Text, Text, Text>
  {
    public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      int Countrycode = Integer.parseInt(value.toString().split(",")[3]);
      if ((Countrycode >= 2) && (Countrycode <= 6)) context.write(null, value);
    }
  }
}
