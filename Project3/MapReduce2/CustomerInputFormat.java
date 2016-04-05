package MapReduce2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class CustomerInputFormat extends FileInputFormat<LongWritable,Text>
{
	public CustomerRecordReader createRecordReader(InputSplit split,TaskAttemptContext context)
	{
		return new CustomerRecordReader();
	}
	
	protected boolean isSplitable(JobContext context,
            Path file)
	{
		return false;
	}
	
}
