package MapReduce2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CustomerMap extends Mapper<LongWritable,Text,IntWritable,Text>{
	IntWritable outputKey = new IntWritable(0);
	Text outputValue = new Text("");
	public void map(LongWritable key, Text value ,Context context) throws IOException, InterruptedException
	{
		String[] info = value.toString().split(",");
		int Salary = Integer.parseInt(info[3]);
		String Gender = info[4];
		outputKey.set(Salary);
		outputValue.set(Gender);
		context.write(outputKey, outputValue);
	}

}
