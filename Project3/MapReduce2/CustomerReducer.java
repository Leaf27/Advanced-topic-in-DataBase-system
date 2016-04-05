package MapReduce2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CustomerReducer extends Reducer<IntWritable, Text,IntWritable,Text>
{
	Text outputValue = new Text("");
	public void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException
	{
		int sumF =0;
		int sumM = 0;
		for(Text value:values)
		{
			String[] info= value.toString().split(",");
			sumF+= Integer.parseInt(info[0]);
			sumM+=Integer.parseInt(info[1]);
		}
		outputValue.set(sumF+","+sumM);
		context.write(key, outputValue);
	}
}
