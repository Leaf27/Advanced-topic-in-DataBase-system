package KMeans;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeansCombiner extends Reducer<IntWritable, Text, IntWritable, Text>{
	Text outputValue = new Text("");
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		double Xsum = 0;
		int Xcount = 0;
		double Ysum = 0;
		int Ycount = 0;
		for(Text value:values)
		{
			String[] info = value.toString().split(",");
			Xsum +=Double.parseDouble(info[0]);
			Xcount+=1;
			Ysum+=Double.parseDouble(info[1]);
			Ycount+=1;	
		}
		outputValue.set(Xsum+","+Xcount+","+Ysum+","+Ycount);
		context.write(key, outputValue);	
	}
	

}
