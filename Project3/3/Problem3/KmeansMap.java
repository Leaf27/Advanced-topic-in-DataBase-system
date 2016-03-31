package KMeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KmeansMap extends Mapper<LongWritable, Text, IntWritable, Text>{
	HashMap<Integer, KMeanDot> centers = new HashMap<Integer, KMeanDot>();
	IntWritable outputKey = new IntWritable();
	Text outputValue = new Text();
	static String out;
	static int depth;

	protected void setup(Context context) throws IOException, InterruptedException 
	{
		Path path;
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		out = conf.get("output");
		depth = conf.getInt("depth",2);
		if(depth==0)
		{
			path = new Path(out+depth);
		}
		else
		{
			path = new Path(out+depth+KmeansMain.OUTPUTSECTION);
		}
		try(BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path))))
		{
			int key;
			String line;
			while((line=br.readLine())!=null)
			{
				String[] info = line.split(KmeansMain.SEPERATOR);
				key = Integer.parseInt(info[0]);
				if(key!=-1)
				{
					centers.put(key, new KMeanDot(info[1]));				
				}
			}
		}
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String[] info = value.toString().split(",");
		double X = Double.parseDouble(info[0]);
		double Y = Double.parseDouble(info[1]);
		KMeanDot dot = new KMeanDot(X,Y);
		
		double distance=Double.MAX_VALUE;
		int cluster=0;
		for(Map.Entry<Integer, KMeanDot> center:centers.entrySet())
		{
			double currentDistance = dot.getDistance(center.getValue());
			if(currentDistance<distance)
			{
				distance = currentDistance;
				cluster = center.getKey();
			}
		}
		outputKey.set(cluster);
		context.write(outputKey, value);
	}
}
