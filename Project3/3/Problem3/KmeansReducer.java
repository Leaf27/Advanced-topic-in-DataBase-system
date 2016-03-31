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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import KMeans.KmeansMain.UpdateCounter;

public class KmeansReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
	HashMap<Integer, KMeanDot> centers = new HashMap<Integer, KMeanDot>();
	HashMap<Integer, KMeanDot> newCenters = new HashMap<Integer, KMeanDot>();
	
	protected void setup(Context context) throws IOException 
	{
		Path path;
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		String out = conf.get("output");
		int depth = conf.getInt("depth",6);
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
			Xcount+=Integer.parseInt(info[1]);
			Ysum+=Double.parseDouble(info[2]);
			Ycount+=Integer.parseInt(info[3]);	
		}
		double centerX = Xsum/Xcount;
		double centerY = Ysum/Ycount;
		newCenters.put(key.get(),new KMeanDot(centerX, centerY));
		context.write(key, new Text(centerX+","+centerY));	
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		boolean res = true;
		for(Map.Entry<Integer, KMeanDot> center:centers.entrySet())
		{
			int key = center.getKey();
			KMeanDot value = center.getValue();
			if(!value.equals(newCenters.get(key)))
			{
				res = false;
			}
		}
		if(res)
		{
			context.write(new IntWritable(-1), new Text(KmeansMain.NO_NEED_MORE_CLUSTERING));		
		}
		else
		{
			context.write(new IntWritable(-1), new Text(KmeansMain.NEED_MORE_CLUSTERING));
			context.getCounter(UpdateCounter.UPDATE).increment(1);
		}
		
		
	}

}
