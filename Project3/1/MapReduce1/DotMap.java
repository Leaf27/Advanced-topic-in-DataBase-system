package MapReduce1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DotMap extends Mapper<LongWritable,Text,Text,Text>
{
	Text outputKey= new Text("");
	Text outputValue=new Text("");
	private static Rectangle Space;
	
	public void setup(Context context)
	{
		Configuration conf = context.getConfiguration();
		String space = conf.get("Space");
		Space=new Rectangle(space);
		
	}
	
	public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException
	{
		String info=value.toString();
		Dot dot =new Dot(info);
		if(dot.IsInRect(Space))
		{
			outputKey.set(DotRect.join(DotRect.getKey(dot)));
			outputValue.set("dot"+","+info);
			context.write(outputKey,outputValue);	
		}							
	}
	
	
	
	
}
