package MapReduce1;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DotRectReducer extends Reducer<Text,Text,Text,Text>
{
	Text outputKey= new Text("");
	Text outputValue=new Text("");
	public void reduce(Text key ,Iterable<Text> values,Context context) throws IOException, InterruptedException
	{
		ArrayList<Dot> Dots = new ArrayList<Dot>();
		ArrayList<Rectangle> Rects = new ArrayList<Rectangle>();
		
		for(Text value:values)
		{
			String component = value.toString();
			String flag = component.split(",")[0];
			int index=component.indexOf(",");
			if(flag.equals("dot"))
			{	
				Dots.add(new Dot(component.substring(index+1)));
			}
			else
			{
				Rects.add(new Rectangle(component.substring(index+1)));				
			}				
		}
		
		for(Dot dot:Dots)
		{
			for(Rectangle rect:Rects)
			{
				if(dot.IsInRect(rect))
				{
					outputKey.set(rect.getName());;
					outputValue.set(dot.toString());;
					context.write(outputKey, outputValue);
				}
			}
		}
	}
}