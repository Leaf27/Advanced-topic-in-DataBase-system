package MapReduce1;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class RectMap extends Mapper<LongWritable,Text,Text,Text>
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
		Rectangle rect = new Rectangle(value.toString());
		if(rect.IsInSpace(Space))
		{
			ArrayList<Rectangle> rects =getSplit(rect);		
			for(Rectangle SingleRect:rects)
			{
				outputKey.set(DotRect.join(DotRect.getKey(SingleRect.Bottomleft())));
				outputValue.set("Rect,"+SingleRect.toString());				
				context.write(outputKey, outputValue);
			}
		}		
	}
	
	private static ArrayList<Rectangle> getSplit(Rectangle rect)
	{
		ArrayList<Rectangle> res = getSpliteByY(getSplitByX(rect));
		return res;
	}
	
	private static ArrayList<Rectangle> getSplitByX(Rectangle rect)
	{
		int startXKey=DotRect.getKey(rect.Bottomleft())[0];
		int endXKey=DotRect.getKey(rect.BottomRight())[0];
		ArrayList<Rectangle> rects=new ArrayList<Rectangle>();
		
		if(startXKey==endXKey)
		{
			rects.add(rect);
		}
		else
		{
			int XSeperator=endXKey*DotRect.GRIDLENGTH;
			rects.add(new Rectangle(rect.getName(),rect.Bottomleft(),XSeperator-rect.getStartX(),rect.getWidth()));
			rects.add(new Rectangle(rect.getName(),new Dot(XSeperator,rect.getStartY()),rect.getEndX()-XSeperator,rect.getWidth()));
		}
		return rects;
	}
	
	private static  ArrayList<Rectangle> getSpliteByY(ArrayList<Rectangle> rects)
	{
		ArrayList<Rectangle> res=new ArrayList<Rectangle>();
		for(Rectangle rect: rects)
		{
			int startYkey=DotRect.getKey(rect.Bottomleft())[1];
			int endYkey=DotRect.getKey(rect.Topleft())[1];
			
			if(startYkey==endYkey)
			{
				res.add(rect);
			}
			else
			{
				int YSeperator=endYkey*DotRect.GRIDWIDTH;
				res.add(new Rectangle(rect.getName(),rect.Bottomleft(),rect.getLength(),YSeperator-rect.getStartY()));
				res.add(new Rectangle(rect.getName(),new Dot(rect.getStartX(),YSeperator),rect.getLength(),rect.getEndY()-YSeperator));
			}
		}
		return res;
	}
}
