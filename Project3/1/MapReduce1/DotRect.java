package MapReduce1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DotRect {
	static double StartX =0;
	static double StartY=0;
	static double EndX=1000000;
	static double EndY=1000000;
	final static int GRIDLENGTH = 200;
	final static int GRIDWIDTH = 100;
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		
		 Configuration conf = new Configuration();
		 
		    if (args.length > 3) {
		    	StartX=Double.parseDouble(args[3]);
		    	StartY=Double.parseDouble(args[4]);
		    	EndX=Double.parseDouble(args[5]);
		    	EndY=Double.parseDouble(args[6]);	   
		    }	    
		    conf.set("Space", "Space"+","+StartX+","+StartY+","+(EndX-StartX)+","+(EndY-StartY));
		    Job job = new Job(conf, "join spacial data");
		    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,DotMap.class);
		    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class,RectMap.class);	
		    job.setJarByClass(DotRect.class);
		    job.setReducerClass(DotRectReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    FileOutputFormat.setOutputPath(job, new Path(args[2]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static int[] getKey(Dot dot)
	{
		int[] keys=new int[2];
		keys[0]=(int)(dot.getX()/DotRect.GRIDLENGTH);
		keys[1]=(int)(dot.getY()/DotRect.GRIDWIDTH);
		return keys;
	}
	
	public static String join(int[] keys)
	{
		return keys[0]+","+keys[1];
	}
}
