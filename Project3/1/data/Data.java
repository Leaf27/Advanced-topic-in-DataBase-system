package Problem1;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class Data {
	static final long SIZE = 100*1024*1024;
	static double dotSize =37;
	static double rectSize = 80;
	static Random rand = new Random();
	static final long HEIGHT=10000;
	static final long WIDTH=10000;
	static final int rectLength=20;
	static final int rectWidth=5;
	
	public static void main(String[] args) throws IOException
	{
		generateDots();
		generateRects();
	}
	
	public static void generateDots() throws IOException
	{
		File file = new File("Dots");
		if(!file.exists())
		{
			file.createNewFile();
		}
		
		ArrayList<Dot> dots=new ArrayList<Dot>();
		
		
		while(dots.size()<=(SIZE/dotSize))
		{
			dots.add(new Dot(RandomNumber(0,HEIGHT),RandomNumber(0,WIDTH)));
		}
		
		try(BufferedWriter bw= new BufferedWriter(new FileWriter(file)))
		{
			for(Dot dot:dots)
			{
				bw.write(dot.toString()+"\n");
			}
		}
				
	}
	
	public static void generateRects() throws IOException
	{
		File file =new File("Rectangles");
		if(!file.exists())
		{
			file.createNewFile();
		}
		
		ArrayList<Rectangle> rectangles = new ArrayList<Rectangle>();
		
		
		String name="R";
		int count =0;
		
		while(rectangles.size()<=(SIZE/rectSize))
		{
			Dot startpoint = new Dot(RandomNumber(0,HEIGHT),RandomNumber(0,WIDTH));
			double length = RandomNumber(1,rectLength);
			double width = RandomNumber(1,rectWidth);
			rectangles.add(new Rectangle(name+count,startpoint,length,width));
			count++;
		}
		
		try(BufferedWriter bw= new BufferedWriter(new FileWriter(file)))
		{
			for(Rectangle rect:rectangles)
			{
				bw.write(rect.toString()+"\n");
			}
		}	
	}

	public static double RandomNumber(long min, long max)
	{
		return rand.nextDouble()*(max-min)+min;
	}

}