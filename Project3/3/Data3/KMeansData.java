package Problem3Data;

import java.io.BufferedWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class KMeansData {
	final static long  SIZE = 100*1024*1024;
	static int DotByte = 36;
	static Random rand = new Random();
	static int K ;
	
	public static void main(String[] args) throws IOException
	{
		K=Integer.parseInt(args[0]);
		GeneData();
	}
	
	public static void GeneData() throws IOException
	{
		ArrayList<KMeanDot> dots = new ArrayList<KMeanDot>();
		for(int i=0;i<SIZE/DotByte;i++)
		{
			double X =RandomNumber(0,10000);
			double Y = RandomNumber(0,10000);
			dots.add(new KMeanDot(X,Y));
		}
		ToFile(dots,"KMeanData");
		
		KMeanDot[] Centers = new KMeanDot[K];
		for(int i=0;i<K;i++)
		{
			Centers[i]=dots.get((int)RandomNumber(0,dots.size()));
		}
		ToFile(Centers,"Centers0");
	}
	
	public static double RandomNumber(long min, long max)
	{
		return rand.nextDouble()*(max-min)+min;
	}
	
	public static void ToFile(ArrayList<KMeanDot> dots,String fileName) throws IOException
	{
		File file = new File(fileName);
		if(!file.exists())
		{
			file.createNewFile();
		}
		
		try(BufferedWriter bw = new BufferedWriter(new FileWriter(file)))
		{
			for(KMeanDot dot :dots)
			{
				bw.write(dot.toString());
			}
		}	
	}
	
	public static void ToFile(KMeanDot[] dots,String fileName) throws IOException
	{

		File file = new File(fileName);
		if(!file.exists())
		{
			file.createNewFile();
		}
		
		try(BufferedWriter bw = new BufferedWriter(new FileWriter(file)))
		{
			for(int i=0;i<dots.length;i++)
			{
				bw.write(i+"\t"+dots[i].toString());
			}
		}	
	}
}
