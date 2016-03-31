package KMeans;

public class KMeanDot {
	private double X;
	private double Y;
	
	public KMeanDot(double X,double Y)
	{
		this.X=X;
		this.Y=Y;
	}
	
	public KMeanDot(String info)
	{
		String[] detail = info.split(",");
		this.X = Double.parseDouble(detail[0]);
		this.Y= Double.parseDouble(detail[1]);
	}
	
	public double X()
	{
		return X;
	}
	
	public double Y()
	{
		return Y;
	}
	public double getDistance(KMeanDot dot)
	{
		return Math.sqrt(Math.pow((X-dot.X()), 2)+Math.pow((Y-dot.Y()), 2));
	}
	
	public String toString()
	{
		return X+","+Y+"\n";
	}
	
	public boolean equals(KMeanDot dot)
	{
		return X==dot.X()&&Y==dot.Y();
	}
}
