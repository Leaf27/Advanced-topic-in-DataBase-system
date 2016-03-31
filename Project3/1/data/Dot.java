package Problem1;

public class Dot {
	private  double X;
	private double Y;
	
	public Dot(double X,double Y)
	{
		this.X=X;
		this.Y=Y;
	}
	
	public Dot(String inform)
	{
		String[] comps=inform.split(",");
		this.X=Double.parseDouble(comps[0]);
		this.Y=Double.parseDouble(comps[1]);
	}
	
	public String toString()
	{
		return this.X+","+this.Y;
	}
	
	public double getX()
	{
		return this.X;
	}
	
	public double getY()
	{
		return this.Y;
	}
	
	public boolean IsInRect(Rectangle rect)
	{
		return this.X>=rect.getStartX()&&this.X<=rect.getEndX()&&this.Y>=rect.getStartY()&&this.Y<=rect.getEndY();
	}

}
