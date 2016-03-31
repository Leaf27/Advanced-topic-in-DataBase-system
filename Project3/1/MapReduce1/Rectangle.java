package MapReduce1;

public class Rectangle {
	private String name;
	private Dot startPoint;
	private double length;
	private double width;
	
	public Rectangle(String name, Dot startPoint,double length,double width)
	{
		this.name=name;
		this.startPoint=startPoint;
		this.length=length;
		this.width=width;
	}
	
	public Rectangle(String detail)
	{
		String[] comps=detail.split(",");
		this.name=comps[0];
		this.startPoint=new Dot(Double.parseDouble(comps[1]),Double.parseDouble(comps[2]));
		this.length=Double.parseDouble(comps[3]);
		this.width=Double.parseDouble(comps[4]);	
	}
	
	public String toString()
	{
		return this.name+","+this.startPoint.toString()+","+this.length+","+this.width;
	}
	
	public double getLength()
	{
		return this.length;
	}
	
	public double getWidth()
	{
		return this.width;
	}
	
	public double getStartX()
	{
		return this.startPoint.getX();
	}
	
	public double getStartY()
	{
		return this.startPoint.getY();
	}
	
	public double getEndX()
	{
		return this.getStartX()+this.length;
	}
	
	public double getEndY()
	{
		return this.getStartY()+this.width;
	}
	
	public String getName()
	{
		return this.name;
	}
	
	public Dot Topleft()
	{
		return new Dot(this.getStartX(),this.getEndY());
	}
	
	public Dot BottomRight()
	{
		return new Dot(this.getEndX(),this.getStartY());
	}
	
	public Dot TopRight()
	{
		return new Dot(this.getEndX(),this.getEndY());
	}
	
	public Dot Bottomleft()
	{
		return this.startPoint;
	}
	
	public boolean AnyPointsInRect(Rectangle space)
	{
		return this.startPoint.IsInRect(space)||this.Topleft().IsInRect(space)||this.BottomRight().IsInRect(space)||this.TopRight().IsInRect(space);
	}
	
	public boolean IsInSpace(Rectangle space)
	{
		return this.AnyPointsInRect(space)||space.AnyPointsInRect(this);
	}

}
