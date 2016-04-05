package MapReduce2;

public class Customer {
	private long CustomerID;
	private String Name;
	private String Address;
	private int Salary;
	private String Gender;
	
	public Customer(long CustomerID,String Name, String Address, int Salary,String Gender)
	{
		this.CustomerID=CustomerID;
		this.Name=Name;
		this.Address=Address;
		this.Salary=Salary;
		this.Gender=Gender;
	}
	
	public String toString()
	{
		return String.format("{CustomerID:%d,\n"+
							"Name:%s,\n"+
							"Address:%s,\n"+
							"Salary:%d,\n"+
							"Gender:%s,\n"+
							"}", this.CustomerID,this.Name,this.Address,this.Salary,this.Gender);
	}
}