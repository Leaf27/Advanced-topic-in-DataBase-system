package MapReduce2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CustomerRecordWritable implements WritableComparable {
  
    private long CustomerID;
    private String Name;
    private String Address;
    private int Salary;
    private String Gender;

 public CustomerRecordWritable(long CustomerID, String Name, String Address,
   int Salary, String Gender) {
  super();
  this.CustomerID = CustomerID;
  this.Name = Name;
  this.Address = Address;
  this.Salary = Salary;
  this.Gender = Gender;
 }
 
    public int getSalary() 
    {
    	return Salary;
    }
    
    public String getGender()
    {
    	return Gender;
    }
    
    public long getCustomerID()
    {
    	return CustomerID;
    }
 

 @Override
    public void readFields(DataInput dataIp) throws IOException 
 {
	  CustomerID = dataIp.readLong();
	  Name = dataIp.readUTF(); 
	  Address = dataIp.readUTF(); 
	  Salary = dataIp.readInt();
	  Gender = dataIp.readUTF();         
    }
 
    @Override
    public void write(DataOutput dataOp) throws IOException 
    {
	     dataOp.writeLong(CustomerID );
	     dataOp.writeUTF( Name.toString()    );
	     dataOp.writeUTF( Address.toString()   );
	     dataOp.writeInt(Salary);
	     dataOp.writeUTF( Gender.toString()  );

    }

	 public int compareTo(CustomerRecordWritable o) 
	 {
	   return CustomerID<o.getCustomerID()?-1:(CustomerID==o.getCustomerID())?0:1;
	 }
	 
	 public int hashCode()
	 {
		 return (int)CustomerID;
	 }

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}
}
