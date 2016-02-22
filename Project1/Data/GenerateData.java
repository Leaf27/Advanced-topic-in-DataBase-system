package Data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Random;
import java.util.UUID;

/**
 * Created by HuanYe on 16/2/1.
 */
public class GenerateData {
    static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    static Random rnd = new Random();

    public boolean GenerateCustomers()
    {
        File customers = new File("customers.db");
        Customer[] Customers= new Customer[50000];
        for(int i=0;i<50000;i++)
        {
            Customers[i] = new Customer();
            Customers[i].ID=i+1;
            Customers[i].name=RandomString(10,20);
            Customers[i].age =RandomInteger(10,70);
            Customers[i].CountryCode = RandomInteger(1,10);
            Customers[i].Salary = RandomFloat(100,10000);
        }

        try(BufferedWriter out = new BufferedWriter(new FileWriter(customers, false)))
        {
            for (Customer customer: Customers)
            {
                out.write(customer.toString());
                out.write("\n");
            }
        }
        catch (Exception e)
        {
            System.out.println("Failed to write to data file, " + e);
            return false;
        }
        return true;
    }

    public boolean GenerateTansactions()
    {
        File transactions = new File("transactions.db");
        Transaction[] Transactions= new Transaction[5000000];
        for(int i=0;i<5000000;i++)
        {
            Transactions[i] = new Transaction();
            Transactions[i].TransID=i+1;
            Transactions[i].CustID=RandomInteger(1,50000);
            Transactions[i].TansTotal =RandomFloat(10,1000);
            Transactions[i].TansNumItems = RandomInteger(1,10);
            Transactions[i].TansDesc = RandomString(20,50);
        }

        try(BufferedWriter out = new BufferedWriter(new FileWriter(transactions, false)))
        {
            for (Transaction transaction: Transactions)
            {
                out.write(transaction.toString());
                out.write("\n");
            }
        }
        catch (Exception e)
        {
            System.out.println("Failed to write to data file, " + e);
            return false;
        }
        return true;

    }

    private static String RandomString(int min,int max)
    {
        int length = RandomInteger(min,max);
        StringBuilder sb = new StringBuilder( length );
        for(int i=0;i<length;i++)
        {
            sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
        }
        return sb.toString();
    }

    private static int RandomInteger(int min, int max)
    {
        return min + (int)(Math.random()*(max-min));
    }

    private static float RandomFloat(int min, int max)
    {
        return (float)(min + Math.random()*(max-min));
    }

    private class Customer
    {
        int ID;
        String name;
        int age;
        int CountryCode;
        double Salary;

        @Override
        public String toString()
        {
            return this.ID+","+this.name+","+this.age+","+this.CountryCode+","+this.Salary;
        }

    }

    private class Transaction
    {
        int TransID;
        int CustID;
        double TansTotal;
        int TansNumItems;
        String TansDesc;

        @Override
        public String toString()
        {
            return this.TransID+","+this.CustID+","+this.TansTotal+","+this.TansNumItems+","+this.TansDesc;
        }
    }
}

