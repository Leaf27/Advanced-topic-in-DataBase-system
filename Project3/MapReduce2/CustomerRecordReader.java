package MapReduce2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class CustomerRecordReader extends RecordReader<LongWritable,Text>{
	private Configuration conf;
	private Path file = null;
	private Text value=new Text("");
	private LongWritable key= new LongWritable(0);
	private LineReader in;
	private long CustomerID;
	private String Name;
	private String Address;
	private int Salary;
	private String Gender;
	private long start;
	private long pos;
	private long end;
	private int maxLineLength;
	
	public CustomerRecordReader()
	{
	}
	
	 public void initialize(InputSplit genericSplit, TaskAttemptContext context)
	            throws IOException {
	        FileSplit split = (FileSplit) genericSplit;
	        file = split.getPath();
	        conf = context.getConfiguration();
	        FileSystem fs=file.getFileSystem(conf);
        	FSDataInputStream fileIn = fs.open(split.getPath());
        	in = new LineReader(fileIn,conf);

            
            this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength",Integer.MAX_VALUE);
            start = split.getStart();
            end= start + split.getLength();
            boolean skipFirstLine = false;
            FSDataInputStream filein = fs.open(split.getPath());
     
            if (start != 0){
                skipFirstLine = true;
                --start;
                filein.seek(start);
            }
            in = new LineReader(filein,conf);
            if(skipFirstLine){
                start += in.readLine(new Text(),0,(int)Math.min((long)Integer.MAX_VALUE, end - start));
            }
            this.pos = start;
        }

      public boolean nextKeyValue() throws IOException {
    	  value.clear();
    	  Text curretLine = new Text();
    	  int newSize = 0;
    	   	  
    	  while(pos<end)
    	  {
    		  newSize += in.readLine(curretLine,maxLineLength,
    	                Math.max((int) Math.min(
    	                        Integer.MAX_VALUE, end - pos),
    	                        maxLineLength));
    		  
    		  if(newSize ==0) break;
    		  pos+=newSize;
    		
        	  String[] info = curretLine.toString().split(":");
        	  String title = info[0];
        	  if(title.contains("}")) break;
        	  String data = info[1].substring(0, info[1].length()-1);
        	  
    		  if(title.contains("CustomerID"))
        	  {
        		  CustomerID = Long.parseLong(data);
        	  }
        	  else if (title.contains("Name"))
        	  {
        		  Name = data;
        	  }
        	  else if(title.contains("Address"))
        	  {
        		  Address = data;
        	  }
        	  else if (title.contains("Salary"))
        	  {
        		  Salary = Integer.parseInt(data);
        	  }
        	  else
        	  {
        		  Gender = data;
        		  createKeyValue();
        	  }       	  
    	  }
    	  if(newSize ==0)
    	  {
    		  key.set(0);
    		  value.set("");
    		  return false;
    	  }
    	  else
    	  {
    		  return true;
    	  }
      }
      
   private void createKeyValue()
   {
	    value.set(CustomerID+","+Name+","+Address+","+Salary+","+Gender);
	    key.set(pos); 
   }
   
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }
 
    @Override
    public Text getCurrentValue() {
        return value;
    }
 
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0; // TODO
    }

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		if(in!=null)
		{
			in.close();
		}
	}
}