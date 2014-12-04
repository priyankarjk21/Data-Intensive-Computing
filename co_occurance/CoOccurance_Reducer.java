package co_occurance;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*To Reduce the received input*/
public class CoOccurance_Reducer extends Reducer<Text,IntWritable,Text,FloatWritable> 
{
	
	
		    private FloatWritable totalCount = new FloatWritable();
		    private FloatWritable getRelativeFrequency= new FloatWritable();
		    Text textVal=new Text("new");
		    
		    protected void reduce(Text key,Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		    {
			    
			    //to compute the Relative Neighbors
			    
		    	String[] keyValue= key.toString().split("\\-");
			    String word=keyValue[0];
			    String neighbor=keyValue[1];
			   
			    
			    Text keyText=new Text();
			    keyText.set(word);
			    
			    //Check if neighbor is appended with "! or not, if yes compute the denominator
			    if(neighbor.equals("!"))
			    {
			    	if(keyText.equals(textVal))
			    	{
			    		int getCountOfValues=getTotalCount(values);
			    		int getCurrentCount=(int) totalCount.get();
			    		int currentTotal=getCountOfValues+getCurrentCount;
			    		totalCount.set(currentTotal);
			    	}
			    	else
			    	{
			    		textVal.set(keyText);
			    		int currentCount=getTotalCount(values);
		                totalCount.set(currentCount);
			    	}
		    	
			    }
			    //if not then compute the relative frequency and emit 
			    else
			    {
			    	int count = getTotalCount(values);
			    	float computedTotal=(float)totalCount.get();
			    	float frequency=count/computedTotal;
			    	getRelativeFrequency.set(frequency);
			    	
			    	//Send Across Count and Relative frequency to write in file
			    	
			    	 StringBuffer valueToSend=new StringBuffer();
					 valueToSend.append(key);
					
					 valueToSend.append("\t");
					 valueToSend.append("Count");
					 valueToSend.append(" ");
					 valueToSend.append(Integer.toString(count));
					 valueToSend.append("\t");
					 valueToSend.append("R-freq");
					 
			    	
		            context.write(new Text(valueToSend.toString()), getRelativeFrequency);
			    }
	   
			    
			    
	        }
		    
		    //Compute the total
		    //Reference: from wordCount.jar Reducer method
		    public int getTotalCount(Iterable<IntWritable> values)
		    {
		    	int count = 0;
			    for (IntWritable value : values) 
			    {
			          count += value.get();
			    }
		    	return count;
		    }
}
