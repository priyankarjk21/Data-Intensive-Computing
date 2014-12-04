package stripes;


import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Reducer for Stripes Method
 * */

public class Stripes_Reducer extends Reducer<Text, MapWritable, Text, FloatWritable>
{
	private MapWritable mapper=new MapWritable();
	protected void reduce(Text key,Iterable<MapWritable> values, Context context)
	{
		//clear the mapper prior to populating
		mapper.clear();
		
		//Retrieving parameters from what Mapper sent
		IntWritable count=new IntWritable();
		IntWritable denominator=new IntWritable();
		Text neighbor=new Text();
		Text splNeighbor=new Text("!");
		try
		{
			Text inputWord=new Text();
			FloatWritable frequency=new FloatWritable();
			for(MapWritable maps:values)
			{
				//Call the method to compute the proper HashMap computations to sum
				getValues(maps);
				
			}
			 for(Entry<Writable, Writable> entry : mapper.entrySet())
			 {
				 //Sum all the values of neighbor and set it as denominator
				 IntWritable val=(IntWritable)entry.getValue();
				 denominator.set(denominator.get()+val.get());
		     }
			 //Compute the Relative frequency of all key-neighbor pair and write it to the 
			 
			 for(Entry<Writable, Writable> entry : mapper.entrySet())
			 {
				 
				 IntWritable numerator=(IntWritable)entry.getValue();
				 float num=(float)numerator.get();
				 float denum=(float)denominator.get();
				 float freq=num/denum;
				 frequency.set(freq);
				
				 //Write into the reducer the key-neighbor pair and its frequency
				 StringBuffer valueToSend=new StringBuffer();
				 valueToSend.append(key);
				 valueToSend.append("-");
				 valueToSend.append(entry.getKey());
				 valueToSend.append("\t");
				 valueToSend.append("Count");
				 valueToSend.append(" ");
				 valueToSend.append(Float.toString(num));
				 valueToSend.append("\t");
				 valueToSend.append("R-freq");
				 
				 //Value to Send with frequency
				 context.write( new Text(valueToSend.toString()), frequency);
				  
			 }
				
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
			
		}
	}
	
	//Retrieve all values from the HashMap and sum it
	public void getValues(MapWritable maps)
	{
		
		//Retrieve the Writable Maps key-set to check the HashMap contents and sum its key-neighbor pair
		Set<Writable> keys=maps.keySet();
				
		for(Writable keyValue: keys)
		{
			
			IntWritable fromCount=(IntWritable)maps.get(keyValue);
			int getCount=fromCount.get();
			
			//Check if mapper already has the key
			boolean checkKeyExists=mapper.containsKey(keyValue);
			if(checkKeyExists)
			{
				IntWritable counter=(IntWritable)mapper.get(keyValue);
				int valOfCounter=counter.get();
				counter.set(valOfCounter+getCount);
			}
			else
			{
				//if neighbor encountered for first time,then add to the Mapper
				fromCount.set(getCount);
				//Write the count to the mapper
				mapper.put(keyValue,fromCount);
			}
		}
	}
	
}