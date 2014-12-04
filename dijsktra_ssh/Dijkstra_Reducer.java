package dijsktra_ssh;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import dijsktra_ssh.Dijsktra.Custom_Counter;

public class Dijkstra_Reducer extends Reducer<Text,Text,Text,Text> 
{
    protected void reduce(Text key,Iterable<Text> val, Context context) throws IOException, InterruptedException
    {
    	
    	int infinite_distance=20000;		//set infinte cost
    	int recv_distance=10000;
    	String connections="";
    	
    	for(Text v: val)
    	{
    		String value=v.toString();
    		String nodesStr[]=value.split("#");
    		String startW=nodesStr[0];
    		
    		if(startW.startsWith("C"))
    		{
	    		
	    		String node_num=nodesStr[1];
	    		int cost=Integer.parseInt(node_num);
	    		if(cost<infinite_distance)
	    		{
	    			infinite_distance=cost;
	    			
	    		}
	    	}
    		else if(startW.startsWith("R"))
    		{
    			 connections=nodesStr[1];
	    		
    		}
    		else if(startW.startsWith("F"))
    		{
    			String allVal[]=value.split("#");
    			String fileInput[]=allVal[1].split("\\s");
    			String distanceStr=fileInput[1];
    			String sourceNode=fileInput[0];
    			String allNodes=fileInput[2];
    			recv_distance=Integer.parseInt(distanceStr);
    			
    		}
    		
    		
    	}
    	if((infinite_distance!=recv_distance))
		{
			context.getCounter(Custom_Counter.DIJKSTRA_FLAG).setValue(1);
		}
    	String costStr=Integer.toString(infinite_distance);
    	
    	//write to the output file
    	context.write(key,new Text(costStr+" "+connections));
    	
    	
    
    }

}
