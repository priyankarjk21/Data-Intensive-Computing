package co_occurance;

import org.apache.hadoop.mapreduce.Partitioner;

public class CoOccurance_Partitioner<Text, IntWritable> extends Partitioner<Text, IntWritable>
{

	public int getPartition(Text arg0, IntWritable arg1, int arg2) 
	{
		String keyVal[]=arg0.toString().split("\\-");
		System.out.println("Key Value is-->"+keyVal[0]);
		return Math.abs(keyVal[0].hashCode()%arg2);
	}

	

}
