package co_occurance;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CoOccurance_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> 
{
	private Text wpPair=new Text();
	private IntWritable one =new IntWritable(1);
	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{

		String tweetLine[]=value.toString().split("\\s");
		
		for(int i=0;i<tweetLine.length;i++)
		{
			
				if((tweetLine[i].startsWith("#")) && (tweetLine[i].length()>2))
				{
		
						
						for(int j=i+1;j<tweetLine.length;j++)
						{
							
								if(tweetLine[j].startsWith("#"))
								{
										
										wpPair.set(tweetLine[i]+"-"+"!");
										context.write(wpPair, one);
										wpPair.set(tweetLine[i]+"-"+tweetLine[j]);
										context.write(wpPair, one);
										
										wpPair.set(tweetLine[j]+"-"+"!");
										
										
										context.write(wpPair, one);		
										wpPair.set(tweetLine[j]+"-"+tweetLine[i]);
										context.write(wpPair, one);
										
										
								}
							
							
						}
					}
		
		}
						

}
	
}