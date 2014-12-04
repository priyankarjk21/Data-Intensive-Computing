package stripes;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;


/*
 * Mapper Class for Stripes Method
 * */
public class Stripes_Mapper extends Mapper<LongWritable,Text,Text,MapWritable>
{
	public MapWritable mapper=new MapWritable();
	Text word=new Text();
	Text splWord=new Text();

	
	protected void map(LongWritable key,Text value,Context context)
	{
		try
		{
			String tweetLine[]=value.toString().split("\\s");
			splWord.set("!");
			
			//Check if line has more than one word
			if(tweetLine.length>1)
			{
			
				for(int i=0;i<tweetLine.length;i++)
				{
					mapper.clear();
					//Check if hash tag has word
					if(tweetLine[i].startsWith("#") && tweetLine[i].length()>2)
					{
						String keyValue[]=tweetLine[i].split("#");
						
						
							word.set(tweetLine[i]);
							for(int j=0;j<tweetLine.length;j++)
							{
								//Don Check with self
								if(j!=i)
								{
									if(tweetLine[j].startsWith("#") && tweetLine[j].length()>2)
									{
										
											//Ensure the word is not a stop word
											String neighborVal[]=tweetLine[j].split("#");
											
												Text neighbor=new Text();
												neighbor.set(tweetLine[j]);
												
												//If word already exists in the mapper ,increment its counter
												if(mapper.containsKey(neighbor))
												{
													IntWritable numberOF=new IntWritable();
													numberOF.set(numberOF.get()+1);
													
												}
												//If word does not exist in the counter then ass in the hashMap
												else
												{
													mapper.put(neighbor,new IntWritable(1));
													
													
												}
										
									}
							   }
							}
					
						//send the key with its respective hashMap
					context.write(word,mapper);
						
					}
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
}


/*public class Stripes_Mapper extends Mapper<LongWritable,Text,Text,MapWritable> 
{
	private MapWritable occurrenceMap = new MapWritable();
	  private Text word = new Text();
protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	   int neighbors = context.getConfiguration().getInt("neighbors", 2);
	   String[] tokens = value.toString().split("\\s+");
	   if (tokens.length > 1) {
	      for (int i = 0; i < tokens.length; i++) {
	          word.set(tokens[i]);
	          occurrenceMap.clear();

	          int start = (i - neighbors < 0) ? 0 : i - neighbors;
	          int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
	           for (int j = start; j <= end; j++) {
	                if (j == i) continue;
	                Text neighbor = new Text(tokens[j]);
	                if(occurrenceMap.containsKey(neighbor)){
	                   IntWritable count = (IntWritable)occurrenceMap.get(neighbor);
	                   count.set(count.get()+1);
	                }else{
	                   occurrenceMap.put(neighbor,new IntWritable(1));
	                }
	           }
	          context.write(word,occurrenceMap);
	     }
	   }
	  }
	
}*/