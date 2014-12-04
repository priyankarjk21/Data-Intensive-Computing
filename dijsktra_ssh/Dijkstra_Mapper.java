package dijsktra_ssh;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Dijkstra_Mapper extends Mapper<LongWritable, Text, Text, Text> 
{
	public 	void map(LongWritable key,Text values,Context context)
	{
		try
		{
				String strVal=values.toString();
				String val[]=strVal.split("\\s");
				String distanceStr= val[1];
				String sourceNode=val[0];
				String allNodes=val[2];
				int node_distance=Integer.parseInt(distanceStr)+1;
				String nodeCost=Integer.toString(node_distance);
				Text textkey=new Text();
				Text textVal=new Text();
				String nodeStr= val[2];
				String to_nodes[]=nodeStr.split(":");
				int i=0;
				String connectedNodes="C";
				String restOfNodes="R";
				String fullVal="F";
				StringBuffer keyVal=new StringBuffer();
			
				//to send the nodes and their respective distance to reducer
				
				while(i<to_nodes.length)
				{
					keyVal.append(connectedNodes);
					keyVal.append("#");
					keyVal.append(nodeCost);
					textVal.set(keyVal.toString());
					textkey.set(to_nodes[i]);
					context.write(textkey, textVal);
					textVal.clear();
					textkey.clear();
					i++;
					keyVal.setLength(0);
				}
				
				//send the cost of self to self
				keyVal.append(connectedNodes);
				keyVal.append("#");
				keyVal.append(distanceStr);
				textVal.set(keyVal.toString());
				textkey.set(sourceNode);
			
				context.write(textkey, textVal);
				keyVal.setLength(0);
				textVal.clear();
				textkey.clear();
				
				//send its connection details
				keyVal.append(restOfNodes);
				keyVal.append("#");
				keyVal.append(allNodes);
				textVal.set(keyVal.toString());
				textkey.set(sourceNode);
				context.write(textkey, textVal);
				textkey.clear();
				textVal.clear();
				keyVal.setLength(0);
				
				keyVal.append(fullVal);
				keyVal.append("#");
				keyVal.append(strVal);
				textVal.set(keyVal.toString());
				textkey.set(sourceNode);
				context.write(textkey, textVal);
				textkey.clear();
				textVal.clear();
				keyVal.setLength(0);
			
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			
		}

}
