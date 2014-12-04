package dijsktra_ssh;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Vector;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
 * To Compute the Shortest Path in Dijsktra's
 * */


public class Dijsktra 
{


	private static final transient Logger LOG = LoggerFactory.getLogger(Dijsktra.class);

	public static void main(String[] args)
	{

		Configuration conf = new Configuration();		

		LOG.info("HDFS Root Path: {}", conf.get("fs.defaultFS"));
		LOG.info("MR Framework: {}", conf.get("mapreduce.framework.name"));
		/* Set the Input/Output Paths on HDFS */
	//	String inputPath = "/ssh_input";
		String outputPath = "/ssh_output";
		Vector<String> previous = new Vector<String>();
		Vector<String> current=new Vector<String>();
		/* FileOutputFormat wants to create the output directory itself.
		 * If it exists, delete it:
		 */
		try 
		{
			long value=2;
			int i=0;
			int j=0;
			
		
			
			while(value>0)
			{
				value=0;
				 j=i;
				j=j+1;
				
				deleteFolder(conf,outputPath+j);
				Job job=null;
				try 
				{
					job = job.getInstance(conf);
				} 
				catch (IOException e1) 
				{

					e1.printStackTrace();
				}
				
					job.setJarByClass(Dijsktra.class);
					//System.out.println("Jar called");
					job.setMapperClass(Dijkstra_Mapper.class);
				//	System.out.println("Mapper called");
					job.setReducerClass(Dijkstra_Reducer.class);
					//System.out.println("Reducer Called");
					job.setOutputKeyClass(Text.class);
					job.setOutputValueClass(Text.class);
					FileInputFormat.addInputPath(job, new Path(outputPath+i));
					i=i+1;
					FileOutputFormat.setOutputPath(job, new Path(outputPath+i));
					//System.out.println("i value is"+i);
					
					
					try 
					{
						job.waitForCompletion(true);
					}
					catch(Exception e)					
					{
						e.printStackTrace();
					}
					try
					{
						Counters counters = job.getCounters();
						Counter c1 = counters.findCounter(Custom_Counter.DIJKSTRA_FLAG);
						value=c1.getValue();
						int k=0;
						k=i-1;
						if(k!=0)
						{
							deleteFolder(conf,outputPath+k);
						}
						//System.out.println("flag set is-->"+value);
					}
					catch(Exception e)
					{
						e.printStackTrace();
					}
					
					
			}

		}
		catch(Exception e)
		{
			e.printStackTrace();

		}
	}

	/**
	 * Delete a folder on the HDFS. This is an example of how to interact
	 * with the HDFS using the Java API. You can also interact with it
	 * on the command line, using: hdfs dfs -rm -r /path/to/delete
	 * 
	 * @param conf a Hadoop Configuration object
	 * @param folderPath folder to delete
	 * @throws IOException
	 */
	private static void deleteFolder(Configuration conf, String folderPath ) throws IOException {
		FileSystem fs = FileSystem.get(conf);
	
		Path path = new Path(folderPath);
		if(fs.exists(path)) {
			fs.delete(path,true);
		}
	}

	public static enum Custom_Counter 
	{
		DIJKSTRA_FLAG;
	};

}
