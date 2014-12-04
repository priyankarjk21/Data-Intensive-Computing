package co_occurance;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*To compute the Co-occurance using */
/*citation : http://codingjunkie.net/text-processing-with-mapreduce-part1/*/

public class PairCooccurance 
{

	private static final transient Logger LOG = LoggerFactory.getLogger(PairCooccurance.class);
	
	public static void main(String[] args)
	{
		
			Configuration conf = new Configuration();		

			LOG.info("HDFS Root Path: {}", conf.get("fs.defaultFS"));
			LOG.info("MR Framework: {}", conf.get("mapreduce.framework.name"));
			/* Set the Input/Output Paths on HDFS */
			String inputPath = "/input";
			String outputPath = "/output";

			/* FileOutputFormat wants to create the output directory itself.
			 * If it exists, delete it:
			 */
			try 
			{
				deleteFolder(conf,outputPath);
			} 
			catch (IOException e1) 
			{
				e1.printStackTrace();
			}
			
			Job job=null;
			try 
			{
				job = Job.getInstance(conf);
			} 
			catch (IOException e1) 
			{
				
				e1.printStackTrace();
			}

			job.setJarByClass(PairCooccurance.class);
			job.setMapperClass(CoOccurance_Mapper.class);
			//job.setCombinerClass(CoOccurance_Reducer.class);
			job.setNumReduceTasks(2);
			job.setPartitionerClass(CoOccurance_Partitioner.class);
			
			job.setReducerClass(CoOccurance_Reducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			try 
			{
				FileInputFormat.addInputPath(job, new Path(inputPath));
			} 
			catch (IllegalArgumentException e) 
			{
				e.printStackTrace();
			} 
			catch (IOException e)
			{
				e.printStackTrace();
			}
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			try 
			{
				System.exit(job.waitForCompletion(true) ? 0 : 1);
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
			} 
			catch (IOException e)
			{
				e.printStackTrace();
			} 
			catch (InterruptedException e) 
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

	

}
