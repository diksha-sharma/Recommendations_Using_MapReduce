package socialnetwork;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q3 
{
	public static class Map extends Mapper<Object, Text, Text, Text>  
	{
		private String user1;
		private String user2;
		private String user;
		private Text friendsList = new Text();
		ArrayList<String> list = new ArrayList<String>();
        
		public void map(Object key, Text value,Context context) throws IOException, InterruptedException 
		{
			String line = value.toString();
			String[] splitLine = line.split("\t");	
			if(splitLine.length == 2)
			{
				if(splitLine[0].equals("3"))
				{
					user1 = splitLine[0].trim();
					friendsList = new Text(splitLine[1].trim());
					String[] sFriends = friendsList.toString().split(",");
					for(String sFriend : sFriends)
					{
						list.add(sFriend);
					}				
				}
				else if(splitLine[0].equals("7"))
				{
					user2 = splitLine[0].trim();
					friendsList = new Text(splitLine[1].trim());
					String[] sFriends = friendsList.toString().split(",");
					for(String sFriend : sFriends)
					{
						list.add(sFriend);
					}	
					
					user = user1 + " " + user2;
					
					ArrayList<String> friends = new ArrayList<String>();
					for(int i=1; i< list.size(); i++)
					{
						if(!list.get(i).equals(list.get(i-1)))
						{
							friends.add(list.get(i));
						}
					}					
					context.write(new Text(user),new Text(list.toString()));
				}				
			}
		}		
	}	
	
	public static class ZipCode extends Mapper<Object, Text, Text, Text>  
	{
		public void map(Object key, Text value,Context context) throws IOException, InterruptedException 
		{
			String userData[] = value.toString().split(",");
			context.write(new Text(userData[0]), new Text(userData[6]));			
		}		
	}
	
	public static void main(String[] args) throws Exception 
	{
		Configuration cConfiguration1 = new Configuration();
		String[] argsGiven = new GenericOptionsParser(cConfiguration1, args).getRemainingArgs();
		if (argsGiven.length != 6) 
		{
			System.err.println("Required arguments not provided...Quitting...");
			System.err.println("hadoop jar <jarname> <classname> <input1> <input2> <intermediate_output> <final_output> <user1> <user2>");
			System.exit(0);
		}
		
		cConfiguration1.set("user1", args[4]);
		cConfiguration1.set("user2", args[5]);
		Job job1 = Job.getInstance(cConfiguration1, "JOB1");
		
		job1.setJarByClass(Q3.class);
		job1.setMapperClass(Map.class);
		job1.setNumReduceTasks(0);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));
		
		boolean isJob1Completed = job1.waitForCompletion(true);
		
		if (isJob1Completed == true)
		{
			Configuration cConfiguration2 = new Configuration();
			Job job2 = Job.getInstance(cConfiguration2, "JOB2");
			
			job2.setJarByClass(Q3.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);

			MultipleInputs.addInputPath(job2, new Path(args[2]),TextInputFormat.class, Map.class);
			MultipleInputs.addInputPath(job2, new Path(args[1]),TextInputFormat.class, ZipCode.class);
			
			FileOutputFormat.setOutputPath(job2, new Path(args[3]));

			job2.waitForCompletion(true);			
		} 
	}
}
