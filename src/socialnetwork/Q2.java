package socialnetwork;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q2 
{
	public static class Map extends Mapper<Object, Text, Text, Text>  
	{
		private String user1;
		private String user2;
		private String user;
		private String fList;
		ArrayList<String> list = new ArrayList<String>();
		String[] sFriendsList = new String[2];
        
		public void map(Object key, Text value,Context context) throws IOException, InterruptedException 
		{
			String line = value.toString();
			String[] splitLine = line.split("\t");				 
			if(splitLine.length == 2)
			{
				if(splitLine[0].equals("3"))
				{
					user1 = splitLine[0].trim();
					fList = splitLine[1].trim();
					sFriendsList[0] = fList;	
					context.write(new Text(user1),new Text(fList.toString()));
				}
				else if(splitLine[0].equals("7"))
				{
					user2 = splitLine[0].trim();
					fList = splitLine[1].trim();
					sFriendsList[1] = fList;
					context.write(new Text(user2),new Text(fList.toString()));
				}				
			}
			
			if(sFriendsList[0].length() > 0 && sFriendsList[1].length() > 0)
			{
				user = user1 + " " + user2;				
				ArrayList<String> friends1 = new ArrayList<String>(Arrays.asList(sFriendsList[0].split(",")));
				ArrayList<String> friends2 = new ArrayList<String>(Arrays.asList(sFriendsList[1].split(",")));
				ArrayList<String> mfriends = new ArrayList<String>();
				
				for(String s: friends1)
				{
					if(friends2.contains(s))
					{
						mfriends.add(s);
					}
				}
									
				context.write(new Text(user),new Text(mfriends.toString()));
			}
		}		
	}
	
	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] argsGiven = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (argsGiven.length != 5) 
		{
			System.err.println("Required arguments not provided...Quitting...");
			System.err.println("hadoop jar <jarname> <classname> <input1> <intermediate_output> <final_output> <user1> <user2>");
			System.exit(0);
		}
		
		conf.set("user1", args[3]);
		conf.set("user2", args[4]);
		Job job = Job.getInstance(conf, "JOB");
		
		job.setJarByClass(Q2.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));		

		job.waitForCompletion(true);
	}
}
