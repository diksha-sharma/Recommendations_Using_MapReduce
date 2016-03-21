package socialnetwork;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q4
{	
	//This class get the birth date and the user id of the users from userdata.txt file
	public static class UsersBirthDateMapper extends Mapper<Text, Text, Text, Text> 
	{
		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException 
		{
			String userData[] = value.toString().split(",");
			String sAge = calculateAge(userData[10]);
			//Now we have key-value pairs with user id and the age of each of their friends
			context.write(new Text(userData[1]),new Text(sAge));
		}
	}
	
	//This method returns the age based on the birth date that is passed as the argument
	public static String calculateAge(String sBirthDate)
	{
		int iAge = 0;
		Calendar oldCal = Calendar.getInstance();
		Calendar currCal = Calendar.getInstance();
		SimpleDateFormat sdfDateFormatter = new SimpleDateFormat("MM/dd/yyyy");
		try 
		{
			Date dBirthDate = sdfDateFormatter.parse(sBirthDate);
			oldCal.setTime(dBirthDate);
		} 
		catch(Exception e) 
		{
			System.err.println("Exception during string to date conversion" + e);
		}
		
		
		iAge = currCal.get(Calendar.YEAR) - oldCal.get(Calendar.YEAR);
		if (currCal.get(Calendar.DAY_OF_YEAR) < oldCal.get(Calendar.DAY_OF_YEAR))
		{
			iAge--;
		}		
		return String.valueOf(iAge);
	}
	
	public static class Top20AverageAge extends Reducer<Text, Text, Text, Text> 
	{
		private TreeMap<String, Float> tMap = new TreeMap<>();
		private AgeComparator ageComparator = new AgeComparator(tMap);
		private TreeMap<String, Float> sortedTMap = new TreeMap<String, Float>(ageComparator);

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			float sum = 0;
			float count = 0;
			for (Text value : values) 
			{
				sum += Float.parseFloat(value.toString());
				count++;
			}
			float avg = new Float(sum / count);
			tMap.put(key.toString(), avg);
		}
	
		//AgeComparator class sorts TreeMap values by average age.
		class AgeComparator implements Comparator<String> 
		{
			TreeMap<String, Float> tmMap;
			public AgeComparator(TreeMap<String, Float> tmTempMap) 
			{
				this.tmMap = tmTempMap;
			}
			public int compare(String a, String b) 
			{
				if (tmMap.get(a) >= tmMap.get(b)) 
				{
					return -1;
				} 
				else 
				{
					return 1;
				}
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException,InterruptedException 
		{
			sortedTMap.putAll(tMap);
			int iCount = 20; //We need top 20 users
			
			for (Entry<String, Float> eEntry : sortedTMap.entrySet()) 
			{
				if (iCount == 0) 
				{
					break;
				}
				
				context.write(new Text(eEntry.getKey()),new Text(String.valueOf(eEntry.getValue())));
				iCount = iCount -1;
			}
		}
	}
	
	//Takes the input from reducer of "Friends1"
	public static class Top20UserAgeMapper extends Mapper<LongWritable, Text, Text, Text> 
	{
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String lineRead = value.toString().trim();
			String[] record = lineRead.split("\t");
			String userId = record[0].trim();
			String avgAge = record[1].trim();
			context.write(new Text(userId), new Text("F1|" + userId + "|" + avgAge));
		}
	}

	//This Mapper class reads the userdata.txt file and emits userid as key and their address as the value
	public static class UserDetailMapper extends Mapper<LongWritable, Text, Text, Text> 
	{
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String userData[] = value.toString().split(",");
			context.write(new Text(userData[0].trim()), new Text("F2|"+ userData[0].trim() + "|" + userData[10].trim()));
		}
	}
	
	//Takes the output from the following mappers and joins them
	// 1.Top20UserAgeMapper
	// 2.UserDetailMapper
	// and emits the address from userdata.txt file and average age of friends for that user	
	public static class Top20UserDetailReducer extends	Reducer<Text, Text, Text, Text> 
	{

		private ArrayList<String> top20Users = new ArrayList<String>();
		private ArrayList<String> UserDetails = new ArrayList<String>();
		private static String splitter = "\\|";

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			for (Text tText : values) 
			{
				String sValue = tText.toString();
				if (sValue.startsWith("F1")) 
				{
					top20Users.add(sValue.substring(3));
				} 
				else 
				{
					UserDetails.add(sValue.substring(3));
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException 
		{
			for (String topBusiness : top20Users) 
			{
				for (String detail : UserDetails) 
				{
					String[] f1Split = topBusiness.split(splitter);
					String f1UserId = f1Split[0].trim();
					String[] f2Split = detail.split(splitter);
					String t2BusinessId = f2Split[0].trim();
					if (f1UserId.equals(t2BusinessId)) 
					{
						context.write(new Text(f1UserId), new Text(f2Split[1] + "\t" + f2Split[2] + "\t"+ f1Split[1]));
						break;
					}
				}
			}
		}
	}
	
	//Program Execution starts here
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration cConfiguration1 = new Configuration();
		String[] argsGiven = new GenericOptionsParser(cConfiguration1, args).getRemainingArgs();

		if (argsGiven.length != 4) 
		{
			System.err.println("Required arguments not provided...Quitting...");
			System.err.println("hadoop jar <jarname> <classname> <input1> <input2> <intermediate_output> <final_output>");
			System.exit(0);
		}
		
		//Getting all users with their ages
		Job job1 = Job.getInstance(cConfiguration1, "JOB1");	
		job1.setJarByClass(Q4.class);
		job1.setMapperClass(UsersBirthDateMapper.class);
		job1.setReducerClass(Top20AverageAge.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(argsGiven[0]));
		FileOutputFormat.setOutputPath(job1, new Path(argsGiven[2]));
		boolean isJob1Completed = job1.waitForCompletion(true);
		if (isJob1Completed == true)
		{
			Configuration cConfiguration2 = new Configuration();
			Job job2 = Job.getInstance(cConfiguration2, "JOB2");
			
			job2.setJarByClass(Q4.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);

			// Set Mappers
			MultipleInputs.addInputPath(job2, new Path(args[2]),TextInputFormat.class, Top20UserAgeMapper.class);
			MultipleInputs.addInputPath(job2, new Path(args[1]),TextInputFormat.class, UserDetailMapper.class);

			// Set Reducer
			job2.setReducerClass(Top20AverageAge.class);
			FileOutputFormat.setOutputPath(job2, new Path(args[3]));

			job2.waitForCompletion(true);
			
		}    
	}
}