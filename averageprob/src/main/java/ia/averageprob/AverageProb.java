package ia.averageprob;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * AverageProb will run on a Hadoop Cluster to calculate the average of a list of numbers
 * in a mapreduce approach.
 * 
 * Input file should be a text file with one number per line. 
 * When running the jar provide following arguments in order
 * 1. Input File Location.
 * 2. Output Folder Location.
 * 3. Maximum No of classes the Mapper should be splitted to.(optional, default=10)
 * 
 * @author imantha ahangama
 * 
 */
public class AverageProb {
	private static int noOfClasses = 10;//Used to update the maximum number of split in the mapper class. Value can be modified with a input argument when running the jar.
	/**
	 * Mapper will receive the input text line by line and it will split the numbers in to provided 'noOfClasses' value. This will be passed to the combiner. 
	 * 
	 * @author imantha
	 *
	 */
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
		private Text rand_class = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException, NumberFormatException{
			
			String line = value.toString();
			try {
				if(line!=null || line!="") {
					/*Text file should provide the numbers line by line*/
					Double.valueOf(line);
					rand_class.set("class-"+new Random().nextInt(noOfClasses));
					output.collect(rand_class, new Text(line));
				}
			}catch(NumberFormatException ex ){
				throw new NumberFormatException("Invalid Number(="+line+") in the Input File");
			}
		}
		
	}
	/**
	 * Combine will combine the mapper output which split the numbers to classes and calculate the sum of each class. Then that will be assigned
	 * to the Key 'Average' and passed to the reducer. As the value for key 'Average' the sum and the no of elements contributed to the sum will be provided.
	 * 
	 * @author imantha
	 *
	 */
	public static class Combine extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,	Reporter reporter) throws IOException {
			
			double sum = 0;
			int size = 0;
			
			while (values.hasNext()) {
				/* Each values in the iterator of a class is scanned through and calculates the sum while counting the number of elements within the class */
				sum += Double.parseDouble(values.next().toString());
				size += 1;
			}
			/*Sum per class is assigned to the 'Average' Key and the sum and the no of elements contributed to this sum will be given as the value*/
			output.collect(new Text("Aver"), new Text(sum + " " + size));
		}
	}
	/**
	 * The output from the Combiner is having the key as 'Average' and the value will be the sum and the number of elements contributing to the sum.
	 * Each line of value will be splitted to find the sum and the no of elements contributing. These will be summed seperately and total of the sum 
	 * will be divided by the total of the elements contributing to find the Average.
	 * 
	 * @author imantha
	 *
	 */
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,	Reporter reporter) throws IOException {
			
			String[] valSplit;
			int sum = 0;
			double size = 0;
			
			while (values.hasNext()) {
				valSplit = values.next().toString().split(" ");
				sum 	+= Double.parseDouble(valSplit[0]);
				size 	+= Integer.parseInt(valSplit[1]);
			}
			double avg = sum/size;
			output.collect(new Text("Average"), new Text(avg+""));
			
		}
	}
	/**
	 * Will be creating the mapreduce job. 
	 * As the arguments following should be given in order,
	 * 1. Input File Location.
	 * 2. Output Folder Location.
	 * 3. Maximum No of classes the Mapper should be splitted to.(optional, default = 10)
	 * 
	 * Input file should be a text file with one number per line. 
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(AverageProb.class);
		conf.setJobName("AverageProb");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Combine.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		try {
			noOfClasses = Integer.parseInt(args[2]);	
		}catch(Exception e) {
			noOfClasses = 10;
		}
		
		JobClient.runJob(conf);
	}
}