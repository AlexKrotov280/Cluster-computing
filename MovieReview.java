/***  Student ID: 2727809
 *    Subject:Cluster Computing ITNPBD7  **/

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.javafx.collections.MappingChange.Map;


public class MovieReview
{

	// The mapper class
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>
	{		
		// Declare Hashmap on a class level to make it visible from both methods setup and map
		ArrayList<String> stopWords = new ArrayList<String>();
		public void setup(Context context) throws IOException, InterruptedException
		{
			try
			{
			// get a cache file from the path, pointed out in the main method job 
			URI[] cacheFiles = context.getCacheFiles(); 
			String[] fname = cacheFiles[0].toString().split("#");
			// read text from a file
			BufferedReader br = new BufferedReader(new FileReader(fname[1]));		
			String stopWord = null;
			// read line from a text and split it by comma delimiter
			stopWord = br.readLine();
			String [] temparray = stopWord.split(",");
			stopWords.add(",");
			stopWords.add(".");
			// Fill the hashmap stopWords by words from a read file
			for (String tmp: temparray)
			{
				stopWords.add(tmp.toLowerCase());
			}
			br.close();
	        }
			catch (Exception e)
			{
			System.err.println("Problems, problems: " + e);
			}
		}
	    public void map(Object index, Text value, Context context) throws IOException, InterruptedException 
	    {	
	    	// Create a "main" hashmap for word counting
	    	HashMap<String,Integer> map = new HashMap<String, Integer>();
	    	// Split the rating line into two parts, key = rating, value = the rest of the line, text
	    	String [] newlist = String.valueOf(value).split("\\t+",2);
	    	// Call the key and value from the array
	    	String key = newlist[1];
	    	String line = newlist[0].toLowerCase();
	    	// Using Tokenizer, split the value (line) into separate words
	    	StringTokenizer word = new StringTokenizer(line);
	    	// Iterating through each word, fill hashmap by words which are not in the stopWords hashmap
	    	while (word.hasMoreTokens()) {  
					String token = word.nextToken();
					if(!stopWords.contains(token.toLowerCase()))
					{
						if(!map.containsKey(token)) 
						{
							map.put(token, 1);
						}
						else
						{
						map.put(token, map.get(token) + 1);
						}
					} 
	    		  }
 		   /* Iterating through the hashmap with words and its sequence, write the rating(as a key) and 
 		    * merged word + its count into the context to transmit it to reducer
	    	*/
	    	for (HashMap.Entry<String, Integer> entry : map.entrySet()){
	    	      context.write(new Text(key), new Text (entry.getKey() + "#w#" + entry.getValue()));
	    	      } 		
	    	}
	    }
	
	// The Reducer class
	public static class IntSumReducer extends Reducer<Text,Text,Text,Text>
	{	
		// Create hashmap for processing the data for each keys from all mappers
		HashMap<String,Integer> wordMap = new HashMap<String,Integer>();  
		String common = "";
	    int max = 0;
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	    {
            /* As data from mapper comes in form of one line for each key and reducer process one line at a time.
             * We are clearing hashmap and counters every time after finishing work with the hashmap 
             * and transmit the data into reducer context for each key.
             */
	    	max = 0;
            common = null;
            wordMap.clear();
	    	// Iterate through each value into the array of values 
            for (Text val : values)
	    	{
            	// Split each value by delimiter(#w#) into word and its counter.
            	String [] newlist = val.toString().split("#w#",2);
	    		// Check that our word is not in the excluded list of words from the cachfile
            	if(!wordMap.containsKey(newlist[0]))
	    		{
            		wordMap.put(newlist[0], Integer.valueOf(newlist[1])); // Fill the hashmap
	    		}
            	else 
            	{
            		wordMap.put(newlist[0], wordMap.get(newlist[0]) + Integer.valueOf(newlist[1]));
            	}
	    	}
	        // Iterating through the values of the hashmap to find the maximum value and respective key, which is "common"
            wordMap.forEach((key1, value) -> 
	        {
	        	if (value > max)
	            {	               
	               max = value;
	               common = key1;
	            }
	        });
	    	
		   		   context.write(new Text(key), new Text(common));	
	    }
	  }

  //Main program to run
  public static void main(String[] args) throws Exception 
  {		
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Basic Word Count");
    job.setJarByClass(MovieReview.class);
    job.addCacheFile(new URI("/user/akr/count/exclude.txt#first")); 
    
    // Set mapper class to TokenizerMapper defined above
    job.setMapperClass(TokenizerMapper.class);
    
    // Set combine class to IntSumReducer defined above
    job.setCombinerClass(IntSumReducer.class);
    
    // Set reduce class to IntSumReducer defined above
    job.setReducerClass(IntSumReducer.class);
    
    // Class of output key is Text
    job.setOutputKeyClass(Text.class);		
    
    // Class of output value is IntWritable
    job.setOutputValueClass(Text.class);
    
    // Input path is first argument when program called
    FileInputFormat.addInputPath(job, new Path(args[0]));
    
    // Output path is second argument when program called
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    // waitForCompletion submits the job and waits for it to complete, 
    // parameter is verbose. Returns true if job succeeds.
    System.exit(job.waitForCompletion(true) ? 0 : 1);		
  }
}
	