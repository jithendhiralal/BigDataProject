package invIndexIDF.invIndexIDF;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvIndexIDFPair extends Configured implements Tool
{	
	/* Mapper */	
	public static class InvIndexMap extends Mapper<LongWritable, Text, Text, Tuple>
	{
		private Map<String, Map<String, Integer>> cMap = new HashMap<String, Map<String, Integer>>();		
		Set<String> stopWords = new HashSet<String>();
		
		protected void setup(Context context) throws IOException
		{
			Configuration conf = context.getConfiguration();
            String stp_file_name = conf.get("stop_file");
			Path pt=new Path("hdfs:" + stp_file_name);
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br;
			try
            {
				br=new BufferedReader(new InputStreamReader(fs.open(pt)));                
            } 
            catch (FileNotFoundException e)
            {
                e.printStackTrace();
                throw new RuntimeException("Could not open stopwords file ",e);
            }
            String word;
            try 
            {
                while((word =br.readLine()) != null)
                {
                    stopWords.add(word);
                }
            }
            catch (IOException e)
            {
                e.printStackTrace();                
                throw new RuntimeException("error while reading stopwords",e);
            }
            br.close();
        }
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{			
			//from business
			if(!value.toString().isEmpty())
			{
				String tokens = "[_|~%$&#+<>\\^=-\\[\\]\\*/\\\\,;,.\\-:{}()?!\"`']";
				String tok = "\\s+";
				String cleanLine = value.toString().toLowerCase().replaceAll(tokens, " ");
				cleanLine = cleanLine.replaceAll(tok, " ");
				String[] words = StringUtils.split(cleanLine.toString()," ");	
				
				FileSplit fileSplit = (FileSplit) context.getInputSplit();
				String fileName = fileSplit.getPath().getName();
				
				for(int i=0;i<words.length;++i)
				{
					String word = words[i].trim();					
					
					Matcher m = Pattern.compile("\\d+").matcher(word);
					
					if(stopWords.contains(word) || m.find() || word.length() <= 2)
						continue;
					
					for(int j=i+1;j<words.length;++j)
					{
						String word2 = words[j].trim();
						if(word2.equals(word))
							continue;
						
						m = Pattern.compile("\\d+").matcher(word2);
						
						if(stopWords.contains(word2) || m.find() || word2.length() <= 2)
							continue;
						
						String pair = word + word2;
						
						//<fileName, wordCount>
						Map<String, Integer> temp = new HashMap<String, Integer>();					
						if(cMap.containsKey(pair))
						{						
							temp = cMap.get(pair);
							if(temp.containsKey(fileName))
							{
								temp.put(fileName, temp.get(fileName)+1);
								cMap.put(pair, temp);
							}
							else
							{
								temp.put(fileName, 1);
								cMap.put(pair, temp);
							}
						}
						else
						{
							temp.put(fileName, 1);
							cMap.put(pair, temp);
						}
					}
				}				
			}
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException
        {
			for (String word : cMap.keySet())
            {
				//<fileName, wordCount>
            	Map<String, Integer> temp = new HashMap<String, Integer>();
            	temp = cMap.get(word);
            	for(String fName : temp.keySet())
            	{
            		context.write(new Text(word), new Tuple(fName, temp.get(fName)));
            	}
            }
        }
	}

	/* Reducer */
	public static class Reduce extends Reducer<Text,Tuple,Text,Text>
	{
		public static int numOfDoc = 0;
		protected void setup(Context context) throws IOException
		{
			Configuration conf = context.getConfiguration();
			numOfDoc = Integer.parseInt(conf.get("numberOfDoc"));			
		}
		
		public void reduce(Text key, Iterable<Tuple> values,Context context ) throws IOException, InterruptedException
		{
			Map<String, Long> rMap = new HashMap<String, Long>();
			HashSet<String> hs = new HashSet<String>();
			
			for(Tuple t : values)
			{
				String fName = t.getFileID();
				if(rMap.containsKey(fName))
				{
					rMap.put(fName, rMap.get(fName) + t.getCount());
				}
				else
				{
					rMap.put(fName, t.getCount().longValue());
					hs.add(fName);
				}
			}
			
			double normalization = Math.log10(numOfDoc / hs.size());//idf
			if(normalization > 0)
			{
				for(String fName : rMap.keySet())
				{
					double val = rMap.get(fName) * normalization;
					rMap.put(fName, Math.round(val));
				}
			
			
				Map<String, Long> sortedMap = sortByValues(rMap);
				StringBuilder str = new StringBuilder();
				int count = 0;
				for (String fName : sortedMap.keySet())
				{
					str.append(fName + ":" + sortedMap.get(fName) + "`");
					count++;
					if(count == 15)
						break;
				}
				
				String val = str.substring(0, str.length()-1);
				context.write(key, new Text(val));
			}			
		}		
	}
	
	@SuppressWarnings("rawtypes")
	private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

            @SuppressWarnings("unchecked")
            
            //decreasing order
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        //LinkedHashMap will keep the keys in the order they are inserted
        //which is currently sorted on natural ordering
        Map<K, V> sortedMap = new LinkedHashMap<K, V>();

        for (Map.Entry<K, V> entry : entries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }
	
	// Driver program
	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new InvIndexIDFPair(), args);
		System.exit(res);		
		//hadoop jar InvIndexIDF-0.0.1-SNAPSHOT.jar invIndexIDF.invIndexIDF.InvIndexIDFPair -D numberOfDoc="445" -D stop_file="/user/ass150430/stop-word-list.txt" /user/ass150430/webText /user/ass150430/iIndexPair
	}

	public int run(String[] args) throws Exception 
	{
		Configuration conf = this.getConf();
			
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();// get all args
		if (otherArgs.length < 2)
		{
			System.err.println("Usage: jar <in> <out>");
			System.exit(2);
		}
				  
		Job job = Job.getInstance(conf, "InvIndexIDFPair");		
		job.setJarByClass(InvIndexIDFPair.class);
		   
		job.setMapperClass(InvIndexMap.class);
		job.setReducerClass(Reduce.class);
			
		// set output key type
		job.setMapOutputKeyClass(Text.class);
		job.setOutputKeyClass(Text.class);		
			
		// set output value type
		job.setMapOutputValueClass(Tuple.class);
		job.setOutputValueClass(Text.class);		
					
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));		
			
		//Wait till job completion
		return job.waitForCompletion(true) ? 0 : 1;
	}
}