package invIndexIDF.invIndexIDF;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class QueryEngine
{
	private static Map<String, ArrayList<String>> iIndexMap = new HashMap<String, ArrayList<String>>();
	private static Map<String, ArrayList<String>> iIndexPairMap = new HashMap<String, ArrayList<String>>();
	private static Map<String, String> urlMap = new HashMap<String,String>();	
	private static Map<String, String> pRankMap = new HashMap<String,String>();
	private static int max_result = 8;	
	
	/* read page rank data into an in memory data structure */
	public static void buildPRankList(String filePath) throws IOException
	{
		BufferedReader br = null;
		try
		{
			String line;
			br = new BufferedReader(new FileReader(filePath + "\\pageRank.txt"));

			while ((line = br.readLine()) != null)
			{
				String words[] = line.split(" ");				
				pRankMap.put(words[0], words[1]);				
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			br.close();
		}
	}
	
	/* read URL data into an in memory data structure */
	public static void buildUrlList(String filePath)throws IOException
	{
		BufferedReader br = null;
		try
		{
			String line;
			br = new BufferedReader(new FileReader(filePath + "\\urllist.txt"));

			while ((line = br.readLine()) != null)
			{
				String words[] = line.split(",");				
				urlMap.put(words[0], words[1]);				
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			br.close();
		}
	}
	
	/* read inverted index into an in memory data structure */
	public static void buildInvIndexTable(String filePath) throws IOException
	{
		BufferedReader br = null;
		try
		{
			String line;
			br = new BufferedReader(new FileReader(filePath + "\\iIndex.txt"));

			while ((line = br.readLine()) != null)
			{
				String words[] = line.split("\t");
				String res[] = words[1].split("`");
				ArrayList<String> temp = new ArrayList<String>();
				for(int i=0;i<res.length;++i)
				{					
					String[] entry = res[i].split(":");
					temp.add(entry[0]);					
				}
				iIndexMap.put(words[0], temp);
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			br.close();
		}
	}
	
	/* read inverted index pair into an in memory data structure */
	public static void buildInvIndexPairTable(String filePath) throws IOException
	{
		BufferedReader br = null;
		try
		{
			String line;
			br = new BufferedReader(new FileReader(filePath + "\\iIndexPair.txt"));

			while ((line = br.readLine()) != null)
			{
				String words[] = line.split("\t");
				String res[] = words[1].split("`");
				ArrayList<String> temp = new ArrayList<String>();
				for(int i=0;i<res.length;++i)
				{					
					String[] entry = res[i].split(":");
					temp.add(entry[0]);					
				}
				iIndexPairMap.put(words[0], temp);
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			br.close();
		}
	}
	
	/* compute the intersection between two lists */
	public static LinkedHashSet<String> intersect(LinkedHashSet<String> h1, LinkedHashSet<String> h2)
	{
		LinkedHashSet<String> res = new LinkedHashSet<String>();
		for(String s : h1)
		{
			if(h2.contains(s))
				res.add(s);
			
			if(res.size() == max_result)
				break;
		}
		return res;
	}
	
	/* compute the union between two lists */
	public static LinkedHashSet<String> union(LinkedHashSet<String> h1, LinkedHashSet<String> h2)
	{
		LinkedHashSet<String> res = new LinkedHashSet<String>();
		res.addAll(h1);
		for(String s : h2)
		{			
			res.add(s);
		}
		return res;
	}
	
	/* compute the list such that elements present in left but not right */
	public static LinkedHashSet<String> minus(LinkedHashSet<String> left, LinkedHashSet<String> right)
	{		
		for(String s : right)
		{
			if(left.contains(s))
				left.remove(s);
		}		
		return left;
	}
	
	/* sort the hash map entries by values */
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
	
	private static LinkedHashSet<String> sortMap(LinkedHashSet<String> val, boolean weighted)
	{		
		HashMap<String, Double> myMap = new HashMap<String, Double>();		
		LinkedHashSet<String> res = new LinkedHashSet<String>();
		for(String s : val)
		{
			myMap.put(s, Double.parseDouble(pRankMap.get(s)));			
		}
		Map<String, Double> sortedMap = sortByValues(myMap);
		for(String url : sortedMap.keySet())
		{
			if((res.size() == max_result) && weighted)
				break;
			res.add(url);				
		}
		return res;
	}
	
	public static LinkedHashSet<String> pairResult(String searchLine, boolean weighted)
	{
		String[] str = searchLine.split(" ");
		LinkedHashSet<String> pairList = new LinkedHashSet<String>();		
		
		int count = 0;
		
		for(int i=0;i<str.length;++i)
		{
			for(int j=i+1;j<str.length;++j)
			{
				String pair = str[i] + str[j];
				ArrayList<String> temp = iIndexPairMap.get(pair);
				if(temp != null)
				{
					for(String urlId : temp)
					{
						if(!urlId.isEmpty())
						{
							count++;
							if(count == 4 && weighted)
								break;
							pairList.add(urlMap.get(urlId));
						}
					}
					count = 0;
				}
				else
				{
					pair = str[j] + str[i];
					temp = iIndexPairMap.get(pair);
					if(temp != null)
					{
						for(String urlId : temp)
						{
							if(!urlId.isEmpty())
							{
								count++;
								if(count == 4 && weighted)
									break;
								pairList.add(urlMap.get(urlId));
							}
						}
						count = 0;
					}
				}
			}
		}
		LinkedHashSet<String> res = new LinkedHashSet<String>();		
		
		if(pairList.size() >= 2)
			pairList = sortMap(pairList, weighted);
		
		for(String url : pairList)
		{
			if(res.size() >= max_result && weighted)
				break;
			res.add(url);
		}
		return res;
	}
	
	private static LinkedHashSet<String> unionResult(String searchLine, boolean weighted)
	{
		String[] str = searchLine.split(" ");
		
		LinkedHashSet<String> resUnion = new LinkedHashSet<String>();			
		LinkedHashSet<String> res = new LinkedHashSet<String>();		
			
		for(String search : str)
		{								
			LinkedHashSet<String> temp = new LinkedHashSet<String>();			
			
			if(iIndexMap.containsKey(search))
			{						
				for(String s : iIndexMap.get(search))
				{														
					temp.add(s);					
				}											
			}
			
			//find union of multiple words results
			if(resUnion.isEmpty())
			{
				resUnion.addAll(temp);
			}
			else
			{
				resUnion = union(resUnion, temp);				
			}			
		}		
				
		if(resUnion.isEmpty())
		{
			System.out.println("No results found for the query \"" + searchLine + "\"");
		}
		else
		{
			LinkedHashSet<String> tmp = new LinkedHashSet<String>();
			for(String urlId : resUnion)
			{
				tmp.add(urlMap.get(urlId));				
			}
			res = sortMap(tmp, weighted);			
		}		
		return res;
	}
	
	private static LinkedHashSet<String> handleQuery(String line, boolean weighted)
	{
		LinkedHashSet<String> result = new LinkedHashSet<String>();
		String[] str = line.split(" ");
		
		if(str.length >= 2)
			result = pairResult(line, weighted);
		
		if((result.size() < max_result) || !weighted)
		{
			LinkedHashSet<String> temp = unionResult(line, weighted);
			for(String url : temp)
			{
				result.add(url);
				if(result.size() == max_result && weighted)
					break;
			}
		}		
		return result;
	}
	
	public static void display(LinkedHashSet<String> val)
	{
		for(String s : val)
			System.out.println(s);
	}
	
	public static void main(String[] args) throws IOException
	{
		String filePath = System.getProperty("user.dir");
		buildInvIndexTable(filePath);
		buildInvIndexPairTable(filePath);
		buildUrlList(filePath);
		buildPRankList(filePath);
		try
		{
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));				
						
			while(true)
			{
				System.out.println("Enter the query :");
				String searchLine = br.readLine();
				
				if(searchLine.equals("exit!"))
				{
					System.out.println("Exiting!!!!!!!!");
					System.exit(0);
				}
				LinkedHashSet<String> res = new LinkedHashSet<String>();
				
				//AND operator
				if(searchLine.contains("+"))
				{
					String[] splitLine = searchLine.split("\\+");
					
					for(int i=0;i<splitLine.length;++i)
					{
						if(i == 0)
						{
							res = handleQuery(splitLine[i], false);								
						}
						else
						{
							LinkedHashSet<String> temp = handleQuery(splitLine[i].trim(), false);								
							res = intersect(temp, res);
							display(res);
						}
					}
				}
				
				//NOT operator
				else if(searchLine.contains("-"))
				{
					String[] splitLine = searchLine.split("-");
					
					for(int i=0;i<splitLine.length;++i)
					{
						if(i == 0)
						{
							res = handleQuery(splitLine[i], true);							
						}
						else
						{
							LinkedHashSet<String> temp = handleQuery(splitLine[i], true);							
							res = minus(res, temp);
							display(res);
						}
					}
				}
				
				//OR operator
				else
				{
					String[] temp = searchLine.split(" ");
					if(temp.length == 2)
						res = handleQuery(searchLine, false);
					else
						res = handleQuery(searchLine, true);
					display(res);					
				}
			}				
		}
		catch(IOException io)
		{
			io.printStackTrace();
		}		
	}	
}
