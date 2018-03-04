package com.cloudxlab.maxtemp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.HashMap;

public class StubMapper extends Mapper<Object, Text, Text, LongWritable> {

  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {

	  //Split the line into words with spaces or tabs as separators
	  //String[] words = value.toString().split("[ \t]+");
      String[] records = value.toString().split("\\r?\\n");
      
	  HashMap<String, Integer> temp = new HashMap<String, Integer>();
		 
		for(String record:records)
		{
			
		String[] fields = record.toString().split(",");

		for(String field:fields)
		{

		  String mkey = fields[1].trim() + ',' + fields[2].trim();
		  Integer mvalue = Integer.parseInt(fields[0].trim());


		  if (!temp.containsKey(mkey)) 
				{
				temp.put(mkey, mvalue);
				}
		  else
				{
				if(temp.get(mkey) < mvalue) temp.put(mkey,mvalue);
				}
		  }
		}

	 
	  for(String k : temp.keySet())
	  {
		Text outKey = new Text(k);
		LongWritable outValue = new LongWritable(temp.get(k));
       
		context.write(outKey, outValue);
	  } 
   
  }
}
