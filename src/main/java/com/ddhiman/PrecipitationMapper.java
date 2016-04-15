package com.ddhiman;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PrecipitationMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	private String wban,hours,ppt,fileTag="PT~";
	private  Float pptFloat;

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter  ) throws IOException
    {
       //taking one line/record at a time and parsing them into key value pairs  
        String line = value.toString();
        String splitarray[] = line.split(",");
        wban = splitarray[0].trim();
        hours = splitarray[2].trim();
        ppt = splitarray[3].trim();
  
       // filter out the night records
        if (!(hours.equals("01") ||hours.equals("02")||hours.equals("03")||hours.equals("04")||hours.equals("05")||
        		hours.equals("06")||hours.equals("07")||hours.equals("24") ) ) 
        {
        	
            try {
            	pptFloat = Float.parseFloat(ppt);
            } catch (NumberFormatException  | NullPointerException npe  )
            {
            	pptFloat = new Float(0.0);
            }
            output.collect(new Text(wban), new Text(fileTag+pptFloat));
        	
        }
             
       }
}
