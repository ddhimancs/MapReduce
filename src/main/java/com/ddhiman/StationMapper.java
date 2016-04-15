package com.ddhiman;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class StationMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>

{
	private String wban,msa,fileTag="ST~";

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
    {
       //taking one line/record at a time and parsing them into key value pairs
        String line = value.toString();
        String splitarray[] = line.split("\\|");
        wban = splitarray[0].trim();
        msa = splitarray[6].trim() + ", " + splitarray[7].trim() ;
       
      //sending the key value pair out of mapper
        output.collect(new Text(wban), new Text(fileTag+msa));
     }
}
	