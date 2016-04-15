package com.ddhiman;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class msaPopReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	
	//Variables to aid the join process
    private String station,presipitation ,population,totalppt;
    private Float presipitationFloat  ;
    private int populationInt;
    //public Float pptFloatOut = (float) 0.0;
    
    private static Map<String,String> msaPopMap= new HashMap<String,String>();
    
    public void configure(JobConf job)
    {
           //To load the MSA  and population into a hash map
           loadMsaPopulation();      
    }
    
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
    {
    	Float pptFloatOut = (float) 0.0;
    	while (values.hasNext())
        {
             String currValue = values.next().toString();
             String valueSplitted[] = currValue.split("~");
             /*identifying the record source that corresponds to a Station
             and parses the values accordingly*/
             if(valueSplitted[0].equals("ST"))
             {
            	 station=valueSplitted[1].trim();
            	 population = msaPopMap.get(station);
            	 
             }
             else if(valueSplitted[0].equals("PT"))
             {
              //getting the precipitation and multiple by population
            	 presipitation = valueSplitted[1].trim();
            	 if (presipitation != null) {
            		 presipitationFloat =Float.parseFloat(presipitation);
            		 pptFloatOut =  pptFloatOut + presipitationFloat ;
            		 
            	 }
            	            	 
             }
                     }
        
        //pump final output to file
        if (station!=null  && population!=null && presipitation!=null)
   	 	{
   		 try{
              	 populationInt = Integer.parseInt(population);}
              	 catch (NumberFormatException | NullPointerException ex) {
              		 populationInt = 0;
              	 }
   		 totalppt =  String.valueOf(populationInt*pptFloatOut);
   		 output.collect(new Text(station + "|") , new Text(totalppt) );
   		 
   	 }
    }
    
    
    //To load the Delivery Codes and Messages into a hash map
 private void loadMsaPopulation()
 {
    String strRead;
    try {
           //read file from Distributed Cache
                  BufferedReader reader = new BufferedReader(new FileReader("/home/hdfs/msa_population.txt"));
                  while ((strRead=reader.readLine() ) != null)
                  {
                        String splitarray[] = strRead.split("\\|");
                        //parse record and load into HahMap
                        msaPopMap.put(splitarray[0].trim().toUpperCase(),  splitarray[1].trim());
                  } 
                  reader.close();
           }
           catch (FileNotFoundException e) {
           e.printStackTrace();
           }catch( IOException e ) {
                    e.printStackTrace();
             } 
    
    }

}

