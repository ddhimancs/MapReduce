Problem Statement: Create a MapReduce program which calculate amount of rainfall recieved by metropolitan statistical areas (MSAs) in the United States.  The population wetness of an MSA is calculated as the number of people in the MSA times the amount of rain received. For the purposes of this exercise, assume that all people remain inside between the hours of 12 AM and 7 AM local time and so rainfall during these hours does not count.

Source data: Data is distributed in three files:

Station file: Piple delimited file contain mapping from WBAN to MSA name.Here is the sample:
WBAN|WMO|CallSign|ClimateDivisionCode|ClimateDivisionStateCode|ClimateDivisionStationCode|Name|State|Location|Latitude|Longitude|GroundHeight|StationHeight|Barometer|TimeZone
94073||IBM|01|25||KIMBALL|NE|KMBAL MUNI/R E ARRAJ FD AP|41.18944|-103.67083|4894|4926||-6
94074|74534|6728|04|05||NUNN|CO|NUNN 7 NNE|40.8066|-104.7552|5390|5390||-7
94075|74520|232C|04|05||BOULDER|CO|BOULDER 14 W|40.0354|-105.5409|9828|9828||-7
94076||20V||05||KREMMLING|CO|MC ELORY AIRFIELD AIRPORT|40.05361|-106.36889|7411|7411||-7

Prescipitaion File: It is comma delimited file which has WBAN, hour and precipitation for May 2015 only.Here is the sample:
Wban,YearMonthDay,Hour,Precipitation,PrecipitationFlag
00321,20150528,17,0.13, 
00321,20150528,18, , 
00321,20150528,19, , 
00321,20150528,20,0.01, 
00321,20150528,21,0.36, 
00321,20150528,22,0.23, 

MSA_Population: It is pipe delimited (|) file which include the MSA name and the population of that MSA, here is the sample:
Abilene, TX|165252
Akron, OH|703200
Albany, GA|157308
Albany, OR|116672
Abilene, TX|165252
Akron, OH|703200
Albany, GA|157308
Albany, OR|116672
Albany-Schenectady-Troy, NY|870716
Albuquerque, NM|887077
Alexandria, LA|153922

Solution: I am doing a reduce side join with two mapper and one reducer class as follows:
1) Use two different mapper class for both processing the initial input from station file and prescipitation file. The key value output is as follows:

a) Station File
  i) Key(Text): WBAN
  ii) Values (Text): An identifier to indicate the source of input(using ‘ST’ for the station file) + MSA name
  
b) Precipitation File : I am also filtering the hours 12 AM and 7 AM in this mapper.
  i) Key (Text): WBAN
  ii) Value (Text):  An identifier to indicate the source of input(using ‘PT’ for the station file) + precipitation

I’m using StationMapper.java to process Station file and PrecipitationMapper.java to process Precipitation file.
In map reduce API, I’m using MulipleInputFormat to specify which input to go into which mapper. But the ouput key value pairs from the mapper go into the same reducer and I am appending the values ‘ST’ or ‘PT’ to help identify in reducer.

2) In reducer file msaPopReducer.java I am using distributed cache to distribute the msa_population. It parse the file and load into HashMap with Key being the MSA name and value is population. On the reducer every key would be having two values one with prefix ‘ST’ and other ‘PT’. Identify the records and from ST get the MSA name corresponding to the WBAN (input key) and from PT get the prescipitation. 

On obtaining the population do a look up on the HashMap to get the population. Also do the aggregate of all the presipitation for that MSA. So finally the output Key values from the reducer would be as follows
a)      Key : MSA
b)      Value : population*sum of precipitation

4) POM file is attached to build the project

5) To run the project
population file on local drive at: /home/hdfs/msa_population.txt
Move the presipitation file on HDFS at: /incoming/ppt
Move the Station file on HDFS at :/incoming/station
Submit the job like this:

yarn jar /home/hdfs/MapReduceJoin-1.0-SNAPSHOT.jar com.ddhiman.JoinDriver /incoming/station /incoming/ppt /incoming/output 

6) Sample of Output file look locate at /incoming/output:

NEW CASTLE, PA|	110240.68
WILLMAR, MN|	206548.81
BEEVILLE, TX|	89848.016
HEREFORD, TX|	48042.555
WICHITA FALLS, TX|	1405633.0
BOGALUSA, LA|	473095.16
