/*
 This assignment is to calculate the average delay of flights 
 */

import java.awt.BufferCapabilities.FlipContents;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.CSVParser;

public class AvgDelay {
	
	//This is for the global counters
	public static enum MATCH_COUNTER {
		  TOTAL_FLIGHTS,
		  TOTAL_SUM
		};
		
		
	public static class MyKey implements WritableComparable<MyKey>{

		private Text midAirport;
		private Text date;
		private Text leg;
		
		public MyKey(){
			set(new Text(), new Text(), new Text());
		}
		
		public MyKey(String midAirport, String date, String leg){
			set(new Text(midAirport), new Text(date), new Text(leg));
		}
		
		public MyKey(Text midAirport, Text date, Text leg){
			set(midAirport, date, leg);
		}

		private void set(Text midAirport, Text date, Text leg) {
			this.midAirport = midAirport;
			this.date= date;
			this.leg = leg;
		}
		
		public Text getMidAirport(){
			return midAirport;
		}

		public Text getDate(){
			return date;
		}
		
		public Text getLeg(){
			return leg;
		}
		
		@Override
		public void readFields(DataInput arg0) throws IOException {
			midAirport.readFields(arg0);
			date.readFields(arg0);
			leg.readFields(arg0);
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			midAirport.write(arg0);
			date.write(arg0);
			leg.write(arg0);
		}

		@Override
		public int compareTo(MyKey mykey) {
			int cmp = midAirport.compareTo(mykey.midAirport);
			if(cmp != 0)
				return cmp;
			cmp = date.compareTo(mykey.date);
			if(cmp != 0)
				return cmp;
			return leg.compareTo(mykey.leg);
		}
		
		public int compareKey(MyKey mykey){
			int cmp = this.midAirport.compareTo(mykey.getMidAirport());
			if(cmp!= 0)
				return cmp;
			cmp = this.date.compareTo(mykey.date);
			if(cmp!=0)
				return cmp;
			return this.leg.compareTo(mykey.leg);
		}
		
		public String toString(){
			return this.getMidAirport().toString()+" "+this.getDate().toString() + " "+this.getLeg().toString();
		}

		
	}
	
	public static class send implements WritableComparable<send>{
		private Text leg;
		private Text arrdelay;
		private Text time;
		
		public send(){
			set(new Text(), new Text(), new Text());
		}
		
		public send(String leg, String arrdelay, String time){
			set(new Text(leg), new Text(arrdelay), new Text(time));
		}
		
		public send(Text leg, Text arrdelay, Text time){
			set(leg, arrdelay, time);
		}

		private void set(Text leg, Text arrdelay , Text time) {
			this.leg = leg;
			this.arrdelay= arrdelay;
			this.time= time;
		}
		
		public Text getArrdelay(){
			return arrdelay;
		}

		public Text getLeg(){
			return leg;
		}
		
		public Text getTime(){
			return time;
		}
		
		@Override
		public void readFields(DataInput arg0) throws IOException {
			leg.readFields(arg0);
			arrdelay.readFields(arg0);
			time.readFields(arg0);
			
		}
		@Override
		public void write(DataOutput arg0) throws IOException {
			leg.write(arg0);
			arrdelay.write(arg0);
			time.write(arg0);
			
			
		}
		@Override
		public int compareTo(send o) {
			int cmp = leg.compareTo(o.leg);
			if(cmp != 0){
				return cmp;
			}
			cmp= arrdelay.compareTo(o.arrdelay);
			if(cmp!=0){
				return cmp;
			}
			return time.compareTo(o.time);
		}
		
		@Override
		public String toString(){
			return this.getArrdelay().toString()+" "+this.getLeg().toString() +" "+this.getTime().toString();
		}
		
	}
	
	
	public static class TokenizerMapper 
    extends Mapper<Object, Text, MyKey, send>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			CSVParser csv = new CSVParser();
			String[] details = csv.parseLine(value.toString());
			if(details[41].equals("0.00")){
				//System.out.println("TRUE");
			}
			 
			// Check for valid flight using the diverted and cancelled field
			boolean validFlight= checkForValidFlight(details); 
			//check for flights between the specified date range
			if((details[0].equals("2007") && Integer.parseInt(details[2]) >= 6) 
					|| ((details[0].equals("2008") && Integer.parseInt(details[2]) <= 5))){
				//check for flights which are not diverted or cancelled
				if(details[41].equals("0.00") && details[43].equals("0.00")){
					//System.out.println("is a valid flight");
					if(details[11].equals("ORD")){ // This is the 1st leg flight
						if(!details[17].equals("JFK")){ // This flight is not going to JFK
							MyKey mykey = new MyKey(details[17], details[5], "leg1");
							//System.out.println(mykey.toString());
							send send1 = new send("leg1", details[37], details[35]);
							//System.out.println("The mykey arguments are:"+ mykey.getAirport().toString());
							context.write(mykey, send1);
						}else{

						}

					}
					else { // this is the 2nd leg flight from intermediate airport 
						if(details[17].equals("JFK") && (!details[11].equals("ORD"))){ // The 2nd leg flight is going to JFK
							// and the origin is not ORD
							//for second leg flight the midairport is the origin of the flight
							MyKey mykey = new MyKey(details[11], details[5], "leg2");

							send send1 = new send("leg2", details[37], details[24]);
							context.write(mykey, send1);
						}
					}

				}
			}
			//System.out.println("Flight date:" +details[5]+ " Origin:"+details[11]+ " destination:"+details[17]+
			//		" arrdelay:"+details[37]+ " arrtime:"+details[35]+ " depttime:"+details[24]+ " diverted:"+details[43]);
		}

		// Checks for valid flight using the diverted and cancelled field
		private boolean checkForValidFlight(String[] details) {
			if((details[41].equals("1") || details[43].equals("1")) && (((details[0].equals("2007")) && details[2].equals("12")) 
																		|| ((details[0].equals("2008")) && details[2].equals("1"))))
				return false;
			return true;
		}
	}
    
    //Depending the records data the partitioner decides which Reduce Task it supposed to go.
    public static class MyPartitioner extends Partitioner<MyKey, send>{

    	//decides which reducer task each key value pair should go to
		@SuppressWarnings("deprecation")
		@Override
		public int getPartition(MyKey mykey, send value, int numOfPartitions) {
			
			DateFormat formatter= null;
			formatter = new SimpleDateFormat("YYYY-MM-dd");
			Date flightDate = null;
			try {
				flightDate = (Date) formatter.parse(mykey.getDate().toString());
			} catch (ParseException e) {
				e.printStackTrace();
			}
			//System.out.println("The date:" + flightDate.toString()+" is going to:"+ (int) ((flightDate.getTime() * 127) % numOfPartitions)+" reducer");
			// --FiXME change the time to a particular date
			return  ((flightDate.getDate() * 127) % numOfPartitions);
		}
    	
    }
    
    //Key comparator is used at each reduce task for sorting all the records it recieves
    public static class KeyComparator extends WritableComparator{

		protected KeyComparator() {
			super(MyKey.class,true);
		}
		
		public int compare(WritableComparable w1, WritableComparable w2){
			MyKey mykey1= (MyKey) w1; MyKey mykey2= (MyKey) w2;
			int cmp = mykey1.compareKey(mykey2);
			return cmp;
		}
    	
    }
    
    //Group comparator is used at each reduce task to group the sorted records to be sent
    //to a particular reduce call
    public static class GroupComparator extends WritableComparator{

		protected GroupComparator() {
			super(MyKey.class,true);
		}
		
		public int compare(WritableComparable w1, WritableComparable w2){
			MyKey mykey1 = (MyKey) w1; MyKey mykey2 = (MyKey) w2;
			int cmp = mykey1.getMidAirport().compareTo(mykey2.getMidAirport());
			if(cmp!=0)
				return cmp;
			return mykey1.getDate().compareTo(mykey2.getDate());
		}
    	
    }
	
	public static class DelayReducer 
	extends Reducer<MyKey,send,MyKey,send> {
		private IntWritable result = new IntWritable();

		public void reduce(MyKey key, Iterable<send> values, 
				Context context
				) throws IOException, InterruptedException {
			int count = 0;
			//list to maintain 1st leg's flight information
			List<send> valueList1 = new ArrayList<send>();
			//list to maintain 2nd leg's flight information
			List<send> valueList2 = new ArrayList<send>();
			for(send val1:values){
				//check if the record is 1st leg
				if(val1.getLeg().toString().equals("leg1")){
					send s = new send(val1.getLeg().toString(), val1.getArrdelay().toString(), val1.getTime().toString());
					valueList1.add(s);
				}else {
					send s = new send(val1.getLeg().toString(), val1.getArrdelay().toString(), val1.getTime().toString());
					valueList2.add(s);
				}
			}
			
			for(int i=0; i<valueList1.size(); i++){
				for(int j=0; j< valueList2.size(); j++){
					if(Float.parseFloat(valueList1.get(i).getTime().toString()) 
							< Float.parseFloat(valueList2.get(j).getTime().toString())){
						
						int delay1 = (int) Float.parseFloat(valueList1.get(i).getArrdelay().toString());
						int delay2 = (int) Float.parseFloat(valueList2.get(j).getArrdelay().toString());
						int temp = delay1 +delay2;
						context.getCounter(MATCH_COUNTER.TOTAL_FLIGHTS).increment(1);
						context.getCounter(MATCH_COUNTER.TOTAL_SUM).increment(temp);
						//System.out.print("for value1:"+valueList1.get(i).toString()+" "+"for value2:"+valueList2.toString());
						//System.out.println("delay is:"+(Float.parseFloat(valueList1.get(i).getArrdelay().toString()) 
							//+ Float.parseFloat(valueList2.get(j).getArrdelay().toString())) );
					}
				}
			}
			
			/*for (send val2 : valueList1) {
				//System.out.println("The key is:" +key.toString() +" "+ "and the delay is:"+val2.arrdelay);
				System.out.println("The value is:"+val2.toString());
				context.write(key, val2);
			}*/
		}
	}


	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	        System.err.println("Usage: wordcount <in> <out>");
	        System.exit(2);
	      }
	    Job job = new Job(conf, "AVGDelay");
	    job.setJarByClass(AvgDelay.class);
	    
	    job.setPartitionerClass(MyPartitioner.class);
	    job.setGroupingComparatorClass(GroupComparator.class);
	    job.setSortComparatorClass(KeyComparator.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setReducerClass(DelayReducer.class);
	    job.setOutputKeyClass(MyKey.class);
	    job.setOutputValueClass(send.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    job.setNumReduceTasks(10);
	    //System.exit(
	    job.waitForCompletion(true);
	    Counters counters = job.getCounters();
	    Counter c1 = counters.findCounter(MATCH_COUNTER.TOTAL_FLIGHTS);
	    System.out.println(c1.getDisplayName()+":"+c1.getValue());
	    Counter c2 = counters.findCounter(MATCH_COUNTER.TOTAL_SUM);
	    System.out.println(c2.getDisplayName()+":"+c2.getValue());
	    Log log = LogFactory.getLog(AvgDelay.class);
	    log.info("AVERAGE DELAY:" +(float) ((float) c2.getValue()/c1.getValue()));
	    
	    
	}
}
