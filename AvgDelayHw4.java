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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

public class AvgDelayHw4 {

	//This is my custom key
	public static class MyKey implements WritableComparable<MyKey>{

		Text airlineID; //information about the airlineID from the data.csv
		Text month;     //Information about the month of flight

		public MyKey(){
			set(new Text(), new Text());
		}
		
		public MyKey(String airlineID, String month){
			set(new Text(airlineID), new Text(month));
		}

		public MyKey(Text airlineID, Text month){
			set(airlineID, month);
		}

		public void set(Text airlineID, Text month){
			this.airlineID = airlineID;
			this.month = month;
		}
		public Text getAirlineID() {
			return airlineID;
		}

		public void setAirlineID(Text airlineID) {
			this.airlineID = airlineID;
		}

		public Text getMonth() {
			return month;
		}

		public void setMonth(Text month) {
			this.month = month;
		}

		@Override
		public void readFields(DataInput arg0) throws IOException {
			airlineID.readFields(arg0);
			month.readFields(arg0);

		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			airlineID.write(arg0);
			month.write(arg0);

		}

		@Override
		public int compareTo(MyKey mykey) {
			int cmp = this.airlineID.compareTo(mykey.airlineID);
			if(cmp != 0){
				return cmp;
			}
			return this.month.compareTo(mykey.month);
		}


	}


	//Mapper class
	public static class TokenizerMapper 
	extends Mapper<Object, Text, MyKey, FloatWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		//Map function
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			CSVParser csv = new CSVParser();
			String[] details = csv.parseLine(value.toString());
			//check for flights which are not diverted or cancelled
			if(details[41].equals("0.00") && details[43].equals("0.00")){
				if(details[0].equals("2008")){
					MyKey mykey = new MyKey(details[7], details[2]);
					float ArrDelayMinutes = Float.parseFloat(details[37]);
					context.write(mykey, new FloatWritable(ArrDelayMinutes));
				}
			}
		}
	}

	//Depending the records data the partitioner decides which Reduce Task it supposed to go.
	public static class MyPartitioner extends Partitioner<MyKey, FloatWritable>{

		//decides which reducer task each key value pair should go to
		@Override
		public int getPartition(MyKey mykey, FloatWritable value, int numOfPartitions) {

			return  ((mykey.airlineID.hashCode()) % numOfPartitions);
		}

	}

	//Key comparator is used at each reduce task for sorting all the records it recieves
	public static class KeyComparator extends WritableComparator{

		protected KeyComparator() {
			super(MyKey.class,true);
		}


		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable w1, WritableComparable w2){
			MyKey mykey1= (MyKey) w1; MyKey mykey2= (MyKey) w2;
			int cmp = mykey1.compareTo(mykey2);
			return cmp;
		}

	}

	//Group comparator is used at each reduce task to group the sorted records to be sent
	//to a particular reduce call
	public static class GroupComparator extends WritableComparator{

		protected GroupComparator() {
			super(MyKey.class,true);
		}

		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable w1, WritableComparable w2){
			MyKey mykey1= (MyKey) w1; MyKey mykey2= (MyKey) w2;
			return mykey1.getAirlineID().compareTo(mykey2.getAirlineID());
		}

	}

	public static class DelayReducer 
	extends Reducer<MyKey,FloatWritable,Text,Text> {
		@SuppressWarnings("unused")
		private IntWritable result = new IntWritable();

		//Reduce call. values received here are from a particular flight and a particular month
		public void reduce(MyKey key, Iterable<FloatWritable> values, 
				Context context
				) throws IOException, InterruptedException {
			Map<Integer, Integer> h = new HashMap<Integer, Integer>();
			int count = 0;
			float total_delay = 0;
			Integer current_Month = Integer.parseInt(key.getMonth().toString());
			for (FloatWritable v:values){
				if(Integer.parseInt(key.getMonth().toString()) != current_Month){
					Integer avg_delay =(int) Math.ceil( total_delay/count);
					h.put(current_Month, avg_delay);
					current_Month = Integer.parseInt(key.getMonth().toString());
					count = 0;
					total_delay =0;
				}else{
					total_delay += Float.parseFloat(v.toString());
					count++;
				}
			}

			h.put(current_Month, (int) Math.ceil(total_delay/count));

			if(h.size()!=12){
				for (int i = 1; i<=12 ; i++){
					if(!h.containsKey(i)){
						h.put(i, 0);
					}
				}
			}
			StringBuilder outValue = new StringBuilder();
			for (Map.Entry<Integer, Integer> entry:h.entrySet()){
				outValue.append(",("+ entry.getKey()+","+entry.getValue()+")");
			}
			context.write(key.airlineID, new Text(outValue.toString()));
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
		job.setJarByClass(AvgDelayHw4.class);

		job.setPartitionerClass(MyPartitioner.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(DelayReducer.class);
		job.setOutputKeyClass(MyKey.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		job.setNumReduceTasks(10);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
