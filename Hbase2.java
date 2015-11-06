
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.hbase.mapred.TableOutputFormat;

public class Hbase2 {
	
	public static class MyKey implements WritableComparable<MyKey>{

		Text airlineID;
		Text month;
		
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

	public static class TokenizerMapper extends 
	TableMapper<MyKey, FloatWritable>{
		
		public static final byte[] CF = "Flightdata".getBytes();
		public static final byte[] Cancelled = "Cancelled".getBytes();
		public static final byte[] Diverted = "Diverted".getBytes();
		public static final byte[] ArrDelayMinutes = "ArrDelayMinutes".getBytes();

		private final IntWritable ONE = new IntWritable(1);
	   	private Text text = new Text();
	   	
		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException{
			 String key = new String(value.getRow());
			 String[] details = key.split("-");
			 String origin = details[0];
			 String AirlineID = details[1];
			 String FlightNum = details[2];
			 String Year = details[3];
			 String Month = details[4];
			 String diverted = new String(value.getValue(CF, Diverted));
			 String cancelled = new String(value.getValue(CF, Cancelled));
			 String arrdelay = new String(value.getValue(CF, ArrDelayMinutes));
			 if(diverted.equals("0.00") && cancelled.equals("0.00")){
					if(Year.equals("2008")){
						MyKey mykey = new MyKey(AirlineID, Month);
						float ArrDelayMinutes = Float.parseFloat(arrdelay);
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
			
			StringBuilder outValue = new StringBuilder();
			for (Map.Entry<Integer, Integer> entry:h.entrySet()){
				outValue.append(",("+ entry.getKey()+","+entry.getValue()+")");
			}
			context.write(key.airlineID, new Text(outValue.toString()));
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config, "Read Hbase");
		job.setJarByClass(Hbase2.class);
		
		//For Hbase Scan
		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		
		//setting other scan attributes
		TableMapReduceUtil.initTableMapperJob(
				"Flight",  // input Hbase table name
				scan, // Scan instance to control CF and attribute selection 
				TokenizerMapper.class, 
				MyKey.class, 
				FloatWritable.class,
				job);
		job.setPartitionerClass(MyPartitioner.class);
	    job.setGroupingComparatorClass(GroupComparator.class);
	    job.setSortComparatorClass(KeyComparator.class);
	    job.setReducerClass(DelayReducer.class);
	    job.setOutputKeyClass(MyKey.class);
	    job.setOutputValueClass(FloatWritable.class);
	    //FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[0]));
	    job.setNumReduceTasks(10);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	}
	
}
