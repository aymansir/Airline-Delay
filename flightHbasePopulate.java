
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
import org.apache.hadoop.io.NullWritable;
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
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class flightHbasePopulate {

	public static class TokenizerMapper 
	extends Mapper<Object, Text, NullWritable, NullWritable >{

		Configuration config;
		HTableDescriptor htd;
		int count;
		public void setup(Context context) throws IOException{
			count = 0;
			config = HBaseConfiguration.create();
			HBaseAdmin hba = new HBaseAdmin(config);
			htd = new HTableDescriptor("Flight");
			htd.addFamily(new HColumnDescriptor("AirlineID"));
			htd.addFamily(new HColumnDescriptor("Month"));
			htd.addFamily(new HColumnDescriptor("ArrDelayMinutes"));
			//hba.createTable(htd);
			//System.out.println("Created table");
		}

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			CSVParser csv = new CSVParser();
			String[] details = csv.parseLine(value.toString());
			//if(details[41].equals("0.00") && details[43].equals("0.00")){
				//if(details[0].equals("2008")){
			Configuration cong = HBaseConfiguration.create();
			HTable table = new HTable(config, "Flight");
					//count++;
					//byte[] tableName = htd.getName();
					//HTable table = new HTable(config, tableName);
					//String rowName = "row"+count;
					byte[] row = Bytes.toBytes(details[7]);
					Put p = new Put(Bytes.toBytes(details[7]));
					p.add(Bytes.toBytes("AirlineID"), Bytes.toBytes("col1"),Bytes.toBytes(details[7]));
					p.add(Bytes.toBytes("Month"), Bytes.toBytes("col2"),Bytes.toBytes(details[2]));
					p.add(Bytes.toBytes("ArrDelayMinutes"), Bytes.toBytes("col3"),Bytes.toBytes(details[37]));
					//HBaseAdmin hba = new HBaseAdmin( hc );
					table.put(p);
				//}
			//}
		}
		
		public void cleanup()throws IOException, InterruptedException{
			//tab
		}
	}
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: wordcount <in> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "word count");
	    job.setJarByClass(flightHbasePopulate.class);
	    job.setMapperClass(TokenizerMapper.class);
	    //job.setPartitionerClass(MyPartitioner.class);
	    //job.setCombinerClass(IntSumReducer.class);
	    //job.setReducerClass(IntSumReducer.class);
	    //job.setOutputKeyClass(Text.class);
	    //job.setNumReduceTasks(6);
	    //job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
