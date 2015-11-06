
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
import org.apache.hadoop.io.Writable;
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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
//import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;

public class Hbase1 {

	public static class TokenizerMapper extends 
				Mapper<Object, Text, ImmutableBytesWritable,Writable>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			CSVParser parser = new CSVParser();
			String[] details = parser.parseLine(value.toString());
			String AirlineID = details[7];
			String Origin = details[11];
			String FlightDate = details[5];
			String FlightNum = details[10];
			String ArrDelayMinutes = details[37];
			String Cancelled = details[41];
			String Diverted = details[43];
			String Month = details[2];
			String flightKey = AirlineID +"-"+FlightDate+"-"+ FlightNum +"-"+ Origin;
			Put put = new Put(flightKey.getBytes());
			put.add("Flightdata".getBytes(), "Cancelled".getBytes(), Cancelled.getBytes());
			put.add("Flightdata".getBytes(),"Diverted".getBytes(), Diverted.getBytes());
			put.add("Flightdata".getBytes(),"ArrDelayMinutes".getBytes(), ArrDelayMinutes.getBytes());
			context.write(new ImmutableBytesWritable(flightKey.getBytes()), put);
		}
	}
	
	public void CreateHbaseTable() throws Exception{
		Configuration config = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(config);
		HTableDescriptor htd = new HTableDescriptor("Flight");
		htd.addFamily(new HColumnDescriptor("Flightdata"));
		admin.createTable(htd);
		admin.close();
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
		      System.err.println("Usage: wordcount <in> <out>");
		      System.exit(2);
		}
		Job job = new Job(conf, "Hbase populate");
		Hbase1 hb = new Hbase1();
		
		//hb.CreateHbaseTable();
		HBaseConfiguration hc = new HBaseConfiguration(new Configuration());
		HBaseAdmin admin = new HBaseAdmin(hc);
		HTableDescriptor htd = new HTableDescriptor("Flight");
		htd.addFamily(new HColumnDescriptor("Flightdata"));
		admin.createTable(htd);
		admin.close();
		
	    job.setJarByClass(Hbase1.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setOutputFormatClass(TableOutputFormat.class);
	    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "Flight");
	    job.setOutputKeyClass(ImmutableBytesWritable.class);
	    job.setOutputValueClass(Put.class);
	    job.setNumReduceTasks(0);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
