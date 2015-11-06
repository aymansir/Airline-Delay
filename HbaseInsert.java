
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseInsert {

	public static void main(String[] args) throws IOException{
		Configuration config = HBaseConfiguration.create();
		HTable ht = new HTable(config, "User");
		Put p = new Put(Bytes.toBytes("row1"));
		p.add(Bytes.toBytes("ID"), Bytes.toBytesBinary("col1"),	Bytes.toBytes("Emp1"));
		p.add(Bytes.toBytes("Name"), Bytes.toBytes("col2"), Bytes.toBytes("prasad"));
		ht.put(p);
	}
}
