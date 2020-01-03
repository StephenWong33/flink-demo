package com.wang.streamCompute.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by Administrator on 2018/10/29 0029.
 */
public class HbaseUtil {
    private  Admin admin = null;
    private  Connection conn = null;
//    static{
//        // 创建hbase配置对象
//        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.rootdir","hdfs://172.16.227.101:9000/hbase");
//        //使用eclipse时必须添加这个，否则无法定位
//        conf.set("hbase.zookeeper.quorum","172.16.227.103");
//        conf.set("hbase.client.scanner.timeout.period", "600000");
//        conf.set("hbase.rpc.timeout", "600000");
//        try {
//            conn = ConnectionFactory.createConnection(conf);
//            // 得到管理程序
//            admin = conn.getAdmin();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//    }

        /**
         * 创建表
         */
        public  void createTable(String tabName, String famliyname) throws Exception {
            HTableDescriptor tab = new HTableDescriptor(tabName);
            // 添加列族,每个表至少有一个列族.
            HColumnDescriptor colDesc = new HColumnDescriptor(famliyname);
            tab.addFamily(colDesc);
            // 创建表
            admin.createTable(tab);
            System.out.println("over");
        }

        /**
         * 插入数据，create "baseuserscaninfo","time"
         */
        public  void put(String tablename, String rowkey, String famliyname, Map<String,String> datamap) throws Exception {
            TableName tableName = TableName.valueOf(tablename);
            if(!admin.tableExists(tableName)){
                createTable(tablename,famliyname);
            }
            Table table = conn.getTable(TableName.valueOf(tablename));
            // 将字符串转换成byte[]
            byte[] rowkeybyte = Bytes.toBytes(rowkey);
            Put put = new Put(rowkeybyte);
            if(datamap != null){
                Set<Map.Entry<String,String>> set = datamap.entrySet();
                for(Map.Entry<String,String> entry : set){
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    put.addColumn(
                        Bytes.toBytes(famliyname), Bytes.toBytes(key), Bytes.toBytes(value+""));
                }
            }
            table.put(put);
            table.close();
            System.out.println("ok");
        }

    /**
     * 获取数据，create "baseuserscaninfo","time"
     * create "pindaoinfo","info"
     * create "userinfo","info"
     * create "productinfo","info"
     * create "orderinfo","info"
     */
    public  String getdata(String tablename, String rowkey, String famliyname,String colum) throws Exception {
        Table table = conn.getTable(TableName.valueOf(tablename));
        // 将字符串转换成byte[]
        byte[] rowkeybyte = Bytes.toBytes(rowkey);
        Get get = new Get(rowkeybyte);
        Result result =table.get(get);
        byte[] resultbytes = result.getValue(famliyname.getBytes(),colum.getBytes());
        if(resultbytes == null){
                return null;
        }

        return new String(resultbytes);
    }

    /**
     * 插入数据，create "baseuserscaninfo","time"
     */
    public  void putdata(String tablename, String rowkey, String famliyname,String colum,String data) throws Exception {
        Table table = conn.getTable(TableName.valueOf(tablename));
        Put put = new Put(rowkey.getBytes());
        put.addColumn(famliyname.getBytes(),colum.getBytes(),data.getBytes());
        table.put(put);
    }

    public static void main(String[] args) throws Exception {
//        System.setProperty("hadoop.home.dir","E:\\soft\\hadoop-2.6.0-cdh5.5.1\\hadoop-2.6.0-cdh5.5.1");
//        createTable("hbaseTest","info");
        Map<String, String> date = new HashMap<>();
        date.put("name","wang");
        date.put("age","18");
//        put("hbaseTest","1","info",date);
        System.out.println("");
    }

    public HbaseUtil(Admin admin, Connection conn) {
        this.admin = admin;
        this.conn = conn;
    }

    public HbaseUtil() {
    }
}
