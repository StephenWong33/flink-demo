package com.wang.streamCompute.sink;

import com.wang.streamCompute.bean.MyHbaseBaseBean;
import com.wang.streamCompute.utils.HbaseUtil;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * Created by wangyashuai on 2020/1/2.
 */
public class MyHbaseSink<T> extends RichSinkFunction<MyHbaseBaseBean>  {
  private String tableName =null;
  private String columnFamily;
  private  HbaseUtil hbaseUtil;
  private Admin admin = null;
  private Connection conn = null;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    String rootdir = parameters.getString("hbase.rootdir", "hdfs://172.16.227.101:9000/hbase");
    String quorum = parameters.getString("hbase.zookeeper.quorum", "172.16.227.103:2181");
    String period = parameters.getString("hbase.client.scanner.timeout.period", "600000");
    String timeout = parameters.getString("hbase.rpc.timeout", "600000");
        // 创建hbase配置对象
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir",rootdir);
        //使用eclipse时必须添加这个，否则无法定位
        conf.set("hbase.zookeeper.quorum",quorum);
        conf.set("hbase.client.scanner.timeout.period", period);
        conf.set("hbase.rpc.timeout", timeout);
        conn = ConnectionFactory.createConnection(conf);
        // 得到管理程序
        admin = conn.getAdmin();
        hbaseUtil = new HbaseUtil(admin,conn);

  }


  @Override
  public void invoke( MyHbaseBaseBean byxHbaseBaseBean, Context context) throws Exception {
    String tableName = byxHbaseBaseBean.getTableName();
    String rowKey = byxHbaseBaseBean.getRowKey();
    String columnFamily = byxHbaseBaseBean.getColumnFamily();
    Map<String, String> datamap = byxHbaseBaseBean.getDatamap();
    hbaseUtil.put(tableName, rowKey,columnFamily,datamap);
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (admin != null) {
      admin.close();
    }
    if (conn != null) {
      conn.close();
    }
  }
}
