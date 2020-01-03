package com.wang.streamCompute.sink;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

/**
 * Created by wangyashuai on 2020/1/1.
 */
public class MyRedisSink extends RichSinkFunction<Tuple3<String,String,String>> {

  private transient Jedis jedis= null;
  @Override
  public void open(Configuration parameters) throws Exception {
    String host = parameters.getString("redis.hosts", "localhost");
    String password = parameters.getString("redis.pwd","");
    Integer db = parameters.getInteger("redis.db", 0);
    Integer port = parameters.getInteger("redis.port", 6379);
    jedis = new Jedis(host, port, 5000);
    jedis.select(db);
    if(password !=""){
      jedis.auth(password);
    }
  }

  @Override
  public void invoke(Tuple3<String, String, String> value, Context context) throws Exception {
    if(!jedis .isConnected()){
      jedis.connect();
    }
    jedis.hset(value.f0,value.f1,value.f2);
  }

  @Override
  public void close() throws Exception {
    super.close();
    jedis.close();
  }
}
