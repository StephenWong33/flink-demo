package com.wang.streamCompute.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Created by wangyashuai on 2020/1/1.
 */
public abstract class MyMsqlSink<T> extends RichSinkFunction<T>{

   transient Connection connection= null;
  @Override
  public void open(Configuration parameters) throws Exception {

    String driverName = parameters.getString("mysql.driver.name", "com.mysql.jdbc.Driver");
    String userName = parameters.getString("mysql.user.name", "root");
    String password = parameters.getString("mysql.password", "123456");
    String url = parameters.getString("mysql.jdbc.url", "");
    connection = DriverManager.getConnection(url, userName, password);
  }
  @Override
  public void invoke(T t, Context context) throws Exception {
    PreparedStatement statement = null;
    try {
      doInvoke(t);
    }finally {
      if(statement != null){
        statement.close();
      }
    }
  }
  @Override
  public void close() throws Exception {
    super.close();
    connection.close();
  }

  /***
   * 插入mysql 利用connection 插入数据
   * @param t 数据模型
   */
  abstract void doInvoke(T t);



}
