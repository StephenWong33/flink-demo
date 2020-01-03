package com.wang.streamCompute.join;

import com.alibaba.druid.pool.DruidDataSource;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

/**
 * 使用连接池 异步请求数据库 提升并发速度
 * Created by wangyashuai on 2019/12/31.
 */
public abstract class AsyncByxMqlJoin<T> extends RichAsyncFunction<String,T> {

   transient DruidDataSource dataSource;
   transient ExecutorService executorService;
  @Override
  public void open(Configuration parameters) throws Exception {
    String driverName = parameters.getString("mysql.driver.name", "com.mysql.jdbc.Driver");
    String userName = parameters.getString("mysql.user.name", "root");
    String password = parameters.getString("mysql.password", "123456");
    String url = parameters.getString("mysql.jdbc.url", "");
    executorService = Executors.newFixedThreadPool(10);
    dataSource  = new DruidDataSource();
    dataSource.setDriverClassName(driverName);
    dataSource.setUsername(userName);
    dataSource.setPassword(password);
    dataSource.setUrl(url);
    dataSource.setInitialSize(3);
    dataSource.setMinIdle(5);
    dataSource.setMaxActive(10);
  }

  @Override
  public void asyncInvoke(String line, ResultFuture<T> resultFuture) throws Exception {
    Future<String> submit = executorService.submit(() -> {
      return asyncQuery(line);
    });
    CompletableFuture.supplyAsync(new Supplier<String>() {
      @Override
      public String get() {
        try {
          return  submit.get();
        } catch (Exception e) {
          e.printStackTrace();
          return  null;
        }
      }
    }).thenAccept((String result)->{
      T t = returnResult(line,result);
      resultFuture.complete(Collections.singleton(t));
    });
  }

  @Override
  public void close() throws Exception {
    super.close();
    dataSource.close();
    executorService.shutdown();
  }

  /**
   *  异步查询数据库
   * @param line kafka数据
   * @return 数据库查询结果
   */
  abstract String asyncQuery(String line);

  /***
   * 返回T的实例
   * @param line kafka数据
   * @param result 数据库返回结果
   * @return map返回结果
   */
  abstract T returnResult(String line ,String result);


}
