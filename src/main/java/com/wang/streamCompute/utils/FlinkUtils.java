package com.wang.streamCompute.utils;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

/**
 * Created by wangyashuai on 2019/12/31.
 */
public class FlinkUtils {
  private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

  /***
   * 创建kafka source
   * @param parameterTool 参数
   * @param schema kafka格式
   * @param <T>
   * @return
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public static <T>  DataStream<T> createKafkaStream(ParameterTool parameterTool,Class<? extends DeserializationSchema>  schema)
    throws IllegalAccessException, InstantiationException {
  Properties props = new Properties();
  props.put("bootstrap.servers", parameterTool.get("bootstrap.servers"));
  props.put("group.id", parameterTool.get("group.id"));
  props.put("enable.auto.commit", parameterTool.get("enable.auto.commit","false"));
  props.put("auto.offset.reset", parameterTool.get("auto.offset.reset","earliest"));

  String topics = parameterTool.getRequired("topics");
  List<String> tpoicLIst = Arrays.asList(topics.split(","));
//  默认5s
  env.enableCheckpointing(parameterTool.getLong("checkpoint.interval",5000), CheckpointingMode.EXACTLY_ONCE);
//  取消任务checkpoint不删除
  env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
//  配置全局参数，其他方法都可以拿到
  env.getConfig().setGlobalJobParameters(parameterTool);
  FlinkKafkaConsumer010<T> kafkaConsumer010 = new FlinkKafkaConsumer010<T>(tpoicLIst, schema.newInstance(), props);
  return env.addSource(kafkaConsumer010);
}

  /**
   * 获取环境
   * @return
   */
  public static StreamExecutionEnvironment getEnv(){
  return  env;
}

}
