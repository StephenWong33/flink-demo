package com.wang.streamCompute.bean;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by wangyashuai on 2020/1/3.
 */
public class MyHbaseBaseBean implements Serializable {

  private String rowKey;
  private String tableName;
  private String columnFamily;
  private  Map<String,String> datamap = new HashMap<>();

  public MyHbaseBaseBean(String rowKey, String tableName, String columnFamily,
      Map<String, String> datamap) {
    this.rowKey = rowKey;
    this.tableName = tableName;
    this.columnFamily = columnFamily;
    this.datamap = datamap;
  }

  public MyHbaseBaseBean() {
  }

  public String getRowKey() {
    return rowKey;
  }

  public void setRowKey(String rowKey) {
    this.rowKey = rowKey;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getColumnFamily() {
    return columnFamily;
  }

  public void setColumnFamily(String columnFamily) {
    this.columnFamily = columnFamily;
  }

  public Map<String, String> getDatamap() {
    return datamap;
  }

  public void setDatamap(Map<String, String> datamap) {
    this.datamap = datamap;
  }
}
