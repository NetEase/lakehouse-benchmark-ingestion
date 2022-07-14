package com.netease.arctic.demo;

import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

public class MainTest {

  public static void main(String args[]) throws DatabaseNotExistException, TableNotExistException, ClassNotFoundException {
    Class.forName("org.apache.hudi.table.HoodieTableFactory");
    System.out.println("test");
  }
}
