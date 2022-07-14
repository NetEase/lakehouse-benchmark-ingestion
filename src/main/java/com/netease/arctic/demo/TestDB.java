package com.netease.arctic.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class TestDB {

  public static void main (String[] args) {
    try {
      Class.forName("com.mysql.cj.jdbc.Driver");
      String url = "jdbc:mysql://10.171.161.168:3306/test1";
      Connection conn = DriverManager.getConnection(url, "sys", "netease");
      int max = 1;
      conn.createStatement().execute("truncate test1.t1");
      for (; ; ) {
        if (max++ % 1000 == 0) {
          System.out.println("insert " + max + " records");
        }
        PreparedStatement pstmt = conn.prepareStatement("INSERT INTO test1.t1 " +
                "VALUES (?, 'mj')");
        pstmt.setInt(1, max);
        pstmt.execute();
        pstmt.close();
        Thread.sleep(500);
      }
    } catch (Exception throwables) {
      throwables.printStackTrace();
    }

  }
}
