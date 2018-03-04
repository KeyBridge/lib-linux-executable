/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ch.keybridge.lib.linux.executable;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author jesse
 */
public class MySQLTest {

  public MySQLTest(String testName) {

    properties.setProperty("mysql.host", "localhost");
    properties.setProperty("mysql.user", "USERNAME");
    properties.setProperty("mysql.pass", "PASSWORD");

  }
  private Properties properties = new Properties();

  public void _testDateFormat() {
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.DAY_OF_YEAR, -1);
    String last_action_date = new DecimalFormat("00").format(calendar.get(Calendar.MONTH) + 1)
      + "/"
      + new DecimalFormat("00").format(calendar.get(Calendar.DAY_OF_MONTH))
      + "/"
      + calendar.get(Calendar.YEAR);

    System.out.println("dow " + calendar.get(Calendar.DAY_OF_MONTH));

    System.out.println("last action date " + last_action_date);
  }

  public void _testShowTables() throws Exception {

    properties.setProperty("mysql.database", "fcc_cdbs");

    MySQL mySQL = MySQL.getInstance(properties);

    String result = mySQL.select("show tables");
    System.out.println("RESULT");
    System.out.println(result);
    List<String> strings = Arrays.asList(result.split("\n"));
    System.out.println(strings);

  }

  public void _testSomeMethod() throws Exception {
    MySQL update = new MySQL();
    update.setProperty("mysql.host", "localhost");
    update.setProperty("mysql.database", "ic_bdbs");
    update.setProperty("mysql.user", "USERNAME");
    update.setProperty("mysql.pass", "PASSWORD");
    update.execute("UPDATE apatstat SET banner    = TRIM(SUBSTRING(calls_banr, -2, 2));");

  }

}
