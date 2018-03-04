/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ch.keybridge.lib.linux.executable;

import java.io.File;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author jesse
 */
public class MySQLImportTest {

  Properties properties = new Properties();

  public MySQLImportTest(String testName) {

    properties.setProperty("mysql.host", "localhost");
    properties.setProperty("mysql.user", "USERNAME");
    properties.setProperty("mysql.pass", "PASSWORD");

    properties.setProperty("mysql.delete", "true");
    properties.setProperty("mysql.force", "true");

//    properties.setProperty("data.filenames", "anten.dat=antenna.dat&ant_fx.dat=antenna_fx.dat&ant_gain.dat=antenna_gain.dat&destina.dat=destination.dat&freq.dat=frequency.dat&nat_serv.dat=nature_of_service.dat&space_sta.dat=space_station.dat&stat_track.dat=status_tracking.dat");
//    properties.setProperty("data.files", "[IBFS.zip]");
//    properties.setProperty("data.host", "ftp://ftp.fcc.gov/pub/Bureaus/International/databases/");
//    properties.setProperty("data.tables", "[address, antenna, antenna_fx, antenna_gain, azimuth, contact, destination, freq_coord, frequency, main, nature_of_service, ptcomm, site, space_station, station, status_tracking]");
//    properties.setProperty("mysql.lines-terminated-by", "\"^|\"");
  }

  public void testVoid() {
    System.out.println("MySQLImportTest disabled - enable to test");
  }

  public void _testParse() {
    String line = "fcc_uls.lo: Records: 21  Deleted: 0  Skipped: 0  Warnings: 326  Time: 6";

    System.out.println("Table  " + line.split(":")[0]);

    Pattern pattern = Pattern.compile("\\s(\\w+): (\\d+)\\s?");
    Matcher matcher = pattern.matcher(line);
    int start = 0;
    while (matcher.find(start)) {

      System.out.println("match " + matcher.group(1) + " / " + matcher.group(2));
      start += matcher.group(0).length();
    }

  }

  public void _testImportCDBS() {
    System.out.println("Test Import CDBS Started at " + new Date());

    properties.setProperty("mysql.database", "fcc_cdbs");
    properties.setProperty("mysql.fields-terminated-by", "'|'");

    MySQLImport mySQLImport = new MySQLImport(properties);

    File dataDir = new File("/tmp/etl/fcc_cdbs/dat");
    for (File file : dataDir.listFiles()) {
      if (file.getName().endsWith(".dat")) {
        try {
          Properties result = mySQLImport.load(file);
//          System.out.println("RESULT for " + file + " - " + result);
        } catch (InterruptedException ex) {
          Logger.getLogger(MySQLImportTest.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
          Logger.getLogger(MySQLImportTest.class.getName()).log(Level.SEVERE, null, ex);
        }
      }
    }
  }

  public void _testImportIBFS() {
    MySQLImport mySQLImport = new MySQLImport(properties);

    File dataDir = new File("/tmp/etl/fcc_ibfs");
    for (File file : dataDir.listFiles()) {
      if (file.getName().endsWith(".dat")) {
        try {
          mySQLImport.load(file);
        } catch (InterruptedException ex) {
          Logger.getLogger(MySQLImportTest.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
          Logger.getLogger(MySQLImportTest.class.getName()).log(Level.SEVERE, null, ex);
        }
      }
    }

  }

}
