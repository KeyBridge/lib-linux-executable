/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ch.keybridge.lib.linux.executable;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 *
 * @author jesse
 */
public class DBViewTest {

  public void _testSDF() throws Exception {
    SimpleDateFormat sdf = new SimpleDateFormat("MMddyy-HHmm");
    System.out.println("SDF: " + sdf.format(Calendar.getInstance().getTime()));
  }

  public void _testToText() throws Exception {

    Path path = Paths.get("/tmp/etl/ic_bdbs", "amstatio.dbf");

    System.out.println("start path is " + path);
    if (path.toString().endsWith(".dbf")) {
      System.out.println("  endswith .dbf");
    }

    Path data = DBView.convert(path);

    System.out.println("  DATA is " + data);

  }

}
