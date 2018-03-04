/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ch.keybridge.lib.linux.executable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 *
 * @author jesse
 */
public class MySQLExportTest {

  public void testvoid() {
    System.out.println("mysql export test disabled - enabled if needed");
  }

  public void _testExport() throws IOException, InterruptedException, Exception {
    MySQLExport exporter = new MySQLExport();
    exporter.setProperty("mysql.host", "localhost");
    exporter.setProperty("mysql.database", "ic_bdbs");
    exporter.setProperty("mysql.user", "USERNAME");
    exporter.setProperty("mysql.pass", "PASSWORD");

    exporter.setProperty("replace", "true");

    Path outfile = Paths.get("/tmp/etl/ic_bdbs", "out.dat");

    String sql = "select * from amstatio limit 10";

    exporter.export(sql, outfile);

    System.out.println("Status " + exporter.getStatus());

  }

  public void _testWC() throws Exception {
    Path outfile = Paths.get("/tmp/etl/ic_bdbs", "out.dat");

    ProcessBuilder processBuilder = new ProcessBuilder("/bin/sh", "-c", "wc -l " + outfile.toString());
    processBuilder.directory(outfile.toFile().getParentFile());
    processBuilder.redirectOutput(ProcessBuilder.Redirect.PIPE);
    processBuilder.redirectError(ProcessBuilder.Redirect.PIPE);
    Process process = processBuilder.start();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println("line " + line);
//        logger.log(Level.INFO, "mysqlexport {0}  Time: {1} ", new Object[]{line, (System.currentTimeMillis() - startTime)});
//        status.setProperty("OutfileRecords", line.split(" ")[0]);
      }
    }
    process.waitFor();

//    System.out.println("DEBUG status " + status);
  }

}
