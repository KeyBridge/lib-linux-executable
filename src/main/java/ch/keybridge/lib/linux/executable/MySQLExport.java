/*
 * Copyright 2018 Key Bridge.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ch.keybridge.lib.linux.executable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility class to wrap the <code>mysql</code> command in a Java process and
 * make that program accessible from within JavaEE ETL business logic.
 * <p>
 * This utility requires that a MySQL client is installed on the host system and
 * the <code>mysql</code> program is available and executable.
 * <p>
 * The <code>mysql</code> command is configured by entries in the preferences
 * container, which are typically loaded from a file. Following is a partial
 * list of preferences entries:
 * <pre>
 * # run.host - (optional) the hostname (partial or full) of the host that should run
 * #            this ETL process. this is useful when multiple systems are running the same
 * #            application but only one host should handle ETL duties
 * run.host=[hostname.domain.com]
 * #
 * # MySQL access configuration - (required)
 * mysql.host=[hostname/ip address]
 * mysql.user=[username]
 * mysql.pass=[password]
 * #
 * # mysql.database - (optional) the database into which the data files below are to be loaded
 * #                  the table names are the file names. to translate the raw file
 * #                  names to something else add a list of translations below.
 * #                  this is required for some ETL programs
 * mysql.database=[database name]
 * #
 * # import parameters (optional)
 * mysql.fields-terminated-by=','
 * # mysql.lines-terminated-by=,
 * mysql.fields-optionally-enclosed-by='"'
 * # mysql.fields-enclosed-by='"'
 * mysql.ignore-lines=2
 * #
 * # data.host - (required) - the remote host from which the data is retrieved
 * #             identify the data source and context root
 * # data.host=http://geolite.maxmind.com/download/geoip/database/GeoLiteCity_CSV/
 * data.host=[URL to the file directory in which data files are located]
 * #
 * # data.file - (required) A comma-delimited list of file names to download from the data.host entry
 * #             enclosing brackets [] are optional, spaces are ignored
 * data.files=[GeoLiteCity-latest.zip]
 * #
 * # data file name - rename rules (optional)  a mapping of raw to cooked file names
 * #              separate entries with '&amp;', separate raw/cooked with '='.
 * data.filenames=GeoLiteCity-Blocks.csv=geoip_block.csv&amp;GeoLiteCity-Location.csv=geoip_location.csv
 * </pre>
 * <p>
 * 09/03/14 - copied from the MysqlDataImport utility <br>
 * 01/04/15 - replace system.out with logger
 * <p>
 * @author Jesse Caulfield
 * @since 1.0.0 added 09/03/14
 */
public class MySQLExport {

  private static final Logger LOGGER = Logger.getLogger(MySQLExport.class.getName());

  /**
   * "mysql". The resource bundle properties file from which this utility tries
   * to find the default MySQL database access credentials.
   */
  private static final String BUNDLE = "mysql";

  /**
   * mysql - the database client program.
   * <p>
   * mysql is a simple SQL shell with input line editing capabilities. It
   * supports interactive and noninteractive use. When used interactively, query
   * results are presented in an ASCII-table format. When used noninteractively
   * (for example, as a filter), the result is presented in tab-separated
   * format. The output format can be changed using command options.
   * <p>
   * The absolute reference to the MySQL Importer program. On UNIX-like systems
   * the shell only executes programs residing in the current directory if given
   * an unambiguous path to it. For example: 'mysql' will not work but
   * '/usr/bin/mysql' does.
   */
  private static final String MYSQL = "/usr/bin/mysql";

  /**
   * The data import properties. If not explicitly identified in the load()
   * method call the properties are referenced to identify MySQL database access
   * parameters (host, user, pass). Regardless, if present, properties are
   * queried to identify data file to table translation.
   */
  private Properties properties;
  /**
   * A container for the just executed process status.
   */
  private Properties status;

  /**
   * Construct a new MySQL Exporter class.
   */
  public MySQLExport() {
    this.properties = new Properties();
  }

  /**
   * Construct a new MySQL Exporter class, copying all properties from a
   * properties instance.
   * <p>
   * @param properties a properties instance.
   */
  public MySQLExport(Properties properties) {
    this();
    for (Map.Entry<Object, Object> entrySet : properties.entrySet()) {
      this.properties.setProperty(String.valueOf(entrySet.getKey()),
                                  String.valueOf(entrySet.getValue()));
    }
  }

  /**
   * Get a MySQL Exporter instance using the provided access configuration.
   * <p>
   * @param host the MySQL database host
   * @param user the access user
   * @param pass the access user password
   * @return a MySQL importer instance.
   */
  public static MySQLExport getInstance(String host, String user, String pass) {
    MySQLExport exporter = new MySQLExport();
    exporter.setProperty("mysql.host", host);
    exporter.setProperty("mysql.user", user);
    exporter.setProperty("mysql.pass", pass);
    return exporter;
  }

  /**
   * Construct a new MySQL Exporter class, setting basic access properties from
   * the default 'mysql.properties' resource bundle.
   * <p>
   * @return a new MySQL Exporter class, setting all properties from a
   *         properties instance.
   * @throws Exception if the resource bundle does not exist or cannot be found.
   */
  public static MySQLExport getInstance() throws Exception {
    MySQLExport mysqlExporter = new MySQLExport();
    try {
      ResourceBundle bundle = ResourceBundle.getBundle(BUNDLE);
      try {
        mysqlExporter.setProperty("mysql.host", bundle.getString("mysql.host"));
        mysqlExporter.setProperty("mysql.user", bundle.getString("mysql.user"));
        mysqlExporter.setProperty("mysql.pass", bundle.getString("mysql.pass"));
      } catch (Exception e) {
        throw new Exception(BUNDLE + " resource bundle does contain an entry for mysql.host, user and pass.");
      }
    } catch (Exception e) {
      throw new Exception(BUNDLE + " resource bundle not found. Please add this properties file to the classpath.");
    }
    return mysqlExporter;
  }

  /**
   * Set a properties field. If not explicitly identified in the load() method
   * call the properties are referenced to identify MySQL database access
   * parameters (host, user, pass). Regardless, if present, properties are
   * queried to identify data file to table translation.
   * <p>
   * @param key   the key to be placed into this property list.
   * @param value the value corresponding to key.
   */
  public void setProperty(String key, String value) {
    this.properties.setProperty(key, value);
  }

  /**
   * Get the status of the most recent load process.
   * <p>
   * @return a non-null Properties instance containing the results from the most
   *         recent load process.
   */
  public Properties getStatus() {
    if (status == null) {
      status = new Properties();
    }
    return status;
  }

  /**
   * Execute the provided SQL statement, writing the result to the output date
   * file.
   * <p>
   * This command makes use of the Java ProcessBuilder and Process classes to
   * call the system 'mysql' command utility and dump the WSIF database into a
   * flat file.
   * <p>
   * Default is to write output in plain text (tab delimited) format. To write
   * the output in pretty-print TABLEs format set the property 'table' to
   * 'true'.
   * <p>
   * Note that the outfile will be overwritten if it exists. Disable this by
   * setting the 'replace' property to 'false'. If needed the outfile parent
   * directories will be automatically created.
   * <p>
   * @param sqlStatement the native, fully qualified SQL SELECT statement to
   *                     execute.
   * @param outFile      the data file into which the data is to be exported.
   *                     This file must NOT already exist and must be creatable
   *                     by the system user.
   * @return TRUE if the load process completed successfully
   * @throws IllegalArgumentException if the SQL statement does not begin with
   *                                  "SELECT" (not case sensitive).
   * @throws IOException              the data file could not be extracted or
   *                                  read
   * @throws InterruptedException     if the process was interrupted
   * @throws Exception                on other error
   */
  public boolean export(String sqlStatement, Path outFile) throws IllegalArgumentException, IOException, InterruptedException, Exception {
    /**
     * Verify the native SQL is at least pro-forma acceptable and that we have a
     * nominally correct configuration.
     */
    if (sqlStatement == null || sqlStatement.isEmpty()) {
      throw new Exception("Null or empty SQL statement.");
    } else if (!sqlStatement.trim().toUpperCase(Locale.getDefault()).startsWith("SELECT")) {
      throw new IllegalArgumentException("SQL statement must begin with \"SELECT\": " + sqlStatement);
    } else if (outFile == null) {
      throw new Exception("Invalid properties configuration. MySQL Destination (outfile) is required.");
    } else if (outFile.toFile().exists()) {
      if (Boolean.valueOf(properties.getProperty("replace", "true"))) {
        outFile.toFile().delete(); // delete the existing file.
      } else {
        throw new IOException(outFile + " already exists and replace is disabled.");
      }
    } else if (properties.getProperty("mysql.database") == null) {
      throw new Exception("Invalid properties configuration. MySQL Database is required.");
    } else if (properties.getProperty("mysql.host") == null) {
      throw new Exception("Invalid properties configuration. MySQL Host is required.");
    } else if (properties.getProperty("mysql.user") == null) {
      throw new Exception("Invalid properties configuration. MySQL User is required.");
    } else if (properties.getProperty("mysql.pass") == null) {
      throw new Exception("Invalid properties configuration. MySQL Pass is required.");
    }
    /**
     * Create the dump directory if it does not already exist.
     */
    if (!outFile.getParent().toFile().exists()) {
      Files.createDirectories(outFile.getParent());
    }
    /**
     * Build then execute the MySQL import system command.
     */
    StringBuilder mysqlexport = new StringBuilder(MYSQL)
      .append(" --database=").append(properties.getProperty("mysql.database"))
      .append(" --host=").append(properties.getProperty("mysql.host"))
      .append(" --user=").append(properties.getProperty("mysql.user"))
      .append(" --password=").append(properties.getProperty("mysql.pass"));
    /**
     * --table, -t Display output in table format. This is disabled by default
     * but can be used to produce pretty-print table output.
     */
    if (Boolean.valueOf(properties.getProperty("table", "false"))) {
      mysqlexport.append(" --table");
    }
    mysqlexport.append(" --execute='")
      .append(sqlStatement.replaceAll("\n", " ")) // handle multi-line
      .append(sqlStatement.trim().endsWith(";") ? "" : ";") // append a terminator
      .append("' > ")
      .append(outFile);
    LOGGER.log(Level.INFO, "Executing system process {0}.", mysqlexport.toString());
    /**
     * Start the clock. Initialize the status.
     */
    long startTime = System.currentTimeMillis();
    this.status = new Properties();
    status.setProperty("startTime", String.valueOf(startTime));
    /**
     * Developer note: Build the command as a string, then run the command in a
     * UNIX Shell. This simplifies setting the various command parameters. Also
     * note that on UNIX-like systems the shell only executes programs residing
     * in the current directory if given an unambiguous path to it.
     */
    ProcessBuilder processBuilder = new ProcessBuilder("/bin/sh", "-c", mysqlexport.toString());
    processBuilder.directory(outFile.toFile().getParentFile());
    /**
     * Sets this process builder standard output destination. If the destination
     * is Redirect.PIPE (the initial value), then the standard output of a
     * subprocess can be read using the input stream returned by
     * Process.getInputStream().
     * <p>
     * MySQL does not produce any output, so there is nothing to capture here.
     */
//    processBuilder.redirectOutput(ProcessBuilder.Redirect.PIPE);
//    processBuilder.redirectError(ProcessBuilder.Redirect.PIPE);
    /**
     * Start the process.
     */
    Process process = processBuilder.start();
    /**
     * Log the process output.
     * <p>
     * MySQL does not produce any output, so there is nothing to capture here.
     */
//    try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
//      String line;
//      while ((line = reader.readLine()) != null) {
//        logger.log(Level.INFO, "mysqlexport {0}  Time: {1} ", new Object[]{line, (System.currentTimeMillis() - startTime)});
//      }
//    }
    /**
     * Wait for the MySQL process to execute. Return TRUE if the process
     * returned zero. Causes the current thread to wait, if necessary, until the
     * process represented by this Process object has terminated. This method
     * returns immediately if the subprocess has already terminated. If the
     * subprocess has not yet terminated, the calling thread will be blocked
     * until the subprocess exits.
     */
    if (process.waitFor() != 0) {
      throw new Exception("MySQL export process did not exit cleanly: " + mysqlexport.toString());
    }
    /**
     * Set the MySQL process status metrics.
     */
    status.setProperty("fileSize", String.valueOf(outFile.toFile().length()));
    status.setProperty("duration", String.valueOf(System.currentTimeMillis() - startTime));
    /**
     * Build a new process to count the number of rows exported.
     */
    processBuilder.command("/bin/sh", "-c", "/usr/bin/wc -l " + outFile.toString());
    processBuilder.redirectOutput(ProcessBuilder.Redirect.PIPE);
    processBuilder.redirectError(ProcessBuilder.Redirect.PIPE);
    process = processBuilder.start();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        status.setProperty("records", line.split(" ")[0]);
      }
    }
    /**
     * Wait for the 'wc' process to execute.
     */
    process.waitFor();
    /**
     * The process exited OK.
     */
    return true;
  }
}
