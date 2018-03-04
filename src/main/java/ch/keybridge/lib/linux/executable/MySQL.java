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
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility class to wrap the <code>mysql</code> command in a Java process and
 * make that program accessible from within JavaEE ETL business logic.
 * <p>
 * This utility requires that a MySQL client is installed on the host system and
 * the <code>mysql</code> program is available and executable.
 * <p>
 * @author Jesse Caulfield 09/09/14
 */
public class MySQL {

  private static final Logger LOGGER = Logger.getLogger(MySQL.class.getName());

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
   * The data import properties. The properties are referenced to identify MySQL
   * database access parameters (host, user, pass).
   */
  private Properties properties;

  /**
   * Construct a new MySQL Exporter class.
   */
  public MySQL() {
    this.properties = new Properties();
  }

  /**
   * Get a MySQL instance using the provided access configuration.
   * <p>
   * @param properties the configuration properties.
   * @return a MySQL importer instance.
   */
  public static MySQL getInstance(Properties properties) {
    MySQL exporter = new MySQL();
    exporter.setProperties(properties);
    return exporter;
  }

  /**
   * Construct a new MySQL Exporter class, setting basic access properties from
   * the default 'mysql.properties' resource bundle.
   * <p>
   * @return a new MySQL Exporter class, setting all properties from a
   *         properties instance.
   */
  public static MySQL getInstance() {
    return new MySQL();
  }

  /**
   * Set a properties field. If not explicitly identified in the load() method
   * call the properties are referenced to identify MySQL database access
   * parameters (host, user, pass). Regardless, if present, properties are
   * queried to identify data file to table translation.
   * <p>
   * @param properties the properties to be placed into this property list.
   */
  public void setProperties(Properties properties) {
    this.properties = new Properties(properties);
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
   * Searches for the property with the specified key in this property list. If
   * the key is not found in this property list, the default property list, and
   * its defaults, recursively, are then checked. The method returns null if the
   * property is not found.
   * <p>
   * @param key the property key.
   * @return the value in this property list with the specified key value.
   */
  public String getProperty(String key) {
    return properties.getProperty(key);
  }

  /**
   * Execute a SQL SELECT statement against a database.
   * <p>
   * Note that all database configurations are set in the Properties.
   *
   * @param sqlStatement the SQL statement
   * @return the statement result
   * @throws Exception if the MySQL executable is missing any required property
   *                   or the SQL statement is null or empty.
   */
  public String select(String sqlStatement) throws Exception {
    /**
     * Verify a nominally correct configuration.
     */
    for (String requiredProperty : new String[]{"mysql.database", "mysql.host", "mysql.user", "mysql.pass"}) {
      if (properties.getProperty(requiredProperty) == null || properties.getProperty(requiredProperty).isEmpty()) {
        throw new Exception("MySQL invalid properties configuration. " + requiredProperty + " is required.");
      }
    }
    /**
     * Verify the native SQL is at least pro-forma acceptable.
     */
    if (sqlStatement == null || sqlStatement.isEmpty()) {
      throw new Exception("MySQL Null or empty SQL statement.");
    }
    /**
     * Build then execute the MySQL import system command.
     */
    StringBuilder mysql = new StringBuilder(MYSQL)
            .append(" --database=").append(properties.getProperty("mysql.database"))
            .append(" --host=").append(properties.getProperty("mysql.host"))
            .append(" --user=").append(properties.getProperty("mysql.user"))
            .append(" --password=").append(properties.getProperty("mysql.pass"));
    mysql.append(" --execute='")
            .append(sqlStatement)
            .append(sqlStatement.trim().endsWith(";") ? "'" : ";'"); // append a terminator
    LOGGER.log(Level.FINE, "MySQL Executing system process {0} : ", mysql.toString());
    /**
     * Developer note: Build the command as a string, then run the command in a
     * UNIX Shell. This simplifies setting the various command parameters. Also
     * note that on UNIX-like systems the shell only executes programs residing
     * in the current directory if given an unambiguous path to it.
     */
    ProcessBuilder processBuilder = new ProcessBuilder("/bin/sh", "-c", mysql.toString());
    /**
     * Start the process.
     */
    Process process = processBuilder.start();
    /**
     * Log the process output with duration.
     */
    StringBuilder response = new StringBuilder();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        response.append(line).append("\n");
      }
    }
    /**
     * Wait for the process to exit.
     */
    if (process.waitFor() != 0) {
      throw new Exception("MySQL process did not exit cleanly: " + mysql.toString());
    }
    /**
     * Return the output.
     */
    return response.toString();
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
   *                     update.
   * @return TRUE if the load process completed successfully
   * @throws IllegalArgumentException if the SQL statement does not begin with
   *                                  "SELECT" (not case sensitive).
   * @throws IOException              the data file could not be extracted or
   *                                  read
   * @throws InterruptedException     if the process was interrupted
   * @throws Exception                if the MySQL executable is missing any
   *                                  required property or the SQL statement is
   *                                  null or empty.
   */
  public Properties execute(String sqlStatement) throws IllegalArgumentException, IOException, InterruptedException, Exception {
    Properties status = new Properties();
    /**
     * Start the clock. Initialize the status.
     */
    long startTime = System.currentTimeMillis();
    /**
     * Verify a nominally correct configuration.
     */
    for (String requiredProperty : new String[]{"mysql.database", "mysql.host", "mysql.user", "mysql.pass"}) {
      if (properties.getProperty(requiredProperty) == null || properties.getProperty(requiredProperty).isEmpty()) {
        throw new Exception("MySQL invalid properties configuration. " + requiredProperty + " is required.");
      }
    }
    /**
     * Add the properties to the status response.
     */
    for (String property : new String[]{"mysql.database", "mysql.host"}) {
      status.setProperty(property, properties.getProperty(property));
    }
    /**
     * Verify the native SQL is at least pro-forma acceptable.
     */
    if (sqlStatement == null || sqlStatement.isEmpty()) {
      throw new Exception("MySQL Null or empty SQL statement.");
    }
    /**
     * Build then execute the MySQL import system command.
     */
    StringBuilder mysql = new StringBuilder(MYSQL)
            .append(" --database=").append(properties.getProperty("mysql.database"))
            .append(" --host=").append(properties.getProperty("mysql.host"))
            .append(" --user=").append(properties.getProperty("mysql.user"))
            .append(" --password=").append(properties.getProperty("mysql.pass"));
    mysql.append(" --execute='")
            .append(sqlStatement)
            .append(sqlStatement.trim().endsWith(";") ? "'" : ";'"); // append a terminator
    LOGGER.log(Level.FINE, "MySQL Executing system process {0} : ", mysql.toString());
    /**
     * Developer note: Build the command as a string, then run the command in a
     * UNIX Shell. This simplifies setting the various command parameters. Also
     * note that on UNIX-like systems the shell only executes programs residing
     * in the current directory if given an unambiguous path to it.
     */
    ProcessBuilder processBuilder = new ProcessBuilder("/bin/sh", "-c", mysql.toString());
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
      throw new Exception("MySQL process did not exit cleanly: " + mysql.toString());
    }
    /**
     * Log a successful database update.
     */
    LOGGER.log(Level.FINE, "MySQL Execute {0}  SQL {1}  Time: {2} ", new Object[]{properties.getProperty("mysql.database"),
                                                                                  sqlStatement,
                                                                                  (System.currentTimeMillis() - startTime)});
    /**
     * The process exited OK. Set the MySQL process status metrics.
     */
    status.setProperty("SQL", sqlStatement);
    status.setProperty("Duration", String.valueOf(System.currentTimeMillis() - startTime));
    return status;
  }

}
