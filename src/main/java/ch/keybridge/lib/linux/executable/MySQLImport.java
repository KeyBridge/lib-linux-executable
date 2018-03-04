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

import java.io.*;
import java.net.URLDecoder;
import java.nio.file.Path;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class to wrap the <code>mysqlimport</code> command in a Java process
 * and make that program accessible from within JavaEE ETL business logic.
 * <p>
 * This utility requires that a MySQL client is installed on the host system and
 * the <code>mysqlimport</code> program is available and executable.
 * <p>
 * The <code>mysqlimport</code> command is configured by entries in the
 * preferences container, which are typically loaded from a file. Following is a
 * partial list of preferences entries:
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
 * 05/30/14 - add support for configuration via a properties file, data file
 * renaming and zip files with directory structure.
 * <p>
 * @author Jesse Caulfield 03/14/14
 */
public class MySQLImport {

  private static final Logger LOGGER = Logger.getLogger(MySQLImport.class.getName());

  /**
   * mysqlimport - a data import program.
   * <p>
   * The mysqlimport client provides a command-line interface to the LOAD DATA
   * INFILE SQL statement.
   * <p>
   * The absolute reference to the MySQL Importer program. On UNIX-like systems
   * the shell only executes programs residing in the current directory if given
   * an unambiguous path to it. For example: 'mysqlimport' will not work but
   * '/usr/bin/mysqlimport' does.
   */
  private static String MySQL_IMPORT = "/usr/bin/mysqlimport";

  /**
   * The data import properties. If not explicitly identified in the load()
   * method call the properties are referenced to identify MySQL database access
   * parameters (host, user, pass). Regardless, if present, properties are
   * queried to identify data file to table translation.
   */
  private Properties properties;
  /**
   * A list of tables in the current database. This is used to validate that the
   * input data file can be matched to a database table.
   */
  private List<String> dataTables;

  /**
   * Construct a new MySQL Importer class.
   * <p>
   * The default configuration of force=true and replace=true are applied.
   */
  public MySQLImport() {
    this.properties = new Properties();
  }

  /**
   * Construct a new MySQL Importer class, setting all properties from a
   * properties instance.
   * <p>
   * @param properties a properties instance.
   */
  public MySQLImport(Properties properties) {
    this();
    this.properties = new Properties(properties);
  }

  /**
   * Get a MySQL Importer instance using the provided access configuration.
   * <p>
   * @param properties the configuration properties.
   * @return a MySQL importer instance.
   */
  public static MySQLImport getInstance(Properties properties) {
    MySQLImport importer = new MySQLImport();
    importer.setProperties(properties);
    return importer;
  }

  /**
   * Get a MySQLImport instance using the default access configuration found in
   * the 'mysql.properties' resource bundle.
   * <p>
   * @return a MySQL importer instance.
   */
  public static MySQLImport getInstance() {
    return new MySQLImport();
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
    this.properties = properties;
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
    properties.setProperty(key, value);
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
   * LOAD the data file into the specified MySQL database.
   * <p>
   * This command makes use of the Java ProcessBuilder and Process classes to
   * call the system 'mysqlimport' command utility.
   * <p>
   * The following properties MUST be set for this method to execute:
   * mysql.database, mysql.host, mysql.user, mysql.pass.
   * <p>
   * @param dataFile the data file to be imported into the database.
   * @return TRUE if the load process completed successfully
   * @throws IOException          the data file could not be extracted or read
   * @throws InterruptedException if the mysqlimport process was interrupted
   */
  public Properties load(Path dataFile) throws Exception {
    /**
     * Call the loader with parameters from the properties.
     */
    return load(dataFile.toFile());
  }

  /**
   * Query the database for a list of all tables in the immediate database.
   *
   * @throws Exception if the database cannot be queried
   */
  private void readDataTables() throws Exception {
    LOGGER.log(Level.FINE, "MySQL Import reading database tables.");
    MySQL mysql = MySQL.getInstance(properties);
    String result = mysql.select("show tables");
    this.dataTables = Arrays.asList(result.split("\n"));
  }

  /**
   * LOAD the data file into the specified MySQL database.
   * <p>
   * This command makes use of the Java ProcessBuilder and Process classes to
   * call the system 'mysqlimport' command utility.
   * <p>
   * The load process metrics may be retrieved with the
   * {@link #getProperty(java.lang.String)} method.
   * <p>
   * @param dataFile the data file to be imported into the database.
   * @return the load process status information
   * @throws IOException          the data file could not be extracted or read
   * @throws InterruptedException if the mysqlimport process was interrupted
   */
  @SuppressWarnings("AssignmentToMethodParameter")
  public Properties load(File dataFile) throws IOException, InterruptedException, Exception {
    Properties status = new Properties();
    /**
     * Start the clock. Initialize the status.
     */
    long startTime = System.currentTimeMillis();
    /**
     * This method requires that the configuration is present in the Properties.
     * Also fail if the data file is not in fact a file (but rather a directory
     * or something else).
     */
    for (String requiredProperty : new String[]{"mysql.database", "mysql.host", "mysql.user", "mysql.pass"}) {
      if (properties.getProperty(requiredProperty) == null || properties.getProperty(requiredProperty).isEmpty()) {
        throw new Exception("MySQL Import invalid properties configuration. " + requiredProperty + " is required.");
      }
    }
    /**
     * Inspect the data file.
     */
    if (dataFile == null || !dataFile.exists() || !dataFile.isFile()) {
      throw new Exception("MySQL Import invalid, empty or null source data file: " + dataFile);
    } else if (dataFile.getName().toLowerCase().endsWith(".zip")) {
      /**
       * If the file is a ZIP file the unzip the ZIP container and try to load
       * its contents. Do not try to load ZIP file archives.
       */
      throw new Exception("MySQL Import unreadable source data file: " + dataFile + " is a ZIP file.");
    } else if (dataFile.length() == 0) {
      /**
       * Java unzip may create an empty file if the zip file contains an empty
       * directory. Test and skip if the file is empty.
       */
      LOGGER.log(Level.WARNING, "MySQL Import data file {0} is empty (zero length). Ignoring.", dataFile.getName());
      status.setProperty("Duration", String.valueOf(System.currentTimeMillis() - startTime));
      status.setProperty("Ignored", "TRUE");
      return status;
    }
    /**
     * Read the data tables if required.
     */
    if (dataTables == null) {
      readDataTables();
    }
    /**
     * Developer notes:
     * <p>
     * The ProcessBuilder.start() and Runtime.exec methods create a native
     * process and return an instance of a subclass of Process that can be used
     * to control the process and obtain information about it. The class Process
     * provides methods for performing input from the process, performing output
     * to the process, waiting for the process to complete, checking the exit
     * status of the process, and destroying (killing) the process.
     * <p>
     * The methods that create processes may not work well for special processes
     * on certain native platforms, such as native windowing processes, daemon
     * processes, Win16/DOS processes on Microsoft Windows, or shell scripts.
     * <p>
     * By default, the created subprocess does not have its own terminal or
     * console. All its standard I/O (i.e. stdin, stdout, stderr) operations
     * will be redirected to the parent process, where they can be accessed via
     * the streams obtained using the methods Process.getOutputStream(),
     * Process.getInputStream(), and Process.getErrorStream(). The parent
     * process uses these streams to feed input to and get output from the
     * subprocess. Because some native platforms only provide limited buffer
     * size for standard input and output streams, failure to promptly write the
     * input stream or read the output stream of the subprocess may cause the
     * subprocess to block, or even deadlock.
     * <p>
     * Where desired, subprocess I/O can also be redirected using methods of the
     * ProcessBuilder class.
     * <p>
     * The subprocess is not killed when there are no more references to the
     * Process object, but rather the subprocess continues executing
     * asynchronously.
     * <p>
     * There is no requirement that a process represented by a Process object
     * execute asynchronously or concurrently with respect to the Java process
     * that owns the Process object.
     * <p>
     * As of 1.5, ProcessBuilder.start() is the preferred way to create a
     * Process.
     * <p>
     * Executes the specified string command in a separate process with the
     * specified environment and working directory.
     * <p>
     * This is a convenience method. An invocation of the form exec(command,
     * envp, dir) behaves in exactly the same way as the invocation
     * exec(cmdarray, envp, dir), where cmdarray is an array of all the tokens
     * in command.
     * <p>
     * More precisely, the command string is broken into tokens using a
     * StringTokenizer created by the call new StringTokenizer(command) with no
     * further modification of the character categories. The tokens produced by
     * the tokenizer are then placed in the new string array cmdarray, in the
     * same order.
     * <p>
     * Parameters:
     * <ul>
     * <li>{@code command } - a specified system command.</li>
     * <li> {@code envp} - array of strings, each element of which has
     * environment variable settings in the format name=value, or null if the
     * subprocess should inherit the environment of the current process.</li>
     * <li> {@code dir} - the working directory of the subprocess, or null if
     * the subprocess should inherit the working directory of the current
     * process.</li>
     * </ul>
     * Returns: A new Process object for managing the subprocess
     */
    /**
     * All data files MUST be lower case - rename them thusly.
     */
//    dataFile.renameTo(new File(dataFile.getParentFile(), dataFile.getName().toLowerCase()));
//    dataFile = new File(dataFile.getParentFile(), dataFile.getName().toLowerCase());
    /**
     * If the properties is set then check to see if the data file should be
     * renamed to match a desired table name before being imported.
     */
//    if (properties.getProperty("data.filenames") != null) {
//      Map<String, String> dataFileNames = decodeKVMap(properties.getProperty("data.filenames"));
//      if (dataFileNames.containsKey(dataFile.getName())) {
//        LOGGER.log(Level.FINE, "MySQL Import renaming data file {0} to {1} before processing.", new Object[]{dataFile.getName(), dataFileNames.get(dataFile.getName())});
//        dataFile.renameTo(new File(dataFile.getParent(), dataFileNames.get(dataFile.getName())));
//        dataFile = new File(dataFile.getParent(), dataFileNames.get(dataFile.getName()));
//      }
//    }
    /**
     * Only process the immediate data file if this file matches a existing
     * database table.
     * <p>
     * Developer note: The regular expression "\\.(?=[^\\.]+$)" tells Java to
     * split on any period that is followed by any number of non-periods,
     * followed by the end of input. There is only one period that matches this
     * definition (namely, the last period). Technically speaking, this
     * technique is called 'zero-width positive lookahead'. The ?= means
     * "positive lookahead". You can also have negative lookahead (?!), positive
     * lookbehind (?<=) and negative lookbehind (?<!). These four expressions
     * are collectively known as the lookaround expressions.
     */
    String baseName = dataFile.getName().contains(".")
                      ? dataFile.getName().split("\\.(?=[^\\.]+$)")[0]
                      : dataFile.getName();
    if (!dataTables.contains(baseName)) {
      LOGGER.log(Level.WARNING, "MySQL Import table {0}.{1} does not exist. Ignoring file {2}.",
                 new Object[]{properties.getProperty("mysql.database"), baseName, dataFile.getName()}
      );
      status.setProperty("Duration", String.valueOf(System.currentTimeMillis() - startTime));
      status.setProperty("Ignored", "TRUE");
      return status;
    }
    /**
     * OK to proceed. Print a status notification.
     */
    LOGGER.log(Level.FINE, "MySQL Import loading data file {0} into database.", dataFile.getName());
    /**
     * Build then execute the MySQL import system command.
     */
    StringBuilder mysqlimport = new StringBuilder(MySQL_IMPORT).append(" --local");
    /**
     * Ignore errors. (Default is FALSE) For example, if a table for a text file
     * does not exist, continue processing any remaining files. Without --force,
     * mysqlimport exits if a table does not exist.
     */
    if (Boolean.valueOf(properties.getProperty("mysql.force"))) {
      mysqlimport.append(" --force");
    }
    /**
     * Empty the table before importing the text file. (Default is TRUE).
     */
    if (Boolean.valueOf(properties.getProperty("mysql.delete"))) {
      mysqlimport.append(" --delete");
    }

    /**
     * IGNORE duplicate entries unless REPLACE is explicitly declared.
     * <p>
     * Replace duplicate fields. (Default is TRUE) The --replace options
     * controls handling of input rows that duplicate existing rows on unique
     * key values. If REPLACE then new rows replace existing rows that have the
     * same unique key value. If IGNORE then input rows that duplicate an
     * existing row on a unique key value are skipped.
     * <p>
     * If false then the --ignore option is set, where input rows that duplicate
     * an existing row on a unique key value are skipped.
     */
    if (Boolean.valueOf(properties.getProperty("mysql.replace"))) {
      mysqlimport.append(" --replace");
    } else {
      mysqlimport.append(" --ignore");
    }
    if (properties.getProperty("mysql.ignore-lines") != null) {
      mysqlimport.append(" --ignore-lines=").append(properties.getProperty("mysql.ignore-lines"));
    }
    if (properties.getProperty("mysql.fields-terminated-by") != null) {
      mysqlimport.append(" --fields-terminated-by=").append(properties.getProperty("mysql.fields-terminated-by"));
    }
    if (properties.getProperty("mysql.lines-terminated-by") != null) {
      mysqlimport.append(" --lines-terminated-by=").append(properties.getProperty("mysql.lines-terminated-by"));
    }
    if (properties.getProperty("mysql.fields-optionally-enclosed-by") != null) {
      mysqlimport.append(" --fields-optionally-enclosed-by=").append(properties.getProperty("mysql.fields-optionally-enclosed-by"));
    } else if (properties.getProperty("mysql.fields-enclosed-by") != null) {
      mysqlimport.append(" --fields-enclosed-by=").append(properties.getProperty("mysql.fields-enclosed-by"));
    }

    mysqlimport.append(" --lock-tables");
    mysqlimport.append(" --low-priority");

    mysqlimport.append(" --host=").append(properties.getProperty("mysql.host"));
    mysqlimport.append(" --user=").append(properties.getProperty("mysql.user"));
    mysqlimport.append(" --password=").append(properties.getProperty("mysql.pass"));
    mysqlimport.append(" ").append(properties.getProperty("mysql.database"));
    mysqlimport.append(" ").append(dataFile);
    LOGGER.log(Level.FINE, "MySQL Import executing system process {0}.", mysqlimport.toString());
    /**
     * Developer note: Build the command as a string, then run the command in a
     * UNIX Shell. This simplifies setting the various command parameters. Also
     * note that on UNIX-like systems the shell only executes programs residing
     * in the current directory if given an unambiguous path to it.
     */
    ProcessBuilder processBuilder = new ProcessBuilder("/bin/sh", "-c", mysqlimport.toString());
    processBuilder.directory(dataFile.getParentFile());
    /**
     * Sets this process builder standard output destination. If the destination
     * is Redirect.PIPE (the initial value), then the standard output of a
     * subprocess can be read using the input stream returned by
     * Process.getInputStream().
     */
    processBuilder.redirectOutput(ProcessBuilder.Redirect.PIPE);
    processBuilder.redirectError(ProcessBuilder.Redirect.PIPE);
    /**
     * Start the process.
     */
    Process process = processBuilder.start();
    /**
     * Log the process output with duration.
     */
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        /**
         * Parse the 'mysqlimport' output and set it to the status properties.
         */
        status.setProperty("Table", line.split(":")[0]);
        Pattern pattern = Pattern.compile("\\s(\\w+): (\\d+)\\s?");
        Matcher matcher = pattern.matcher(line);
        int start = 0;
        while (matcher.find(start)) {
          status.setProperty(matcher.group(1), matcher.group(2));
          start += matcher.group(0).length();
        }
        status.setProperty("Time", String.valueOf(System.currentTimeMillis() - startTime));
        /**
         * Log the action.
         */
        LOGGER.log(Level.INFO, "MySQL Import {0}", status.toString());
      }
    }
    /**
     * Return TRUE if the process returned zero. Causes the current thread to
     * wait, if necessary, until the process represented by this Process object
     * has terminated. This method returns immediately if the subprocess has
     * already terminated. If the subprocess has not yet terminated, the calling
     * thread will be blocked until the subprocess exits.
     */
    if (process.waitFor() != 0) {
      throw new Exception("MySQL Import process did not exit cleanly: " + mysqlimport.toString().replaceFirst("--password=\\S+ ", "--password=xxxxxxxx "));
    }
    /**
     * The process exited OK. Set the MySQL process status metrics.
     */
    status.setProperty("Duration", String.valueOf(System.currentTimeMillis() - startTime));
    return status;
  }

  //<editor-fold defaultstate="collapsed" desc="String Encode / Decode Methods">
  /**
   * Decode an encoded String into a list of Strings.
   * <p/>
   * This method decodes each string from the UTF-8 character set to ensure it
   * is properly returned exactly as it was entered prior to persistence in the
   * database.
   * <p/>
   * @param encodedListString The encoded string.
   * @return A non-null (possibly empty) ArrayList of Strings.
   */
  @SuppressWarnings("AssignmentToMethodParameter")
  private List<String> decodeList(String encodedListString) {
    List<String> stringList = new ArrayList<>();
    /**
     * Null and empty check.
     */
    if (encodedListString == null || encodedListString.isEmpty()) {
      return stringList;
    }
    /**
     * Strip brackets off the input String.
     */
    if (encodedListString.contains("[") && encodedListString.contains("]")) {
      encodedListString = encodedListString.substring(1, encodedListString.length() - 1);
    }
    for (String string : encodedListString.split(",")) {
      stringList.add(decode(string).trim());
    }
    return stringList;
  }

  /**
   * Decode URL-encoded text into a raw text string.
   * <p/>
   * This method will silently ignore illegal hex characters in escape pattern,
   * such as (%). Decoding is a best-effort try so this method will ignore any
   * text parameter decoding errors; specifically if the string cannot be
   * decoded from the UTF8 character set.
   * <p/>
   * @param urlEncodedText the URL-encoded string
   * @return the original, un-encoded string
   */
  private String decode(String urlEncodedText) {
    try {
      return URLDecoder.decode(urlEncodedText, "UTF-8");
    } catch (UnsupportedEncodingException | NullPointerException exception) {
      return "";
    }
  }

  /**
   * Decode a URI-encoded key value map into its constituents.
   * <p/>
   * This method uses an internal TreeMap to ensure the key/value pairs are
   * always sorted by KEY.
   * <p/>
   * @param keyValueString a URL-encoded set of key/value pairs
   * @return non-null Map (TreeMap implementation)
   */
  @SuppressWarnings("AssignmentToMethodParameter")
  private Map<String, String> decodeKVMap(String keyValueString) {
    Map<String, String> queryMap = new TreeMap<>();
    /**
     * Only process if the keyValueString is not empty
     */
    if (keyValueString != null && !keyValueString.isEmpty()) {
      /**
       * Strip the leading '?' if present. '?' is prepended by some URI
       * builders.
       */
      if (keyValueString.startsWith("?")) {
        keyValueString = keyValueString.substring(1);
      }
      /**
       * Parse the URI-encoded string into a set of key/value pairs on '&', then
       * decode each key/value pair into a map entry.
       */
      for (String keyValuePair : keyValueString.split("&")) {
        try {
          /**
           * Split the Key/Value pair on '=', then populate the map with the
           * (decoded) key and (decoded) value pairs (decoding again here is
           * important!)
           */
          String[] kv = decode(keyValuePair).split("=");
          queryMap.put(decode(kv[0]), decode(kv[1]));
        } catch (Exception ex) {
          //        Logger.getLogger(URIEncodeDecodeFactory.class.getName()).log(Level.SEVERE, null, ex);
        }
      }
    }
    return queryMap;
  }//</editor-fold>
}
