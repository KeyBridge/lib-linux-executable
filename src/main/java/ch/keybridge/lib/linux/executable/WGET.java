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
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility class to wrap the <code>wget</code> command in a Java process and
 * make that program accessible from within JavaEE ETL business logic.
 * <p>
 * GNU Wget is a free utility for non-interactive download of files from the
 * Web. It supports HTTP, HTTPS, and FTP protocols, as well as retrieval through
 * HTTP proxies.
 * <p>
 * Wget is non-interactive, meaning that it can work in the background, while
 * the user is not logged on. This allows you to start a retrieval and
 * disconnect from the system, letting Wget finish the work. By contrast, most
 * of the Web browsers require constant user's presence, which can be a great
 * hindrance when transferring a lot of data.
 * <p>
 * Wget can follow links in HTML, XHTML, and CSS pages, to create local versions
 * of remote web sites, fully recreating the directory structure of the original
 * site. This is sometimes referred to as "recursive downloading." While doing
 * that, Wget respects the Robot Exclusion Standard (/robots.txt). Wget can be
 * instructed to convert the links in downloaded files to point at the local
 * files, for offline viewing.
 * <p>
 * Wget has been designed for robustness over slow or unstable network
 * connections; if a download fails due to a network problem, it will keep
 * retrying until the whole file has been retrieved. If the server supports
 * regetting, it will instruct the server to continue the download from where it
 * left off.
 * <p>
 * @author Jesse Caulfield
 * @since 01/29/15
 */
public class WGET {

  private static final Logger LOGGER = Logger.getLogger(WGET.class.getName());

  /**
   * Wget - The non-interactive network downloader.
   * <p>
   * Wget is non-interactive, meaning that it can work in the background, while
   * the user is not logged on. This allows you to start a retrieval and
   * disconnect from the system, letting Wget finish the work. By contrast, most
   * of the Web browsers require constant user's presence, which can be a great
   * hindrance when transferring a lot of data.
   * <p>
   * Wget can follow links in HTML, XHTML, and CSS pages, to create local
   * versions of remote web sites, fully recreating the directory structure of
   * the original site. This is sometimes referred to as "recursive
   * downloading." While doing that, Wget respects the Robot Exclusion Standard
   * (/robots.txt). Wget can be instructed to convert the links in downloaded
   * files to point at the local files, for offline viewing.
   * <p>
   * Wget has been designed for robustness over slow or unstable network
   * connections; if a download fails due to a network problem, it will keep
   * retrying until the whole file has been retrieved. If the server supports
   * regetting, it will instruct the server to continue the download from where
   * it left off.
   */
  private static final String WGET_BINARY = "/usr/bin/wget";
  /**
   * 5 seconds.
   * <p>
   * The network timeout to seconds. This is equivalent to specifying
   * --dns-timeout, --connect-timeout, and --read-timeout, all at the same time.
   * <p>
   * Set the DNS lookup timeout to seconds seconds. DNS lookups that don't
   * complete within the specified time will fail. By default, there is no
   * timeout on DNS lookups, other than that implemented by system libraries.
   * <p>
   * Set the connect timeout to seconds seconds. TCP connections that take
   * longer to establish will be aborted. By default, there is no connect
   * timeout, other than that implemented by system libraries.
   * <p>
   * Set the read (and write) timeout to seconds seconds. The "time" of this
   * timeout refers to idle time: if, at any point in the download, no data is
   * received for more than the specified number of seconds, reading fails and
   * the download is restarted. This option does not directly affect the
   * duration of the entire download.
   * <p>
   * Of course, the remote server may choose to terminate the connection sooner
   * than this option requires. The default read timeout is 900 seconds.
   */
  private static final int TIMEOUT = 5; // seconds
  /**
   * 1 time(s)
   * <p>
   * Set number of retries to number. Specify 0 or inf for infinite retrying.
   * The default is to retry 20 times, with the exception of fatal errors like
   * "connection refused" or "not found" (404), which are not retried.
   */
  private static final int TRIES = 1;
  /**
   * 2 redirect.
   * <p>
   * Set the maximum number of redirections to follow for a resource. The
   * default is 20, which is usually far more than necessary. However, on those
   * occasions where you want to allow more (or fewer), this is the option to
   * use.
   */
  private static final int REDIRECTS = 2;

  /**
   * Exit Status. Wget may return one of several error codes if it encounters
   * problems. With the exceptions of 0 and 1, the lower-numbered exit codes
   * take precedence over higher-numbered ones, when multiple types of errors
   * are encountered.
   * <ul>
   * <li>0 No problems occurred.</li>
   * <li>1 Generic error code.</li>
   * <li>2 Parse error—for instance, when parsing command-line options, the
   * ‘.wgetrc’ or ‘.netrc’...</li>
   * <li>3 File I/O error.</li>
   * <li>4 Network failure.</li>
   * <li>5 SSL verification failure.</li>
   * <li>6 Username/password authentication failure.</li>
   * <li>7 Protocol errors.</li>
   * <li>8 Server issued an error response. </li>
   * </ul>
   */
  private static final String[] EXIT_STATUS = new String[]{"OK",
                                                           "Generic error",
                                                           "Parse error",
                                                           "File I/O error",
                                                           "Network failure",
                                                           "SSL verification failure",
                                                           "Username/password authentication failure",
                                                           "Protocol errors",
                                                           "Server issued an error response"

  };

  /**
   * Helper method to asynchronously download a collection of data files into a
   * directory.
   * <p>
   * @param sources     a collection of data file URLS (should be unique)
   * @param destination the DIRECTORY into which the downloaded resources are to
   *                    be saved. If null then remote file(s) will be saved into
   *                    a new temporary directory.
   * @param overwrite   indicator than any existing destination file should be
   *                    overwritten
   */
  public static void get(Collection<URL> sources, Path destination, boolean overwrite) {
    for (URL url : sources) {
      try {
        get(url, destination, overwrite);
      } catch (Exception exception) {
        LOGGER.log(Level.SEVERE, "WGET Utility {0}", exception.getMessage());
      }
    }
  }

  /**
   * Download the source file URL into a temporary directory on the local file
   * system.
   * <p>
   * @param url a fully qualified URL pointing to the remote resource to
   *            download
   * @return the extract process status information
   * @throws Exception if the WGET process fails or the destination file cannot
   *                   be written.
   */
  public static Properties get(URL url) throws Exception {
    return get(url, null, true);
  }

  /**
   * Download the source file URL into the destination file location.
   * <p>
   * @param source      a fully qualified URL pointing to the remote resource to
   *                    download
   * @param destination the DIRECTORY into which the downloaded resources are to
   *                    be saved. If null then remote file(s) will be saved into
   *                    a new temporary directory.
   * @param overwrite   indicator than any existing destination file should be
   *                    overwritten
   * @return the extract process status information
   * @throws Exception if the WGET process fails or the destination file cannot
   *                   be written.
   */
  public static Properties get(URL source, Path destination, boolean overwrite) throws Exception {
    long startTime = System.currentTimeMillis();
    Properties status = new Properties();
//    status.setProperty("StartTime", String.valueOf(startTime));
    status.setProperty("source", source.toString());
    /**
     * Create the download directory if it does not already exist.
     */
    Path downloadDir;
    if (destination == null) {
      /**
       * Conditionally create a temporary directory if the destination is not
       * set.
       */
      downloadDir = Files.createTempDirectory("wget-");
    } else {
      downloadDir = destination;
      if (!downloadDir.toFile().exists()) {
        LOGGER.log(Level.FINE, "WGET Utility Creating download directory {0}", downloadDir);
        Files.createDirectories(downloadDir);
      }
    }
    /**
     * Handle if the destination file exists. Remove existing.
     */
    Path destinationFile = downloadDir.resolve(getFileName(source));
//    status.setProperty("Destination", destinationFile.toString());
    if (destinationFile.toFile().exists()) {
      if (overwrite) {
        LOGGER.log(Level.FINEST, "WGET Utility Removing existing file {0}", destinationFile);
        Files.delete(destinationFile);
      } else {
        throw new Exception("WGET Utility is configured NOT to overwrite existing file.");
      }
    }
    /**
     * Download in to a temporary file, then move the file to the destination.
     */
    Path temporaryFile = Files.createTempFile("wget-", null);
    /**
     * Build the download command.
     * <p>
     * Refuse any redirection.
     */
    String wget = new StringBuilder(WGET_BINARY)
      .append(" --quiet ")
      .append(" --max-redirect=").append(REDIRECTS) // allow two redirect
      .append(" --tries=").append(TRIES)
      .append(" --timeout=").append(TIMEOUT)
      .append(" -O ").append(temporaryFile)
      .append(" ").append(source)
      .toString();
    /**
     * Developer note: Build the command as a string, then run the command in a
     * UNIX Shell. This simplifies setting the various command parameters. Also
     * note that on UNIX-like systems the shell only executes programs residing
     * in the current directory if given an unambiguous path to it.
     */
    ProcessBuilder processBuilder = new ProcessBuilder("/bin/sh", "-c", wget);
    /**
     * Developer note: do NOT download into the destination directory. This
     * breaks the application when using a FileWatcher.
     */
//    processBuilder.directory(downloadDir.toFile()); // NO !!!!
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
     * Log the process output with duration. Since the program is configured to
     * run silently only an error would be reported.
     */
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        LOGGER.log(Level.WARNING, "WGET Utility {0}", line);
      }
    }
    /**
     * Move the temporary file to the destination file.
     * <p>
     * Error if the process did not return zero.
     * <p>
     * waitfor() causes the current thread to wait, if necessary, until the
     * process represented by this Process object has terminated. This method
     * returns immediately if the subprocess has already terminated. If the
     * subprocess has not yet terminated, the calling thread will be blocked
     * until the subprocess exits.
     */
    int returnStatus = process.waitFor();
    /**
     * If the WGEt process exited cleanly then pivot (move) the temporary file
     * either to the destination (if configured) or to the (temporary) download
     * directory, preserving the source file name from the URL.
     */
    status.setProperty("status", EXIT_STATUS[returnStatus]);
    if (returnStatus == 0) {
      long size = temporaryFile.toFile().length(); // length in Bytes
      long duration = System.currentTimeMillis() - startTime; // time in Millis
      double speed = (size * 8) / (duration * 1000);  // speed in mbps
      status.setProperty("size", String.valueOf(size));
      status.setProperty("duration", String.valueOf(duration));
      status.setProperty("speed", String.valueOf(speed));
      LOGGER.log(Level.INFO, "WGET Utility {0}", status.toString());
      Files.move(temporaryFile, destinationFile);
    } else {
      temporaryFile.toFile().delete();
      throw new Exception("WGET Utility " + source + "  Error: " + EXIT_STATUS[returnStatus]);
    }
    return status;
  }

  /**
   * Get the file name from a URL. This returns the last string token in the URL
   * path.
   * <p>
   * For example, the URL
   * <code>http://transition.fcc.gov/ftp/Bureaus/MB/Databases/cdbs/am_ant_sys.zip</code>
   * has the path <code>/ftp/Bureaus/MB/Databases/cdbs/am_ant_sys.zip</code> and
   * filename <code>am_ant_sys.zip</code>.
   * <p>
   * @param url the remote file URL
   * @return the file name from the url.
   */
  private static String getFileName(URL url) {
    String[] tokens = url.getPath().split("/");
    return tokens[tokens.length - 1];
  }

  /**
   * Download the source file URL into the destination file location.
   * <p>
   * Developer note: Do not use this method from an EJB. Instead annotate the
   * calling method with an Asynchronous tag.
   * <p>
   * @param destination a fully qualified file path
   * @param source      a fully qualified source file URL
   * @param overwrite   overwrite existing files, if present
   */
  public static void getAsync(URL source, Path destination, boolean overwrite) {
    new Thread(new WGETRunnable(source, destination, overwrite), "get").start();
  }

  /**
   * Internal runnable class to download a file in a new thread.
   */
  private static class WGETRunnable implements Runnable {

    private final boolean overwrite;
    private final URL source;
    private final Path destination;

    public WGETRunnable(URL source, Path destination, boolean overwrite) {
      this.source = source;
      this.destination = destination;
      this.overwrite = overwrite;
    }

    @Override
    public void run() {
      try {
        WGET.get(source, destination, overwrite);
      } catch (Exception ex) {
        LOGGER.log(Level.SEVERE, "WGET Utility ASYNC failed to retrieve {0}: {1}", new Object[]{source, ex.getMessage()});
      }
    }

  }

  /**
   * EXIT STATUS
   * <p>
   * Wget may return one of several error codes if it encounters problems. With
   * the exceptions of 0 and 1, the lower-numbered exit codes take precedence
   * over higher-numbered ones, when multiple types of errors are encountered.
   * <p>
   * In versions of Wget prior to 1.12, Wget's exit status tended to be
   * unhelpful and inconsistent. Recursive downloads would virtually always
   * return 0 (success), regardless of any issues encountered, and non-
   * recursive fetches only returned the status corresponding to the most
   * recently-attempted download.
   */
  public static enum ExitStatus {
    OK(0, "No problems occurred."),
    ERROR(1, "Generic error code."),
    PARSE_ERROR(2, "Command parse error."),
    FILE_ERROR(3, "File I/O error."),
    NETWORK_FAILURE(4, "Network failure."),
    SSL_VERIFICATION(5, " SSL verification failure."),
    AUTHENTICATION_FAILURE(6, "Username/password authentication failure."),
    PROTOCOL_ERROR(7, "Protocol errors."),
    SERVER_ERROR(8, "Server issued an error response.");

    private final int code;
    private final String description;

    private ExitStatus(int exitStatus, String description) {
      this.code = exitStatus;
      this.description = description;
    }

    /**
     * Get a return type from the exit status code.
     *
     * @param code the exit status code
     * @return the return type.
     */
    public static ExitStatus fromCode(int code) {
      for (ExitStatus type : ExitStatus.values()) {
        if (type.getCode() == code) {
          return type;
        }
      }
      throw new IllegalArgumentException("Unrecognized exit status code: " + code);
    }

    public int getCode() {
      return code;
    }

    public String getDescription() {
      return description;
    }

  }
}
