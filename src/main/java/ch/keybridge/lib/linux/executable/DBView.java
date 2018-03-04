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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility class to wrap the <code>dbview</code> command in a Java process and
 * make that program accessible from within JavaEE ETL business logic.
 * <p>
 * This utility requires that the 'dbview' executable is installed on the host
 * system and the <code>dbview</code> program is available and executable.
 * <p>
 * Dbview is a little tool that will display dBase III files. You can also use
 * it to convert your old .dbf files for further use with Unix. It should also
 * work with dBase IV files, but this is mostly untested.
 * <p>
 * dBase (also stylized dBASE) was one of the first database management systems
 * for microcomputers. . dBase's underlying file format, the .dbf file, is
 * widely used in applications needing a simple format to store structured data.
 * <p>
 * DbView is a lightweight tool that you can use to open and view DBF database
 * files.
 * <p>
 * With it it’s easy to view the contents of a DBF file and even edit entries if
 * it is permitted. The data is neatly organized in a table form and is easy to
 * read.
 * <p>
 * DbView displays a user-friendly interface and provides quick access to all
 * its functions and features right from the main window. You can view its
 * structure and obtain the array, as well as generate an SQL view. It’s also
 * possible to get a clipper-like array in relation to field names and length.
 * <p>
 * You are able to add filters, apply a quick selection to all or specific
 * sections of data and go to a certain record. It’s possible to copy and paste
 * content, as well as it is to delete and recall it. DbView also allows you to
 * export the database and enables you to choose the string format.
 * <p>
 * The <code>dbview</code> command is configured by entries in the preferences
 * container, which are typically loaded from a file. Following is a partial
 * list of preferences entries:
 * <p>
 * @author jesse 08/24/15
 */
public class DBView {

  private static final Logger LOGGER = Logger.getLogger(DBView.class.getName());

  /**
   * Path to the dbview executable.
   */
  private static final String DBVIEW_BINARY = "/usr/bin/dbview";

  /**
   * Convert dBase III data files into colon (":") delimited text files.
   * <p>
   * The converted data files are written into the SAME directory as the source
   * data files with a ".dat" extensions.
   * <p>
   * @param path a dBase III data file fully qualified path reference
   * @return the converted data file path reference
   * @throws Exception if the data file is not dBaseIII compatible; the dbview
   *                   process fails or does not exit cleanly
   */
  public static Path convert(Path path) throws Exception {
    /**
     * Add a hook to intercept dBase III files and convert them into usable data
     * files that can be loaded into MySQL. Only accept .dbf files. Note that
     * .dbt files are dBase I but are also not supported.
     */
    if (path.toString().toLowerCase().endsWith(".dbf")) {
      LOGGER.log(Level.FINE,
                 "Converting dBase III {0} to text {1}",
                 new Object[]{path.getFileName(), path.getFileName().toString().toLowerCase().replace(".dbf", ".dat")});
    } else {
      throw new Exception(path.getFileName() + " is not a dBase III file.");
    }
    /**
     * DBView options:
     * <p>
     * --browse, -b : switches into browse mode. Using this mode no field names
     * will be displayed, instead every record will displayed in one line using
     * a delimiter to separate fields.
     * <p>
     * --delimiter, -d delimiter. The default delimiter in browse mode is the
     * colon sign ``:''. This parameter overrides it. This can be useful
     * especially if you plan to examine the output with scripts.
     * <p>
     * --trim, -t : When this option is specified, leading and trailing spaces
     * are omitted. This might be useful when in browse mode.
     */
    StringBuilder dbview = new StringBuilder(DBVIEW_BINARY)
            .append(" -b ")
            .append(" -t ")
            .append(path.getFileName())
            .append(" > ")
            .append(path.getFileName().toString().toLowerCase().replace(".dbf", ".dat"));
    ProcessBuilder processBuilder = new ProcessBuilder("/bin/sh", "-c", dbview.toString());
    processBuilder.directory(path.getParent().toFile());
    processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
    processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
    Process process = processBuilder.start();
    /**
     * Log the process output.
     */
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        LOGGER.log(Level.INFO, "dbview: {0}.", line);
      }
    }
    /**
     * Return TRUE if the process returned ONE. Causes the current thread to
     * wait, if necessary, until the process represented by this Process object
     * has terminated. This method returns immediately if the subprocess has
     * already terminated. If the subprocess has not yet terminated, the calling
     * thread will be blocked until the subprocess exits.
     */
    if (process.waitFor() != 1) {
      throw new Exception("DBView process did not exit cleanly. " + process.exitValue());
    }
    /**
     * The process exited OK.
     */
    return Paths.get(path.toString().toLowerCase().replace(".dbf", ".dat"));
  }
}
