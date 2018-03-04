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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * Utility class to wrap <code>zip</code> file archive capabilities.
 * <p>
 * zip is a compression and file packaging utility. It is compatible with PKZIP
 * (Phil Katz's ZIP for MSDOS systems).
 * <p>
 * @author Jesse Caulfield
 */
public class ZIP {

  private static final Logger LOGGER = Logger.getLogger(ZIP.class.getName());

  /**
   * Unzip a ZIP file and save its contents into the designated output
   * directory.
   * <p>
   * @param zipfile        the ZIP archive file. Any file not ending in 'zip'
   *                       (not case sensitive) throws an error.
   * @param unZipDirectory (optional) the directory into which the un-archived
   *                       content should be placed. If not set then a new
   *                       temporary directory is automatically created.
   * @return the output directory where the ZIP archive contents have been
   *         expanded
   * @throws FileNotFoundException if the zip file is not present
   * @throws IOException           if the zip file cannot be opened or the
   *                               output directory is not present and cannot be
   *                               created
   */
  @SuppressWarnings({"NestedAssignment", "ConvertToTryWithResources"})
  public static Properties unzip(Path zipfile, Path unZipDirectory) throws FileNotFoundException, IOException {
    if (!zipfile.toString().toLowerCase().endsWith(".zip")) {
      throw new IOException(zipfile + " does not appear to be a zip file.");
    }
    /**
     * Begin recording the un-archive process.
     */
    long startTime = System.currentTimeMillis();
    Properties status = new Properties();
    status.setProperty("Source", zipfile.toFile().getName());
    status.setProperty("Size", String.valueOf(zipfile.toFile().length()));
    int fileCount = 0; // number of archived files in this package.
    /**
     * If no UnZIP directory is declared then create a temporary extraction
     * directory into which the zip archive content is to be expanded. This
     * should be deleted when finished processing.
     */
//    Path archivePath = Files.createTempDirectory("zip-", PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxrwxr-x")));
    Path archivePath = unZipDirectory != null ? unZipDirectory : Files.createTempDirectory("zip-");

    ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(zipfile.toFile()));
    ZipEntry zipEntry;
    byte[] buffer = new byte[1024];
    while ((zipEntry = zipInputStream.getNextEntry()) != null) {
      /**
       * First Extract to a temporary file, then move (pivot) the temporary file
       * to the destination.
       */
      Path tempFile = Files.createTempFile("unzip", "tmp");
      FileOutputStream fileOutputStream = new FileOutputStream(tempFile.toFile());
      int len;
      while ((len = zipInputStream.read(buffer)) > 0) {
        fileOutputStream.write(buffer, 0, len);
      }
      fileOutputStream.close();
      /**
       * Unzip all files into the same directory. If the zipEntry contains a
       * directory (e.g. the "/" character) then parse the name to get the base
       * file name.
       * <p>
       * Output the ZIP entry contents to a LOWER CASE file.
       */
      Path outputFile = archivePath.resolve(zipEntry.getName().toLowerCase());
      if (zipEntry.getName().contains(File.separator)) {
        LOGGER.log(Level.FINE, "Zip file {0} contains a path. Extracting to top level directory.", zipEntry.getName());
        String[] names = zipEntry.getName().split(File.separator);
        outputFile = archivePath.resolve(names[names.length - 1].toLowerCase());
      }
      /**
       * Create the output directory if required.
       */
      if (!archivePath.toFile().exists()) {
        LOGGER.log(Level.FINE, "Zip create output directory {0}.", archivePath);
        Files.createDirectories(archivePath, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxrwxr-x")));
      }
      /**
       * Pivot (move) the temp file to the destination file.
       */
      Files.move(tempFile, outputFile, StandardCopyOption.REPLACE_EXISTING);
      fileCount++;
    }
    /**
     * Unarchive complete.
     */
    zipInputStream.closeEntry();
    zipInputStream.close();
    /**
     * Record the duration. Then done.
     */
    status.setProperty("Files", String.valueOf(fileCount));
    status.setProperty("Duration", String.valueOf(System.currentTimeMillis() - startTime));
    return status;
  }

  /**
   * Create a new (temporary) ZIP archive file containing all files in a source
   * directory using ZipOutputStream. Builds a ZIP file containing all the files
   * in the indicated directory.
   * <p>
   * The created ZIP archive file is created as a temporary file and should be
   * copied, renamed, moved and otherwise post-processed as needed.
   *
   * @param sourceDirectory the directory containing file contents to write into
   *                        a new ZIP archive file.
   * @return a Path to the new ZIP archive file
   * @throws IOException if the new ZIP archive file cannot be created
   *                     (permission error) or if data cannot be written (disk
   *                     error)
   */
  public static Path zip(Path sourceDirectory) throws IOException {
    /**
     * Create a zip file with the default file name. To create the zip file use
     * the ZipOutputStream(OutputStream out) constructor of ZipOutputStream
     * class.
     */
    Path zipArchive = Files.createTempFile("zip-", "zip");
    try (ZipOutputStream zipOutputStream = new ZipOutputStream(new FileOutputStream(zipArchive.toFile()))) {
      int bytesRead;
      byte[] buffer = new byte[1024];
      /**
       * Iterate through all the source directory files.
       */
      for (File sourceFile : sourceDirectory.toFile().listFiles()) {
        try (BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(sourceFile))) {
          ZipEntry zipEntry = new ZipEntry(sourceFile.getName());
          zipOutputStream.putNextEntry(zipEntry);
          /*
           * After creating an entry in the zip file, actually write the file
           * data.
           */
          while ((bytesRead = bufferedInputStream.read(buffer)) > 0) {
            zipOutputStream.write(buffer, 0, bytesRead);
          }
          /*
           * After writing the file to ZipOutputStream, use void closeEntry()
           * method of ZipOutputStream class to close the current entry and
           * position the stream to write the next entry.
           */
          zipOutputStream.closeEntry();
        }
      }
      /**
       * Closes the ZIP output stream as well as the stream being filtered.
       */
      zipOutputStream.close();
    } // end try with resources
    return zipArchive;
  }

  /**
   * Recursively remote a (temporary) path and all of its contents. Includes a
   * safety check to only remove resources under the system temporary path.
   * <p>
   * @param path a pointer to the path to be removed..
   */
  private static void remove(Path path) {
    /**
     * Failsafe - do not purge directories outside the temporary path.
     */
    if (!path.startsWith(Paths.get(System.getProperty("java.io.tmpdir")))) {
      throw new IllegalArgumentException("Invalid attempt to remove a file/directory outside the temporary path: " + path);
    }
    LOGGER.log(Level.FINEST, "Removing temporary directory {0}", path);
    if (path.toFile().isDirectory()) {
      for (File file : path.toFile().listFiles()) {
        if (file.isDirectory()) {
          remove(file.toPath());
        }
        file.delete();
      }
    }
    path.toFile().delete();
  }
}
