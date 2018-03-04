# lib-linux-executable

**Linux Executables**

A collection of Java process wrappers for commonly used native Linux 
commands; such as WGET, ZIP, MySQL, MySQLImport, etc.


# Utilities

 * DBView
 
Dbview is a tool that will display dBase III files. You can also use
it to convert your old .dbf files for further use with Unix. It should also
work with dBase IV files, but this is mostly untested.
This utility wraps the <code>dbview</code> command in a Java process and
make that program accessible from within JavaEE ETL business logic.
This utility requires that the 'dbview' executable is installed on the host
system and the <code>dbview</code> program is available and executable.


  * MySQL

Utility class to wrap the <code>mysql</code> command in a Java process and
make that program accessible from within JavaEE ETL business logic.
This utility requires that a MySQL client is installed on the host system and
the <code>mysql</code> program is available and executable.


  * MySQLExport
  
Utility class to wrap the <code>mysql</code> command in a Java process and
make that program accessible from within JavaEE ETL business logic.

This utility provides a convenient method to export an entire table to a text file.

  * MySQLImport
  
Utility class to wrap the <code>mysqlimport</code> command in a Java process
and make that program accessible from within JavaEE ETL business logic.
This utility requires that a MySQL client is installed on the host system and
the <code>mysqlimport</code> program is available and executable.
The <code>mysqlimport</code> command is configured by entries in the
preferences container, which are typically loaded from a file.

  * WGET

Utility class to wrap the <code>wget</code> command in a Java process and
make that program accessible from within JavaEE ETL business logic.
GNU Wget is a free utility for non-interactive download of files from the
Web. It supports HTTP, HTTPS, and FTP protocols, as well as retrieval through
HTTP proxies.
Wget is non-interactive, meaning that it can work in the background, while
the user is not logged on. This allows you to start a retrieval and
disconnect from the system, letting Wget finish the work. By contrast, most
of the Web browsers require constant user's presence, which can be a great
hindrance when transferring a lot of data.

  * XmlLint

  Utility class to wrap the Unix <code>xmllint</code> command in a Java process
  and make that program accessible for XML document testing and schema
  validation.

  <em>From http://xmlsoft.org/xmldtd.html#validate</em>

DTD is the acronym for Document Type Definition. This is a description of the
content for a family of XML files. This is part of the XML 1.0 specification,
and allows one to describe and verify that a given document instance conforms
to the set of rules detailing its structure and content.
Validation is the process of checking a document against a DTD (more
generally against a set of construction rules).

  * Zip
  
Utility class to wrap <code>zip</code> file archive capabilities.
zip is a compression and file packaging utility. It is compatible with PKZIP
(Phil Katz's ZIP for MSDOS systems).


# Example Usage


```java
  Properties properties = new Properties();
  properties.setProperty("mysql.host", "localhost");
  properties.setProperty("mysql.user", "USERNAME");
  properties.setProperty("mysql.pass", "PASSWORD");
  properties.setProperty("mysql.database", "fcc_cdbs");

  MySQL mySQL = MySQL.getInstance(properties);

  String result = mySQL.select("show tables");
  System.out.println("RESULT");
  System.out.println(result);

  List<String> strings = Arrays.asList(result.split("\n"));
  System.out.println(strings);

```








# History
    v1.1.0 - first release
    v1.2.0 - use temporary files, then pivot when done
    v1.3.0 - update mysqlImport and mysql executables, use properties, improve stability
    v1.3.1 - return properties result from WGET and MySQL execution
    v1.3.2 - return properties result from ZIP unarchive method
    v1.3.3 - rename from lib-linux-utility t0 lib-linux-executable
    v1.4.0 - refactor containing package, rename some classes

  
  
# License

Apache 2.0


