/*
 *  Copyright (C) 2015 Caulfield IP Holdings (Caulfield) and/or its affiliates.
 *  All rights reserved. Use is subject to license terms.
 *
 *  Software Code is protected by Caulfield Copyrights. Caulfield hereby reserves
 *  all rights in and to Caulfield Copyrights and no license is granted under
 *  Caulfield Copyrights in this Software License Agreement. Caulfield generally
 *  licenses Caulfield Copyrights for commercialization pursuant to the terms of
 *  either Caulfield's Standard Software Source Code License Agreement or
 *  Caulfield's Standard Product License Agreement.
 *
 *  A copy of either License Agreement can be obtained on request by email from:
 *  info@caufield.org.
 */
package ch.keybridge.lib.linux.executable;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 *
 * @author Jesse Caulfield
 */
public class WGETTest {

  public void testVoid() {
    System.out.println("  WGET Utility tests disabled - enable as required");
  }

  public void _testMulti() throws MalformedURLException, InterruptedException {
    System.out.println("Executing Test Multi from transition.fcc.gov");
    /**
     * Disable by default - enable to test CDBS access
     */
    Set<URL> urls = new HashSet<>();
    for (String file : new String[]{"am_augs.zip", "am_eng_data.zip", "am_towers.zip", "ant_make.zip", "ant_pattern.zip"}) {
      urls.add(new URL("http://transition.fcc.gov/ftp/Bureaus/MB/Databases/cdbs/" + file));
    }
    assertFalse(urls.isEmpty());
    /**
     * Get async.
     */
    WGET.get(urls, Paths.get("/tmp", "etl", "fcc_cdbs", "zip"), true);
//    System.out.println("  multi sleeping 15 seconds");    Thread.sleep(15000);    System.out.println("  multi waking up");
  }

  public void _testGetFail() throws MalformedURLException, Exception {
    System.out.println("Executing Test GET FAIL");
    /**
     * Confirm get initial.
     */
    URL url = new URL("http://transition.fcc.gov/ftp/Bureaus/MB/Databases/cdbs/am_ant_sys.zip");
    Properties status = WGET.get(url, Paths.get("/tmp/wget-cdbs"), true);
//    assertNotNull(path);
//    assertTrue(path.toFile().exists());
//    assertTrue(path.toFile().length() > 0);
    System.out.println("  retrieve " + url + "  OK");
    /**
     * Confirm fail to overwrite.
     */
    try {
      WGET.get(url, Paths.get("/tmp/wget-cdbs"), false);
      fail("WGET overwrite is disabled - should fail");
    } catch (Exception exception) {
      System.out.println("  fail OK: " + exception.getMessage());
    }
  }

  public void _testGet() throws MalformedURLException, Exception {
    System.out.println("Executing Test GET from Industry Canada");
    URL url = new URL("http://spectrum.ic.gc.ca/engineering/engdoc/baserad.zip");
    Properties baserad = WGET.get(url);
//    assertNotNull(baserad);
//    assertTrue(baserad.toFile().exists());
    System.out.println("  retrieve " + url + "  OK");
  }

}
