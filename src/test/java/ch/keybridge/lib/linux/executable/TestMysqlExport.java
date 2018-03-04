/*
 *  Copyright (C) 2014 Caulfield IP Holdings (Caulfield) and/or its affiliates.
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

import java.util.Properties;

/**
 *
 * @author Jesse Caulfield 
 */
public class TestMysqlExport {

  public static void main(String args[]) throws Exception {
    Properties p = new Properties();
    p.setProperty("mysql.database", "wsif");
    p.setProperty("mysql.host", "localhost");
    p.setProperty("mysql.user", "USERNAME");
    p.setProperty("mysql.pass", "PASSWORD");

    MySQLExport export = new MySQLExport(p);

    StringBuilder mysqlexport = new StringBuilder();
    mysqlexport
            //      .append(" --execute='")
            .append("SELECT a.call_sign, w.uuid, w.registrar, s.site_number, c.latitude, c.longitude, w.service_type, w.data_source, w.data_source_id, w.date_last_updated, d.channel, d.antenna_hag, d.antenna_haat, d.erp, t.name, t.channel, tc.latitude, tc.longitude")
            .append(" FROM wireless_service w, license a, station s, location l, coordinate c, device d")
            .append(" LEFT JOIN (device t, station ts, location tl, coordinate tc) ON (t.id = d.transmitter AND t.station = ts.id AND ts.location = tl.id AND tl.coordinate = tc.id)")
            .append(" WHERE w.license = a.id AND s.wireless_service = w.id AND s.location = l.id AND l.coordinate = c.id AND d.station = s.id")
            .append(";");

//    export.export(new File("/tmp"));
  }

}
