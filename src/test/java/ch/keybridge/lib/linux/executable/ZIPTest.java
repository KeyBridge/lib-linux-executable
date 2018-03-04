/*
 * Copyright 2016 Key Bridge.
 *
 * All rights reserved. Use is subject to license terms.
 * This software is protected by copyright.
 *
 * See the License for specific language governing permissions and
 * limitations under the License.
 */
package ch.keybridge.lib.linux.executable;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 *
 * @author Key Bridge LLC
 */
public class ZIPTest {

  public void _testSomeMethod() throws IOException {

    Path archive = Paths.get("/tmp/etl", "facility.zip");

    Path unarchive = archive.getParent().resolve("facility");

    ZIP.unzip(archive, unarchive);

  }

}
