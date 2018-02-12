/**
 * Created by George on 2017/11/24.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
//import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Random;

public class TestFavoredNodesEndToEnd { //测试特定文件写到指定节点
    //private static MiniDFSCluster cluster;
  private static Configuration conf;
  private final static int NUM_DATA_NODES = 10;
  private final static int NUM_FILES = 4; //文件数
  private final static byte[] SOME_BYTES = new String("foo").getBytes();
  private static DistributedFileSystem dfs;
  private static ArrayList<DataNode> datanodes;

          @BeforeClass
  /*public static void setUpBeforeClass() throws Exception {
            conf = new Configuration();
            cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATA_NODES)
                       .build();
            cluster.waitClusterUp();
            dfs = cluster.getFileSystem();
            datanodes = cluster.getDataNodes();
         }

          @AfterClass
  public static void tearDownAfterClass() throws Exception {
            if (cluster != null) {
                  cluster.shutdown();
                }
          }*/

          @Test
  public void testFavoredNodesEndToEnd() throws Exception {
            //create 10 files with random preferred nodes
                   for (int i = 0; i < NUM_FILES; i++) {
                  Random rand = new Random(System.currentTimeMillis() + i);
                  //pass a new created rand so as to get a uniform distribution each time
                          //without too much collisions (look at the do-while loop in getDatanodes)
                                  InetSocketAddress datanode[] = getDatanodes(rand); //datanode节点地址
                  Path p = new Path("test/users"+i); //节点
                  FSDataOutputStream out = dfs.create(p, FsPermission.getDefault(), true,
                              4096, (short)3, (long)4096, null, datanode);
                  out.write(SOME_BYTES);
                  out.close();
                  BlockLocation[] locations =
                              dfs.getClient().getBlockLocations(p.toUri().getPath(), 0,
                                          Long.MAX_VALUE);
                  //make sure we have exactly one block location, and three hosts
                          //assertTrue(locations.length == 1 && locations[0].getHosts().length == 3);
                  //verify the files got created in the right nodes
                          for (BlockLocation loc : locations) {
                        String[] hosts = loc.getNames();
                        String[] hosts1 = getStringForInetSocketAddrs(datanode);
                        //assertTrue(compareNodes(hosts, hosts1));
                      }
                }
          }

         /* @Test
  public void testWhenFavoredNodesNotPresent() throws Exception {
            //when we ask for favored nodes but the nodes are not there, we should
                    //get some other nodes. In other words, the write to hdfs should not fail
                            //and if we do getBlockLocations on the file, we should see one blklocation
                                    //and three hosts for that
                                            Random rand = new Random(System.currentTimeMillis());
            InetSocketAddress arbitraryAddrs[] = new InetSocketAddress[3];
            for (int i = 0; i < 3; i++) {
                  arbitraryAddrs[i] = getArbitraryLocalHostAddr();
                }
            Path p = new Path("/filename-foo-bar");
            FSDataOutputStream out = dfs.create(p, FsPermission.getDefault(), true,
                        4096, (short)3, (long)4096, null, arbitraryAddrs);
            out.write(SOME_BYTES);
            out.close();
            BlockLocation[] locations =
                        dfs.getClient().getBlockLocations(p.toUri().getPath(), 0,
                                    Long.MAX_VALUE);
            assertTrue(locations.length == 1 && locations[0].getHosts().length == 3);
          }*/


          private String[] getStringForInetSocketAddrs(InetSocketAddress[] datanode) {//datanode地址转换为字符串
            String strs[] = new String[datanode.length];
            for (int i = 0; i < datanode.length; i++) {
                  strs[i] = datanode[i].getAddress().getHostAddress() + ":" +
                    +       datanode[i].getPort();
                }
            return strs;
          }



          private InetSocketAddress[] getDatanodes(Random rand) {
            //Get some unique random indexes
                    int idx1 = rand.nextInt(NUM_DATA_NODES);
           int idx2;

                    do {
                  idx2 = rand.nextInt(NUM_DATA_NODES);
                } while (idx1 == idx2);

                    int idx3;
            do {
                  idx3 = rand.nextInt(NUM_DATA_NODES);
                } while (idx2 == idx3 || idx1 == idx3);

                    InetSocketAddress[] addrs = new InetSocketAddress[3];
            addrs[0] = datanodes.get(idx1).getXferAddress();
            addrs[1] = datanodes.get(idx2).getXferAddress();
            addrs[2] = datanodes.get(idx3).getXferAddress();
           return addrs;
          }
}
