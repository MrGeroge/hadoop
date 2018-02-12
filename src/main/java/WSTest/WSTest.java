package WSTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by George on 2017/11/27.
 */
public class WSTest {
    public static void main(String[] args) throws IOException {
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(config);
        DistributedFileSystem hdfs = (DistributedFileSystem) fs; //得到当前分布式上下文
        DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();  //获得DataNode节点信息
        Path paths[] = new Path[]{new Path("hdfs://localhost:9000/ws/text1"), new Path("hdfs://localhost:9000/ws/text2"), new Path("hdfs://localhost:9000/ws/text3"), new Path("hdfs://localhost:9000/ws/text4")}; //待写入路径
        for (int i = 0; i < dataNodeStats.length; i++) {
            System.out.println("DataNode_" + i + "_Name:" + dataNodeStats[i].getHostName()); //打印出每个DataNode地址和端口
        }

        InetSocketAddress[] nodes = new InetSocketAddress[4];
        for(int i=0;i<4;i++){
            nodes[i]=new InetSocketAddress(dataNodeStats[i].getIpAddr(),dataNodeStats[i].getXferPort());
        }
        for (int i = 0; i < 4; i++) {
            InetSocketAddress[] dataNode = new InetSocketAddress[1];
            dataNode[0] = nodes[i];
            FSDataOutputStream out = hdfs.create(paths[i], FsPermission.getDefault(), true, 4096, (short) 1, (long) 4096, null, dataNode); //创建HDFS输出流
            out.writeUTF("text:" + (i + 1) + "#############################");
            out.flush();
            out.close();
        }
        //WEB UI http://localhost:50070 观察是否text1,text2,text3,text4是否写入HDFS中，且判断每个文件是否写入对应的DataNode
        boolean[] isLocated=new boolean[]{false,false,false,false}; //文件是否在相应的节点
        for (int i = 0; i < 4; i++) {
            BlockLocation[] locations =
                    hdfs.getClient().getBlockLocations(paths[i].toUri().getPath(), 0,
                            Long.MAX_VALUE);  //文件对应的块地址
            String datanode=getStringForInetSocketAddrs(nodes[i]); //获得DateNode地址
            if(locations[0].getNames()[0].equalsIgnoreCase(datanode)){
                isLocated[i]=true;
            }
            else{
                continue;
            }
        }
    }
    public static String getStringForInetSocketAddrs(InetSocketAddress datanode) {

            return  datanode.getAddress().getHostAddress() + ":" +
                           datanode.getPort();

          }
}
