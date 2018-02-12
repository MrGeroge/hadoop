import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * Created by George on 2017/5/3.
 */
public class FindFIleOnHDFS {
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        FileSystem fs=FileSystem.get(new URI("hdfs://localhost:9000"),conf,"root");
        BlockLocation[] locations=fs.getFileBlockLocations(new Path("/cq"),0,88);
        for(BlockLocation bl:locations){
            String[] hosts=bl.getHosts();
            String[] names=bl.getNames();
            System.out.println("block0 at "+hosts[0]);
            System.out.println("block size="+bl.getLength());
            System.out.println("block name="+names[0]);
            System.out.println("block offset"+bl.getOffset());
        }
       fs.close();
    }
}
