import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * Created by George on 2017/5/2.
 */
public class HadoopFileOperation {//HDFS基本文件操作
    public static void main(String[] args) throws Exception{
        Configuration conf=new Configuration();
        FileSystem fs=FileSystem.get(new URI("hdfs://localhost:9000"),conf,"root");
        fs.mkdirs(new Path("/cq"));
        fs.close();
    }
}
