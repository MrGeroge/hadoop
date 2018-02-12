import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.net.URI;

/**
 * Created by George on 2017/5/3.
 */
public class HadoopJarTest {
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
        FileSystem fs=FileSystem.get(new URI("hdfs://localhost:9000"),conf,"root");
        fs.mkdirs(new Path("/cq"));
        fs.close();
    }
}
