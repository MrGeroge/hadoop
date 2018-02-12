import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * Created by George on 2017/5/2.
 */
public class HadoopIOOperation {//HDFS IO操作
    private FileSystem fs;
    @Before
    public void init()throws Exception{
        Configuration conf=new Configuration();
        fs=FileSystem.get(new URI("hdfs://localhost:9000"),conf,"root");
    }
    @Test
    public void test1() throws Exception {
        FSDataInputStream in=fs.open(new Path("/cq"));
        FileOutputStream out=new FileOutputStream("/Users/George/Desktop/net2.txt");
        IOUtils.copyBytes(in,out,4096);
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
        fs.close();
    }
    @Test
    public void test2() throws Exception {
        FileInputStream in=new FileInputStream("/Users/George/Desktop/net2.txt");
        FSDataOutputStream out=fs.create(new Path("/cq/net"));
        fs.setReplication(new Path("/cq/net"), (short) 1);
        IOUtils.copyBytes(in,out,4096);
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
        fs.close();
    }
}
