package AdRecommend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by George on 2017/5/17.
 */
public class WMapper extends Mapper<Object,Text,Text,Text> {
    private Map<String,Integer> DFMap=new HashMap<String,Integer>();
    private int NSum=0;
    @Override
    protected void setup(Context context) {
        URI[] uris= new URI[0];//得到DF和N
        try {
            uris = context.getCacheFiles();
        } catch (IOException e) {
            e.printStackTrace();
        }
        for(URI uri:uris){
            if(uri.getPath().endsWith("part-r-00001")){//N
                Configuration conf=new Configuration();
                FileSystem fs= null;
                try {
                    fs = FileSystem.get(new URI("hdfs://localhost:9000"),conf,"root");
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                }
                FSDataInputStream in= null;
                try {
                    in = fs.open(new Path("/output/part-r-00001"));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                FileOutputStream out= null;
                try {
                    out = new FileOutputStream("/Users/George/Desktop/n");
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                try {
                    IOUtils.copyBytes(in,out,4096);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                IOUtils.closeStream(in);
                IOUtils.closeStream(out);
                try {
                    fs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                BufferedReader br= null;
                try {
                    br = new BufferedReader(new FileReader("/Users/George/Desktop/n"));
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                String line= null;
                try {
                    line = br.readLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if(line.startsWith("count")){
                    String[] strs=line.split("\t");
                    NSum=Integer.parseInt(strs[1]);//N
                }
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else{//DF
                Configuration conf=new Configuration();
                FileSystem fs= null;
                try {
                    fs = FileSystem.get(new URI("hdfs://localhost:9000"),conf,"root");
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                }
                FSDataInputStream in= null;
                try {
                    in = fs.open(new Path("/output1/part-r-00000"));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                FileOutputStream out= null;
                try {
                    out = new FileOutputStream("/Users/George/Desktop/df");
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                try {
                    IOUtils.copyBytes(in,out,4096);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                IOUtils.closeStream(in);
                IOUtils.closeStream(out);
                try {
                    fs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                BufferedReader br= null;
                try {
                    br = new BufferedReader(new FileReader("/Users/George/Desktop/df"));
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                String line;
                try {
                    while((line=br.readLine())!=null){
                        String[] strs=line.split("\t");
                        DFMap.put(strs[0],Integer.parseInt(strs[1]));
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
       String[] strs=value.toString().split("\t");
        String[] words=strs[0].split("_");
        String keyword=words[0];
        String ID=words[1];
        int n=NSum;
        int df=DFMap.get(keyword);
        int tf=Integer.parseInt(strs[1]);
        NumberFormat nf=NumberFormat.getInstance();
        nf.setMaximumFractionDigits(5);
        double w=tf*Math.log(n/df);
        String result=keyword+":"+nf.format(w);
        context.write(new Text(ID),new Text(result));
    }
}
