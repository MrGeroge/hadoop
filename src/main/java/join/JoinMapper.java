package join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by George on 2017/5/4.
 */
public class JoinMapper extends Mapper<Object,Text,JoinPair,Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        JoinPair joinPair=new JoinPair();
        FileSplit fileSplit=(FileSplit)context.getInputSplit();//得到切片信息
        String path=fileSplit.getPath().toString();//得到文件路径
        System.out.println("key="+key.toString()+"  value="+value.toString()+"  path="+path);
        System.out.println("################################################");
        if(path.indexOf("application")>0){
            joinPair.setId(0);
            joinPair.setProduct(key.toString());
        }
        else{
            joinPair.setId(1);
            joinPair.setProduct(key.toString());
        }
        context.write(joinPair,value);
        System.out.println("joinPair="+joinPair.toString());
        System.out.println("################################################");
    }
}
