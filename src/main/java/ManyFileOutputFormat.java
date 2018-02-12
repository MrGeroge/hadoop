import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;

/**
 * Created by George on 2017/5/3.
 */
public abstract class ManyFileOutputFormat extends MultipleOutputFormat<Text,IntWritable>{
    @Override
    protected String generateFileNameForKeyValue(Text key, IntWritable value, String name) {//根据key生成多个文件名
        //return super.generateFileNameForKeyValue(key, value, name);
        char c=key.toString().toLowerCase().charAt(0);
        if(c>='a'&&c<='z'){
            return c+".txt";
        }
        else{
            return "other.txt";
        }
    }
}
