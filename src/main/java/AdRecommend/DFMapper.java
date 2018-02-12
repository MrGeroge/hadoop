package AdRecommend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by George on 2017/5/17.
 */
public class DFMapper extends Mapper<Object,Text,Text,IntWritable> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit split=(FileSplit)context.getInputSplit();
        String path=split.getPath().getName();
        if(path.contains("part-r-00000")){
            String[] strs=value.toString().split("\t");
            String[] words=strs[0].split("_");
            context.write(new Text(words[0]),new IntWritable(1));
        }
        else{
            System.out.println("count sum="+value.toString().split("\t")[1]);
        }
    }
}
