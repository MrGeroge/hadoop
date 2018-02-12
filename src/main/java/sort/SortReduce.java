package sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by George on 2017/5/4.
 */
public class SortReduce extends Reducer<StringIntPair,IntWritable,Text,Text>{
    @Override
    protected void reduce(StringIntPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        StringBuffer stringBuffer=new StringBuffer();
        Iterator<IntWritable> it = values.iterator();
        while(it.hasNext()){
            stringBuffer.append(it.next()+",");
        }
        String result=new String();
        result=stringBuffer.toString().substring(0,stringBuffer.length()-1);
        context.write(new Text(key.getKey()),new Text(result));
    }
}
