package sort.test;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
/**
 * Created by George on 2017/5/4.
 */
public class SortReducer extends Reducer<TextInt, IntWritable, Text, Text>{

    @Override
    protected void reduce(TextInt textInt, Iterable<IntWritable> values,
                          Context context)
            throws IOException, InterruptedException {

        StringBuffer stringCombine = new StringBuffer();
        Iterator<IntWritable> itr = values.iterator();
        while(itr.hasNext())
        {
            int value = itr.next().get();
            stringCombine.append(value + ",");
        }
        int length = stringCombine.length();
        if(length > 0)
            stringCombine.deleteCharAt(length - 1);
        context.write(new Text(textInt.getStr()), new Text(stringCombine.toString()));
    }
}

