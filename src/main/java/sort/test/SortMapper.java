package sort.test;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by George on 2017/5/4.
 */
public class SortMapper extends Mapper<Object, Text, TextInt, IntWritable>{

    public TextInt textInt = new TextInt();
    public IntWritable intp = new IntWritable(0);

    @Override
    protected void map(Object key, Text value,
                       Context context)
            throws IOException, InterruptedException {
        int i =  Integer.parseInt(value.toString());

        textInt.setStr(key.toString());
        textInt.setValue(i);
        intp.set(i);
        context.write(textInt,intp);
    }

}
