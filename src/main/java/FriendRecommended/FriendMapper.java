package FriendRecommended;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

/**
 * Created by George on 2017/5/11.
 */
public class FriendMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] strs=line.split("\t");
        context.write(new Text(strs[0]),new Text(strs[1]));
        context.write(new Text(strs[1]),new Text(strs[0]));
    }
}
