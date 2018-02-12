package AdRecommend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by George on 2017/5/17.
 */
public class WReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb=new StringBuffer();
        for(Text t:values){
            sb.append(t.toString()+" ");
        }
        context.write(key,new Text(sb.toString()));
    }
}
