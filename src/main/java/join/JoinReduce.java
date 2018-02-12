package join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by George on 2017/5/4.
 */
public class JoinReduce extends Reducer<JoinPair,Text,Text,Text> {
    @Override
    protected void reduce(JoinPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String tradeId=values.iterator().next().toString();
        while(values.iterator().hasNext()){
            String payId=values.iterator().next().toString();
            context.write(new Text(tradeId),new Text(payId));
        }
    }
}
