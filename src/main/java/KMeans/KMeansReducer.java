package KMeans;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by George on 2017/6/5.
 */
public class KMeansReducer extends Reducer<KMeansNode,KMeansNode,Text,Text> {
    @Override
    protected void reduce(KMeansNode key, Iterable<KMeansNode> values, Context context) throws IOException, InterruptedException {
        String outputKey="("+key.getX()+","+key.getY()+")";
        StringBuffer stringBuffer=new StringBuffer();
        for(KMeansNode node:values){
            stringBuffer.append("("+node.getX()+","+node.getY()+")");
        }
        context.write(new Text(outputKey),new Text(stringBuffer.toString()));
    }
}
