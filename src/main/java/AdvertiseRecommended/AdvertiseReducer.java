package AdvertiseRecommended;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by George on 2017/5/11.
 */
public class AdvertiseReducer extends Reducer<AdvertisePair,Text,Text,Text>{
    @Override
    protected void reduce(AdvertisePair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> it=values.iterator();
        while(it.hasNext()){
            context.write(new Text(key.getKey()),it.next());
        }
    }
}
