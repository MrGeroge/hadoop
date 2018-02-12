package temperature;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by George on 2017/5/16.
 */
public class TemperatureReducer extends Reducer<TemperaturePair,Text,Text,Text> {
    @Override
    protected void reduce(TemperaturePair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //context.write(new Text(key.getYear()+""),values.iterator().next());
        for(Text t:values){
            context.write(new Text(key.getYear()+""),t);
            return;
        }    }
}
