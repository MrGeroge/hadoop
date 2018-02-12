package temperature;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by George on 2017/5/16.
 */
public class TemperaturePartition extends Partitioner<TemperaturePair,Text> {
    @Override
    public int getPartition(TemperaturePair temperaturePair, Text text, int i) {
        return (temperaturePair.getYear() *127) % i;
    }
}
