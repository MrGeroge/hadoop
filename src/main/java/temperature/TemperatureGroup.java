package temperature;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by George on 2017/5/16.
 */
public class TemperatureGroup extends WritableComparator {
    public TemperatureGroup(){
        super(TemperaturePair.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TemperaturePair pair1=(TemperaturePair)a;
        TemperaturePair pair2=(TemperaturePair)b;
        return (pair1.getYear()-pair1.getYear());
    }
}
