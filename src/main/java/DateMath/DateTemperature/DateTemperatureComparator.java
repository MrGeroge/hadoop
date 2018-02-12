package DateMath.DateTemperature;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by George on 2017/11/22.
 */
public class DateTemperatureComparator extends WritableComparator {
    protected DateTemperatureComparator(){
        super(DateTemperaturePair.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        DateTemperaturePair pair1=(DateTemperaturePair)a;
        DateTemperaturePair pair2=(DateTemperaturePair)b;
        return pair1.compareTo(pair2);
    }
}
