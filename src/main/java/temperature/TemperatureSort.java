package temperature;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by George on 2017/5/16.
 */
public class TemperatureSort extends WritableComparator {
   public TemperatureSort(){
       super(TemperaturePair.class,true);
   }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TemperaturePair pair1=(TemperaturePair)a;
        TemperaturePair pair2=(TemperaturePair)b;
        int res=pair1.getYear()-pair2.getYear();
        if(res!=0){
            return res;
        }
        else{
            return (pair2.getTemp()-pair1.getTemp());
        }
    }
}
