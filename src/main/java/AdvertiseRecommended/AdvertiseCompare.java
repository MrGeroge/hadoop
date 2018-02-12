package AdvertiseRecommended;

import org.apache.hadoop.io.WritableComparator;
import sort.StringIntPair;

/**
 * Created by George on 2017/5/11.
 */
public class AdvertiseCompare extends WritableComparator {
    public AdvertiseCompare() {
        super(AdvertisePair.class,true);
    }

    @Override
    public int compare(Object a, Object b) {
        AdvertisePair pair1=(AdvertisePair)a;
        AdvertisePair pair2=(AdvertisePair)b;
        return pair1.getKey().compareTo(pair2.getKey());
    }
}
