package AdvertiseRecommended;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by George on 2017/5/11.
 */
public class AdvertiseSort extends WritableComparator {
    public AdvertiseSort(){
        super(AdvertisePair.class,true);
    }
    /*static {                                        // register this comparator
        WritableComparator.define(AdvertisePair.class, new AdvertiseSort());
    }*/

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        AdvertisePair pair1=(AdvertisePair)a;
        AdvertisePair pair2=(AdvertisePair)b;
        if(pair1.getKey().equals(pair2.getKey())){
            System.out.println("exe there");
            System.out.println("###################");
            return (pair2.getWeight()-pair1.getWeight());
        }
        else{
            System.out.println("exe here");
            System.out.println("###################");
            return pair1.getKey().compareTo(pair2.getKey());
        }
    }
}
