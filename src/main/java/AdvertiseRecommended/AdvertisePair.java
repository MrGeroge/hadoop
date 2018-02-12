package AdvertiseRecommended;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by George on 2017/5/11.
 */
public class AdvertisePair implements WritableComparable<AdvertisePair> {
    private String key;//关键字
    int weight;//权重

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    @Override
    public int compareTo(AdvertisePair o) {
       if(this.key.equals(o.getKey())){//如果key相同，则比较value
           return (-(this.weight-o.getWeight()));
       }
       else{
           return this.key.compareTo(o.getKey());
       }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.key);
        dataOutput.writeInt(this.weight);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.key=dataInput.readUTF();
        this.weight=dataInput.readInt();
    }
}
