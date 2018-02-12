package sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by George on 2017/5/4.
 */
public class StringIntPair implements WritableComparable<StringIntPair> {
    private String key;
    private int value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    @Override
    public int compareTo(StringIntPair o) {

        return this.key.compareTo(o.getKey());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.key);
        out.writeInt(this.value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.key=in.readUTF();
        this.value=in.readInt();
    }
}
