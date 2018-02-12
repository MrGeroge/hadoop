package join;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by George on 2017/5/4.
 */
public class JoinPair implements WritableComparable<JoinPair> {
    private String product;//商品信息
    private int id;//标志是商品ID／交付ID

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public int compareTo(JoinPair o) {
        return (this.product.compareTo(o.getProduct()));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.product);
        out.writeInt(this.id);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.product=in.readUTF();
        this.id=in.readInt();
    }

    @Override
    public String toString() {
        String str="id="+this.id+"   product="+this.product;
        return str;
    }
}
