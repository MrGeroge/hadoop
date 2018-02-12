package KMeans;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by George on 2017/6/5.
 */
public class KMeansNode implements WritableComparable<KMeansNode> {
    private int x;
    private int y;

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public int getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }

    @Override
    public int compareTo(KMeansNode o) {
        if(this.x==o.getX()&&this.y==o.getY()){
            return 0;
        }
        else{
            return -1;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(x);
        dataOutput.writeInt(y);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.x=dataInput.readInt();
        this.y=dataInput.readInt();
    }
}
