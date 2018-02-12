import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by George on 2017/5/3.
 */
public class Text implements WritableComparable {
    private int id;
    private String name;
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(name);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id=dataInput.readInt();
        name=dataInput.readUTF();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public int compareTo(Object o) {
        Text text=(Text)o;
        if(this.id>text.getId()){
            return 1;
        }
        else if(this.id==text.getId()){
            return 0;
        }
        else{
            return -1;
        }
    }
}
