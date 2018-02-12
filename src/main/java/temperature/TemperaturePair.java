package temperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by George on 2017/5/16.
 */
public class TemperaturePair implements WritableComparable<TemperaturePair>{
    private int year;
    private int temp;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getTemp() {
        return temp;
    }

    public void setTemp(int temp) {
        this.temp = temp;
    }

    @Override
    public int compareTo(TemperaturePair o) {
        int res=this.year-o.getYear();
        if(res!=0){
            return res;
        }
        else{
            return (o.getTemp()-this.temp);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.year);
        dataOutput.writeInt(this.temp);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year=dataInput.readInt();
        this.temp=dataInput.readInt();
    }
}
