package week2.mr.hw;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PhoneFlowBean implements Writable {
    private long upFlow;
    private long downFlow;
    private long totalFlow;

    public PhoneFlowBean() {
        super();

    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setTotalFlow(long totalFlow) {
        this.totalFlow = totalFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);

        dataOutput.writeLong(downFlow);

        dataOutput.writeLong(totalFlow);
    }

    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readLong();

        downFlow = dataInput.readLong();

        totalFlow = dataInput.readLong();
    }

    @Override
    public String toString() {
        return "\t" + upFlow + "\t" + downFlow + "\t" + totalFlow;
    }

}
