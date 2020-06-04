package org.example.hadoop.joinfile;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class JoinWritable implements Writable {

    private Text mrFileName;
    private Text mrValue;

    public JoinWritable(Text mrFileName, Text mrValue) {
        set(mrFileName, mrValue);
    }

    public JoinWritable(String fileName, String value) {
        set(new Text(fileName), new Text(value));
    }

    public void set(Text mrFileName, Text mrValue) {
        this.mrFileName = mrFileName;
        this.mrValue = mrValue;
    }

    public Text getMrFileName() {
        return mrFileName;
    }

    public Text getMrValue() {
        return mrValue;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        mrFileName.write(dataOutput);
        mrValue.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        mrFileName.readFields(dataInput);
        mrValue.readFields(dataInput);
    }
}
