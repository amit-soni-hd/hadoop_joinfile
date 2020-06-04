package org.example.hadoop.joinfile;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class JoinFileReducer extends MapReduceBase implements Reducer<Text, JoinWritable, NullWritable, Text> {

    @Override
    public void reduce(Text text, Iterator<JoinWritable> iterator, OutputCollector<NullWritable, Text> outputCollector, Reporter reporter) throws IOException {

        String name = null;
        String dept = null;

        while (iterator.hasNext()) {
            JoinWritable value = iterator.next();
            System.out.println(value.getMrFileName().toString());
            name = value.getMrValue().toString();
//            if (value.getMrFileName().toString().equalsIgnoreCase("empname.txt")) {
//                name = value.getMrValue().toString();
//            } else if (value.getMrFileName().toString().equalsIgnoreCase("empdept.txt")) {
//                dept = value.getMrValue().toString();
//            }
        }
        StringBuffer result = new StringBuffer(text.toString()).append(",");
        result.append(name).append(",").append(name);
        outputCollector.collect(NullWritable.get(), new Text(result.toString()));
    }
}
