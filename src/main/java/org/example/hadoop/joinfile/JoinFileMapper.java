package org.example.hadoop.joinfile;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class JoinFileMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, JoinWritable> {

    private String filename = "hello";

//    @Override
//    public void configure(JobConf job) {
//        filename = job.get("map.input.file");
//    }

    @Override
    public void map(LongWritable longWritable, Text text, OutputCollector<Text, JoinWritable> outputCollector, Reporter reporter) throws IOException {

        String token[] = text.toString().split(",");
        filename += ".txt";

        if (token.length == 2) {
            outputCollector.collect(new Text(token[0]), new JoinWritable(token[1], filename));
        }

//        101,Amit ->   key -> 101    value ->  (Amit, empname.txt)
//        101,Sales  -> key -> 101    value -> (Sales, empdept.txt);


    }
}
