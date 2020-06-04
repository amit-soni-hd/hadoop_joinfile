package org.example.hadoop.joinfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class JoinFileDriver {
    private static final Logger logger = Logger.getLogger(JoinFileDriver.class.getName());

    public static void main(String[] args) throws IOException {

        Configuration configuration = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();

        if (remainingArgs.length != 3) {
            logger.log(new LogRecord(Level.SEVERE, "please enter the emp name, emp dept and output file"));
            System.exit(2);
        }

        JobConf entries = new JobConf(configuration, JoinFileDriver.class);
        entries.setJobName("merge-two-file");
        entries.setJarByClass(JoinFileDriver.class);

        entries.setReducerClass(JoinFileReducer.class);
        entries.setMapperClass(JoinFileMapper.class);

        entries.setOutputKeyClass(NullWritable.class);
        entries.setOutputValueClass(Text.class);
        entries.setMapOutputKeyClass(Text.class);
        entries.setMapOutputValueClass(JoinWritable.class);

        FileInputFormat.addInputPath(entries, new Path(remainingArgs[0]));
        FileInputFormat.addInputPath(entries, new Path(remainingArgs[1]));
        FileOutputFormat.setOutputPath(entries, new Path(remainingArgs[2]));

        boolean complete = JobClient.runJob(entries).isComplete();

        logger.info(complete ? "job is successfully done" : "something is wrong");
    }
}
