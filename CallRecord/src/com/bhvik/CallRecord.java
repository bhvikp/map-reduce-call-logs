package com.bhvik;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CallRecord {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

    	Job job = new Job();
    	job.setJarByClass(CallRecord.class);
    	job.setJobName("MaxCallRecord");
    	
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	
    	job.setMapperClass(CallMapper.class);
    	job.setReducerClass(CallReducer.class);
    	
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);       
    	System.out.println(job.waitForCompletion(true)?0 : 1);

    }

}
