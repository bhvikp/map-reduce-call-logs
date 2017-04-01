package com.bhvik;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CallReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	int countCall; 
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		for(IntWritable call : values){
			if(call.get() > 60){
				countCall = call.get();
			}
		}
		context.write(key, new IntWritable(countCall));
	}
}
