package com.bhvik;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CallMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String line = value.toString();
		String phoneNo = line.substring(0, 10);
		String startCall = line.substring(22, 41);
		String endCall	= line.substring(42, 61);
		
		//check for STD calls by finding 1 at end of the line
		if(line.charAt(62)== '1')
		{
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date d1 = null;
			Date d2 = null;
			try {
				d1 = format.parse(startCall);
				d2 = format.parse(endCall);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			long callDuration = d2.getTime() - d1.getTime();
			long callMinute = callDuration / (60 * 1000);
			
			context.write(new Text(phoneNo), new IntWritable((int) callMinute));
		}
		
	}

}
