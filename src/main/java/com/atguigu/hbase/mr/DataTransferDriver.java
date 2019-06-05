package com.atguigu.hbase.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

public class DataTransferDriver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = HBaseConfiguration.create();
		
		Job job = Job.getInstance(conf);
		
		job.setJobName("hbasemr1");
		
		job.setJarByClass(DataTransferDriver.class);
		
		Scan scan = new Scan();
		
		// 设置Job运行的各个组件和参数
		TableMapReduceUtil.initTableMapperJob("fruit", scan, 
				DataTransferMapper.class, ImmutableBytesWritable.class, Put.class, job);
		
		TableMapReduceUtil.initTableReducerJob("fruit_mr", DataTransferReducer.class, job);
		
		job.waitForCompletion(true);
	}

}
