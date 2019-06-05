package com.atguigu.hbase.mr;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.KeyValueSerialization;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * 功能是从一个HBase表中读取数据，写出到另一个HBase表！
 * 		读数据功能交给TableInputFormat，读的数据key(rowkey)-value(Result)
 * 
 * 			中间：  key(rowkey)-value(Result)---mapper---写出----reducer读入---写出
 * 
 * 		写工具交给TableOutPutFormat,写reducer输出的key-value，写出的格式key(rowkey)-value(Put)
 * 
 * MR要从hbase的表中读数据，写到hbase的表中
 * 
 * 1.  read：  输入格式（根据数据源的不同，使用不同的输入格式）
 * 					TableInputFormat， 每个region切一片！
 * 					每次读取一行记录，key为rowkey，value为Result
 * 2.  map ：  将输入格式读到的数据，处理
 * 					mapper需要继承于TableMapper.
 * 				Mapper<ImmutableBytesWritable, Result, KEYOUT, VALUEOUT>
 * 
 * 			
 * 3. reduce :  将排序好的数据合并处理
 * 					reducer需要继承TableReducer
 * 4. write : 输出格式
 * 
 * --------------------
 * 在driver中，使用TableMapReduceUtil.initTableMapperJob来设置job中mapper的属性！
 * 
 * 5. Put也就是Mutation类型并没有实现Writable接口！
 * 			在创建Job时，TableMapReduceUtil.initTableMapperJob()时，提供了三种序列化器
 * 				MutationSerialization.class.getName(), ResultSerialization.class.getName(),
        		KeyValueSerialization.class.getName()
 * 
 */
public class DataTransferMapper extends TableMapper<ImmutableBytesWritable, Put>{
	
	
	
	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context)
			throws IOException, InterruptedException {
		
		// 为put对象设置rowkey
		 Put out_value=new Put(key.copyBytes());
		 
		 // 封装put对象
		 Cell[] cells = value.rawCells();
		 
		 for (Cell cell : cells) {
			
			 out_value.add(cell);
			 
		}
		 
		context.write(key, out_value);
		
		
	}
	
	

}
