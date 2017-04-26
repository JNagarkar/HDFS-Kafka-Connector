package com.spnotes.kafka.simple;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Scanner;

import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Producer {
    private static Scanner in;
    public static void main(String[] argv)throws Exception {

	System.out.println(argv!=null ? argv.length: "null");

/*
        if (argv.length != 1) {
            System.err.println("Please specify 1 parameters ");
            System.exit(-1);
        }*/
        String topicName = "test";
        System.out.println("Enter message(type exit to quit)");
	//Configure the Producer
	Properties configProperties = new Properties();
	configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
	configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
	configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
	org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);	


	readFromHDFS(producer,configProperties,topicName);
/*        while(!line.equals("exit")) {
            //TODO: Make sure to use the ProducerRecord constructor that does not take parition Id
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName,line);
            producer.send(rec);
            line = in.nextLine();
        }
*/
/*	for(int i=0;i < 20;i++){
	    //TODO: Make sure to use the ProducerRecord constructor that does not take parition Id
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName,i+"");
            producer.send(rec);	
	}
	producer.close();
*/

}


     	public static void readFromHDFS(org.apache.kafka.clients.producer.Producer producer ,Properties configProperties,String topicName) throws Exception{
		try{
			System.out.println("Now read from HDFS");

			System.out.println("topicNAme is:"+topicName);
			String FS_PARAM_NAME = "fs.defaultFS";

			Configuration hadoopConfig = new Configuration();	
			System.out.println("configured filesystem = " + hadoopConfig.get(FS_PARAM_NAME));
                        Path pt=new Path("hdfs://quickstart.cloudera:8020//user/cloudera/apache-access-log.txt");
                        
/*		hadoopConfig.set("fs.hdfs.impl", 
			org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
		    );
		    hadoopConfig.set("fs.file.impl",
			org.apache.hadoop.fs.LocalFileSystem.class.getName()
		    );
*/

			FileSystem fs = FileSystem.get(hadoopConfig);

			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
                        String line;
                        line=br.readLine();

			if(producer==null){
				System.out.println("producer is null");
			}else{
				System.out.println(producer.toString());			
			}

		

                        while (line != null){
				//TODO: Make sure to use the ProducerRecord constructor that does not take parition Id
//				System.out.println(line);				
				ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName,line);
				producer.send(rec);
                                line=br.readLine();
                        }
                }catch(Exception e){
                	System.out.println("Exception:"+e.toString());
		}
		finally{
			System.out.println("closing producer");
		        producer.close();
		}
	}
}







  













 




