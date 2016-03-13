package org.gstry.weather.reader;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopFileReader {
	
	private static final Logger LOG = LoggerFactory.getLogger(HadoopFileReader.class);

	public static int readSequenceFile(FileSystem fs,Path filePath,Configuration conf) throws IOException{
		LOG.info("Reading sequence file at path:{} with {}",filePath.getName(),fs.toString());
		
		if(!fs.exists(filePath)){
			LOG.error("The file does not exist within the filesystem");
			return -1;
		}
		if(!fs.isFile(filePath)){
			LOG.error("{} is not a file.",filePath.getName());
			return -1;
		}
		Reader reader = new SequenceFile.Reader(conf,Reader.file(filePath));
		Writable key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);	
		Writable value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);		
		while(reader.next(key, value)){
			System.out.println(key.toString() + "/t" + value.toString());
		}		
		reader.close();
		return 0;	
		
	}
}
