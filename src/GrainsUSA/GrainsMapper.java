package GrainsUSA;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class GrainsMapper  extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	private static final Logger logger = Logger.getLogger(GrainsMapper.class);
	
	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
    	String valueString = value.toString();
		String[] grainsData = valueString.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
		try {
			String frequencyDescr = grainsData[15].replaceAll("^\"|\"$", "");
			String unitId = grainsData[11];
			if (frequencyDescr.equals("Annual") && unitId.equals("1")) {
			    DoubleWritable amount = new DoubleWritable(Double.parseDouble(grainsData[18]));
			    output.collect(new Text(grainsData[3] + " " + grainsData[12]), amount);
			} 
		}
		catch (Exception e) {
			throw e;
		}
		
	}
	
}
