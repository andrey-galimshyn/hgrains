package GrainsUSA;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class GrainsReducer  extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text t_key, Iterator<DoubleWritable> values, OutputCollector<Text,DoubleWritable> output, Reporter reporter) throws IOException {
		Text key = t_key;
		double amountForGrain = 0;
		while (values.hasNext()) {
			// replace type of value with the actual type of our value
			DoubleWritable value = (DoubleWritable) values.next();
			amountForGrain += value.get();
			
		}
		output.collect(key, new DoubleWritable(amountForGrain));
	}
	
}
