import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by hadoop on 17-6-26.
 */
public class MaxTemperatureMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private NcdcRecordParser parser = new NcdcRecordParser();
    enum Temperature {
        OVER_100
    }
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Version 1
//        String line = value.toString();
//        String year = line.substring(15, 19);
//        int airTemperature = Integer.parseInt(line.substring(87, 92));
//        context.write(new Text(year), new IntWritable(airTemperature));


        //v2
        parser.parse(value);
        if (parser.isValidTemperature()) {
            int airTemperature = parser.getAirTemperature();
            if (airTemperature > 1000) {
                System.err.println("Temperature over 100 degrees for input: " + value);
                context.setStatus("Detected possibly corrupt record: see logs.");
                context.getCounter(Temperature.OVER_100).increment(1);
            }
            context.write(new Text(parser.getYear()), new IntWritable(parser.getAirTemperature()));
        }
    }
}
