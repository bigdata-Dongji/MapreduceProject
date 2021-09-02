package meteorological;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherReduce extends Reducer<Weather,NullWritable,Weather,NullWritable>{
    @Override
    protected void reduce(Weather key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable value:values) {
            context.write(key,value);
        }
    }
}
