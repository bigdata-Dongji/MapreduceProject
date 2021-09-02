package recruit;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JsonReduce extends Reducer<Jsonrecruit, NullWritable,Jsonrecruit,NullWritable> {
    @Override
    protected void reduce(Jsonrecruit key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable value:values) {
            context.write(key, value);
        }
    }
}
