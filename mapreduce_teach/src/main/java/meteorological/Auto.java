package meteorological;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**自定义分区*/
public class Auto extends Partitioner<Weather,NullWritable>{

    public static Map<String, Integer> provinceDict = new HashMap<String, Integer>();
    static {
        int a = 0;
        for (int i = 1980; i <= 1981; i++) {
            provinceDict.put(i + "", a);
            a++;
        }
    }

    @Override
    public int getPartition(Weather weather, NullWritable nullWritable, int i) {
        Integer id = provinceDict.get(weather.toString().substring(0, 4));
        return id == null ? 2 : id;
    }
}

