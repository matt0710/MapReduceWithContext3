package MP.MaxPartecipants;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class MaxPartecipantsReducer2 extends Reducer<Text, Text, Text, Text> {

    public void reduce (Text text, Iterable<Text> iterable, Context context) throws IOException, InterruptedException {

        String[] row = new String[2];
        TreeMap<Integer, String> treeMap = new TreeMap<>(Comparator.reverseOrder());
        //HashMap<String, Integer> map = new HashMap<>();

        for (Text t : iterable) {
            row = t.toString().split(";");
            treeMap.put(Integer.parseInt(row[1]), row[0]);
        }
        //int max = Collections.max(map.values());

        context.write(text, new Text(treeMap.get(treeMap.firstKey())));
    }
}