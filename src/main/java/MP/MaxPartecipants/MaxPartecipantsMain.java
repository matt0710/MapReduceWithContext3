package MP.MaxPartecipants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Date;

public class MaxPartecipantsMain {
    public static void main (String[] args) throws Exception {

        long startTime = new Date().getTime();

        if (args.length != 2) {
            System.err.println("MaxPartecipants needs <inPath> <outPath>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MaxPartecipantsMain");

        job.setJarByClass(MaxPartecipantsMain.class);
        job.setMapperClass(MaxPartecipantsMapper.class);
        job.setReducerClass(MaxPartecipantsReducer2.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        long endTime = new Date().getTime();

        System.out.println("Time required is: " + (endTime - startTime) + " ms");

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
