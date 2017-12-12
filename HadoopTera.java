import java.io.*;
import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

public class MyTeraSort {

        public static class Map extends Mapper<Text, Text, Text, Text> {

                private Text mPPkey = new Text();
                private Text mPPvalue = new Text();


        // Mapper function written below
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

                        String t = value.toString();
                        String mK = t.substring(0, 10);
                        String mV = t.substring(13, 98);
                        mV = mV + t.substring(t.length() - 1);
                        mPPkey.set(mK);
                        mPPvalue.set(mV);
                        context.write(mPPkey,mPPvalue);
                }
        }

        // Reducder function written below
        public static class Reduce extends Reducer<Text, Text, Text, Text> {

                public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                        for (Text val : values)
                        {
                                context.write(key, val);
                        }
                }
        }

        public static void main(String[] args) throws Exception {

                // create file pointers for write operation
                FileInputStream inFp = null;
FileOutputStream outFp = null;
                String str = "Reporting in this variable";
                byte[] strBytes = str.getBytes();
                Configuration cf = new Configuration();
                outFp = new FileOutputStream("Fileoutput.txt");


                String[] a1 = new GenericOptionsParser(cf, args).getRemainingArgs();
                if (a1.length != 3) {
                        System.out.println("Please Enter Proper arguments");
                        str = "Enter proper arguments";
                        strBytes = str.getBytes();
                        if(outFp!=null)
                        {
                                outFp.write(strBytes);
                        }
                        System.exit(2);
                }

                long startTime =0, endTime = 0;
                double execTime = 0;

                startTime = System.currentTimeMillis();
                Job job = new Job(cf, "MyTeraSort");
                if(job!=null)
                {

                        job.setJarByClass(MyTeraSort.class);                            // Job Created to run Map Reduce program
                        job.setMapperClass(Map.class);                                  // Set the mapper using setMapperClass
                        job.setReducerClass(Reduce.class);                              // Set the reducer using SetReducerClass
                        job.setNumReduceTasks(32);                                      // Set the number of reducers using setNumReducerTasks
                        job.setInputFormatClass(KeyValueTextInputFormat.class);         // Set the input format using the function setInputFormatClass
                        job.setMapOutputKeyClass(Text.class);                           // Set the output key using setMapOutputKey
                        job.setOutputKeyClass(Text.class);
                        job.setOutputValueClass(Text.class);
                        FileInputFormat.addInputPath(job, new Path(a1[0]));             // Get the path for input file
                        Path Directory = new Path(a1[2]);
                        if(Directory!=null)
                        {
                                 Path partition_path = new Path(Directory, "partitioning");
                                TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partition_path);
                        }
{
                                str = "Directory not found in TeraSorting";
                                strBytes = str.getBytes();
                                if(outFp!=null)
                                {
                                        outFp.write(strBytes);
                                }
                        }



                        InputSampler.Sampler<Text,Text> sampler1 = new InputSampler.RandomSampler<>(0.01,1000,100);
                        if(sampler1!=null)
                        {

                                InputSampler.writePartitionFile(job, sampler1);
                        }
                        else
                        {
                                str = "Sampler not found in TeraSort";
                                strBytes = str.getBytes();
                                if(outFp!=null)
                                {
                                        outFp.write(strBytes);
                                }
                        }

                        job.setPartitionerClass(TotalOrderPartitioner.class);
                        FileOutputFormat.setOutputPath(job, new Path(a1[1]));
               }
                else
                {
                        str = "TeraSort Job is null";
                        strBytes = str.getBytes();
                        if(outFp!=null)
                        {
                                outFp.write(strBytes);
                        }
                }
				endTime = System.currentTimeMillis();
                execTime = endTime - startTime;
                execTime = execTime/1000;
                System.out.println("Execution time: "+ execTime);

                str = "Execution time: "+ execTime + " seconds";
				strBytes = str.getBytes();
                try {

                        //outFp = new FileOutputStream("Fileoutput.txt");

                        if(outFp!=null)
                        {
                                outFp.write(strBytes);
                        }	

                   }finally

                   {

                        if (outFp != null) {
                                outFp.close();
                        }

                 }
                 System.exit(job.waitForCompletion(true	) ? 0 : 1);
        }
}
