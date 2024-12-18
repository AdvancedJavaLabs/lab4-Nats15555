package src;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import src.data.Sales;
import src.data.SalesMapper;
import src.data.SalesReducer;
import src.sort.Sort;
import src.sort.SortMapper;
import src.sort.SortReducer;

import java.io.IOException;

public class SalesAnalysis {

    public static void main(String[] args) throws Exception {
        // Проверка на количество аргументов командной строки
        if (args.length < 4) {
            System.exit(2);
        }

        String inputDir = args[0];
        String outputDir = args[1];
        String outputTmpDir = args[1] + "-tmp";
        int reducersNumber = Integer.parseInt(args[2]);
        int blockSize = Integer.parseInt(args[3]);

        Configuration conf = new Configuration();
        conf.set("mapreduce.input.fileinputformat.split.maxsize", Integer.toString(blockSize * 1024)); // по 1 кб

        long startTime = System.currentTimeMillis();
        //Собираем и анализируем стату
        if (!analysisJob(conf, reducersNumber, inputDir, outputTmpDir)) {
            System.out.println("Упала первая джоба");
            System.exit(1);
        }

        //Сортируем стату
        System.exit(sortJob(conf, reducersNumber, outputTmpDir, outputDir) ? 0 : 1);
    }


    private static boolean analysisJob(Configuration conf, int reducersNumber, String inputDir, String outputTmpDir) throws IOException, InterruptedException, ClassNotFoundException {
        Job salesJob = Job.getInstance(conf, "analysis");
        salesJob.setNumReduceTasks(reducersNumber);
        salesJob.setJarByClass(SalesAnalysis.class);
        salesJob.setMapperClass(SalesMapper.class);
        salesJob.setReducerClass(SalesReducer.class);
        salesJob.setMapOutputKeyClass(Text.class);
        salesJob.setMapOutputValueClass(Sales.class);
        salesJob.setOutputKeyClass(Text.class);
        salesJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(salesJob, new Path(inputDir));
        FileOutputFormat.setOutputPath(salesJob, new Path(outputTmpDir));

        return salesJob.waitForCompletion(true);
    }

    private static boolean sortJob(Configuration conf, int reducersNumber, String outputTmpDir, String outputDir) throws IOException, InterruptedException, ClassNotFoundException {
        Job sortJob = Job.getInstance(conf, "sorting");
        sortJob.setNumReduceTasks(reducersNumber);
        sortJob.setJarByClass(SalesAnalysis.class);
        sortJob.setMapperClass(SortMapper.class);
        sortJob.setReducerClass(SortReducer.class);

        sortJob.setMapOutputKeyClass(DoubleWritable.class);
        sortJob.setMapOutputValueClass(Sort.class);

        sortJob.setOutputKeyClass(Text.class);
        sortJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(sortJob, new Path(outputTmpDir));
        FileOutputFormat.setOutputPath(sortJob, new Path(outputDir));

        return sortJob.waitForCompletion(true);
    }
}

