package com.example;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new WordCount(), args);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: hadoop jar crunch-demo-1.0-SNAPSHOT-job.jar" + " [generic options] input output");
            System.err.println();
            GenericOptionsParser.printGenericCommandUsage(System.err);
            return 1;
        }

        String inputPath = args[0];
        String outputPath = args[1];
        Pipeline pipeline = new MRPipeline(WordCount.class, getConf());
        PCollection<String> lines = pipeline.readTextFile(inputPath);
        PCollection<String> words = lines.parallelDo(new Tokenizer(), Writables.strings());
        PCollection<String> noStopWords = words.filter(new StopWordFilter());
        PTable<String, Long> counts = noStopWords.count();
        pipeline.writeTextFile(counts, outputPath);
        PipelineResult result = pipeline.done();
        return result.succeeded() ? 0 : 1;
    }
}
