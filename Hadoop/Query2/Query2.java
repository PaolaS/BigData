import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.ObjectWritable;

/* QUERY 2:
		weekly percentages of delays that are due to weather, throughout the entire data set
*/
public class Query2 extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		
		JobConf job = new JobConf(conf, Query2.class);
		job.setJobName("Query2");
		
		job.setOutputKeyClass(YearWeekWritable.class);
		job.setOutputValueClass(ObjectWritable.class);
		
        job.setMapperClass(Query2Map.class);
        job.setReducerClass(Query2Reduce.class);
			
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		JobClient.runJob(job);
		return 0;
	}
	
public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new Query2(), args);
        System.exit(res);

	}

}
