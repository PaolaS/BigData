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
import org.apache.hadoop.io.DoubleWritable;

/* QUERY 4: 
	a weekly "penalty" score for each airport that depends on both 
	its incoming and outgoing flights. The score adds 0.5 for each 
	incoming flight that is more than 15 minutes late, and 1 for 
	each outgoing flight that is more than 15 minutes late
*/
public class Query4 extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		
		JobConf job = new JobConf(conf, Query4.class);
		job.setJobName("Query4");
		
		job.setOutputKeyClass(YearWeekAirportWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		
        job.setMapperClass(Query4Map.class);
        job.setReducerClass(Query4Reduce.class);
			
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		JobClient.runJob(job);
		return 0;
	}
	
public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new Query4(), args);
        System.exit(res);
		

	}
	

}
