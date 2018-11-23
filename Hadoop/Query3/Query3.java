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
import org.apache.hadoop.io.IntWritable;

/* QUERY 3: 
	the percentage of flights belonging to a given "distance group" that were able 
	to halve their departure delays by the time they arrived at their destinations. 
	Distance groups assort flights by their total distance in miles. Flights with 
	distances that are less than 200 miles belong in group 1, flights with distances 
	that are between 200 and 399 miles belong in group 2, flights with distances that 
	are between 400 and 599 miles belong in group 3, and so on. The last group contains 
	flights whose distances are between 2400 and 2599 miles.
*/
public class Query3 extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		
		JobConf job = new JobConf(conf, Query3.class);
		job.setJobName("Query3");
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DelaysWritable.class);
		
        job.setMapperClass(Query3Map.class);
        job.setReducerClass(Query3Reduce.class);
			
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		JobClient.runJob(job);
		return 0;
	}
	
public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new Query3(), args);
        System.exit(res);
		

	}
	

}
