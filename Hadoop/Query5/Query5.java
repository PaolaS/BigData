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
import org.apache.hadoop.io.Text;

/* QUERY 5: 
 * -----------------------------------------------
*/
public class Query5 extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		
		JobConf job = new JobConf(conf, Query5.class);
		job.setJobName("Query5");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
        job.setMapperClass(Query5Map.class);
        job.setReducerClass(Query5Reduce.class);
			
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		JobClient.runJob(job);
		return 0;
	}
	
public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new Query5(), args);
        System.exit(res);
		

	}
	

}
