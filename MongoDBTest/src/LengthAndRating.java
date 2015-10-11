import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class LengthAndRating {

	public static class LengthAndRatingMapper extends
			Mapper<Object, Text, IntWritable, Text> {

		// The mapper outputs runtimes and tvShow records which have that runtime 
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String jsonString = value.toString();
			
			// Parsing the TvShow record into a JSON object
			Object parsedObject = JSONValue.parse(jsonString);
			JSONObject jsonObject = (JSONObject) parsedObject;
			int runtimeIntValue=0;
			
			try{
				// Extracting the runtime from JSON object
				runtimeIntValue=Integer.parseInt(jsonObject.get("runtime").toString().trim());
			}
			catch (Exception e){
				
				runtimeIntValue=0;
			}
			
			IntWritable runtime = new IntWritable(runtimeIntValue);
			
			/* Mapper outputs
			 * key: runtime
			 * value:tvShow record
			 */
			
			context.write(runtime, new Text(jsonString));
		}
	}

	public static class LengthAndRatingReducer extends
			Reducer<IntWritable, Text, IntWritable, DoubleWritable> {

		// The reducer calculates the average rating for each runtime
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			int numberOfElements;
			double totalRatings, averageRating,currentRating;
			String jsonString;
			Object parsedObject;
			JSONObject jsonReresentation;

			numberOfElements = 0;
			currentRating=0;
			totalRatings = 0;
			averageRating = 0;

			for (Text currentRecord : values) {
				jsonString = currentRecord.toString();
				
				// Parsing the tvShow record into a json object
				parsedObject = JSONValue.parse(jsonString);
				jsonReresentation = (JSONObject) parsedObject;
				try{
					// Extracting the rating
					currentRating=Double.parseDouble(jsonReresentation.get("rating").toString().trim());				
				}
				
				catch(Exception e){
					currentRating=0;					
				}
				totalRatings += currentRating;
				++numberOfElements;
			}

			if (numberOfElements > 0) {
				
				// Calculating the average rating
				averageRating = totalRatings / numberOfElements;
				
				/* Reducer Outputs
				 * Key: runtime
				 * Value: average rating for that runtime
				 */
				context.write(key, new DoubleWritable(averageRating));
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Length and Rating");
		job.setJarByClass(LengthAndRating.class);
		job.setMapperClass(LengthAndRatingMapper.class);
		job.setReducerClass(LengthAndRatingReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0].trim()));
		FileOutputFormat.setOutputPath(job, new Path(args[1].trim()));
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
