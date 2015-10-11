import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;


public class TopShowInRegion {

	public static class TopShowInRegionMapper extends Mapper<Object, Text, Text, Text>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String jsonString=value.toString();
			Object parsedObject=JSONValue.parse(jsonString);
			JSONObject jsonRepresentation=(JSONObject)parsedObject;
			String country;
			
			if(jsonRepresentation.get("country")!=null){
				country=jsonRepresentation.get("country").toString().trim();				
			}
			
			else{
				country=null;
			}
			
			if(country!=null){
				context.write(new Text(country),new Text(jsonString));
				
			}
		}		
	}
	
	public static class TopShowInRegionReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Context context)
				throws IOException, InterruptedException {
			TreeMap<Double, String> tvShowAccordingToRating=new TreeMap<Double, String>();
			String jsonString;
			Object parsedObject;
			JSONObject jsonRepresentation;
			double rating;
			String country;
			String tvShowTitle;
			
			country=key.toString();
			for(Text currentShow:values){
				jsonString=currentShow.toString().trim();
				parsedObject=JSONValue.parse(jsonString);
				jsonRepresentation=(JSONObject)parsedObject;
				
				if(jsonRepresentation.get("rating")!=null){
					rating=Double.parseDouble(jsonRepresentation.get("rating").toString().trim());
					tvShowTitle=jsonRepresentation.get("title").toString().trim();
					tvShowAccordingToRating.put(rating,tvShowTitle);					
				}				
			}
			
			if(tvShowAccordingToRating.size()>0){
				String mostRatedTvShow=tvShowAccordingToRating.get(tvShowAccordingToRating.lastKey());
				context.write(new Text(country),new Text(mostRatedTvShow));				
			}
		}		
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"Top Show In Region");
		job.setJarByClass(TopShowInRegion.class);
		job.setMapperClass(TopShowInRegionMapper.class);
		job.setReducerClass(TopShowInRegionReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0].toString().trim()));
		FileOutputFormat.setOutputPath(job, new Path(args[1].toString().trim()));
		System.exit(job.waitForCompletion(true)?0:1);		
	}
}
