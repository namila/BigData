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

public class TopGenreInRegion {

	public static class TopGenreInRegionMapper extends
			Mapper<Object, Text, Text, Text> {

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String jsonString, region, genres;
			Object parsedObject;
			JSONObject jsonRepresentation;

			jsonString = value.toString().trim();
			parsedObject = JSONValue.parse(jsonString);
			jsonRepresentation = (JSONObject) parsedObject;

			if (jsonRepresentation.get("country") != null) {
				region = jsonRepresentation.get("country").toString().trim();
			}

			else {
				region = null;
			}

			if (region!=null && region.isEmpty()==false) {
				context.write(new Text(region), new Text(jsonString));
			}
		}
	}

	public static class TopGenreInRegionReducer extends
			Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String jsonString, genres, region, highestRatedGenres;
			double rating;
			Object parsedObject;
			JSONObject jsonRepresentation;

			parsedObject = null;
			jsonRepresentation = null;
			jsonString = null;
			genres = null;
			highestRatedGenres = null;
			rating = 0;
			region = key.toString().trim();
			TreeMap<Double, String> genresAcoordingToRating = new TreeMap<Double, String>();

			for (Text currentRecord : values) {
				jsonString = currentRecord.toString().trim();
				parsedObject = JSONValue.parse(jsonString);
				jsonRepresentation = (JSONObject) parsedObject;

				if (jsonRepresentation.get("rating") != null) {
					rating = Double.parseDouble(jsonRepresentation
							.get("rating").toString().trim());
					genres = jsonRepresentation.get("genres").toString();
				}

				if (genres != null) {
					genresAcoordingToRating.put(rating, genres);
				}

			}

			highestRatedGenres = genresAcoordingToRating
					.get(genresAcoordingToRating.lastKey());
			context.write(new Text(region), new Text(highestRatedGenres));

		}

	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Top Genre In Region");
		job.setJarByClass(TopGenreInRegion.class);
		job.setMapperClass(TopGenreInRegionMapper.class);
		job.setReducerClass(TopGenreInRegionReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0].toString().trim()));
		FileOutputFormat
				.setOutputPath(job, new Path(args[1].toString().trim()));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
