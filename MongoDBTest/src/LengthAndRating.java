import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;


public class LengthAndRating {
	
	public static class LengthAndRatingMapper extends Mapper<Object, Text, IntWritable, IntWritable>{

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
		}
		
		
	}
	
	
	
	
	public static void main(String[] args) throws IOException{
		File jsonDataFile=new File("/home/hduser/JSONData/TvShowsJson.json");
		Reader myReader=new FileReader(jsonDataFile);
		BufferedReader myBufferedReader=new BufferedReader(myReader);
		String jsonString=myBufferedReader.readLine();
		Object jsonObject=JSONValue.parse(jsonString);
		JSONObject jsonRepresentation=(JSONObject)jsonObject;
		System.out.println(jsonRepresentation.get("runtime"));
		System.out.println(jsonRepresentation.get("rating"));
		
		
		
	}

}
