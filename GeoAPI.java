package BigData.KafkaTwitterProducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import BigData.KafkaTwitterProducer.GeocodeImplementation;
import BigData.KafkaTwitterProducer.GeocodeImplementation.location;

public class GeoAPI {
	
	public static String jsonCoord(String city) throws IOException {
		
		String address=city.replace(" ", "");
		URL url = new URL("http://maps.googleapis.com/maps/api/geocode/json?address=" + address + "&sensor=false");
		URLConnection connection = url.openConnection();
		BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
		String inputLine;
		String jsonResult = "";
		while ((inputLine = in.readLine()) != null) {
		    jsonResult += inputLine;
		}
		in.close();
		return jsonResult; 
		}
	
	@SuppressWarnings("static-access")
	public static String coordinates(String city) throws JsonSyntaxException, IOException {
		Gson gson = new Gson();
		GeoAPI apireq=new GeoAPI();
		GeocodeImplementation result = gson.fromJson(apireq.jsonCoord(city), GeocodeImplementation.class);
		location coord  = result.results[0].geometry.location;
		String latitude = coord.lat;
		String longitude = coord.lng;
		return latitude+","+longitude;
	}
	

}