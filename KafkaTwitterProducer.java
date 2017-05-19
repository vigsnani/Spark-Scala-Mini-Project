package BigData.KafkaTwitterProducer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import twitter4j.*;
import twitter4j.conf.*;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import BigData.KafkaTwitterProducer.GeoAPI;


public class KafkaTwitterProducer {

	public static void main(String[] args) throws Exception {

		final LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(1000);

		String consumerKey = args[0].toString();
		String consumerSecret = args[1].toString();
		String accessToken = args[2].toString();
		String accessTokenSecret = args[3].toString();
		String topicName = args[4].toString();

		String[] arguments = args.clone();
		String[] key = Arrays.copyOfRange(arguments, 5, arguments.length);
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret)
								.setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret);

		TwitterStream twitter = new TwitterStreamFactory(cb.build()).getInstance();
		StatusListener listener = new StatusListener() {

			public void onStatus(Status status) {
				queue.offer(status);
			}

			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				System.out.println("Deletion notice id:" + statusDeletionNotice.getStatusId());
			}

			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				System.out.println("Limitation notice:" + numberOfLimitedStatuses);
			}

			public void onScrubGeo(long userId, long upToStatusId) {
				System.out.println("Scrub_geo event userId:" + userId + "upToStatusId:" + upToStatusId);
			}

			public void onStallWarning(StallWarning warning) {
				System.out.println("Stall warning:" + warning);
			}

			public void onException(Exception ex) {
				ex.printStackTrace();
			}
		};

		twitter.addListener(listener);
		FilterQuery query = new FilterQuery().track(key);
		twitter.filter(query);


		Properties properties = new Properties();
		properties.put("metadata.broker.list", "localhost:9092");
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("acks", "all");
		properties.put("retries", 0);
		properties.put("batch.size", 16384);
		properties.put("linger.ms", 1);
		properties.put("buffer.memory", 33554432);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


		Producer<String, String> producer = new KafkaProducer<String, String>(properties);
		int i = 0;
		int j = 0;

		while (true) {
			Status ret = queue.poll();
			if (ret == null|| ret.getUser().getLocation()==null) {
				Thread.sleep(100);
			} else {
					System.out.println("Tweet:" + ret.getText());
					System.out.println("Loc: " + ret.getUser().getLocation());
					String latlong= GeoAPI.coordinates(ret.getUser().getLocation());
					System.out.println("Lat and Long: " + latlong);
					producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(j++), ret.getText()+","+latlong));
			}

		}
	}



}