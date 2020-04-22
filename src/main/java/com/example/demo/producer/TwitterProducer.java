package com.example.demo.producer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {

	private Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
	
	// Please register yourself onto Twitter API (https://developer.twitter.com/en)
	// and get the secrets and token for your usage.
	private String consumerKey = "XXXXX";
	private String consumerSecret = "XXXXX";
	private String token = "XXXXX";
	private String secret = "XXXXX";
	List<String> terms = Lists.newArrayList("quotes");

	public static void main(String[] args) {
		System.out.println("Hello");

		new TwitterProducer().run();
	}

	public void run() {
		logger.info("Setup");
		// set up blocking queue : Be sure to size these properly based on
		// expected TPS of your stream
		BlockingQueue<String> msgQueue = new LinkedBlockingDeque<String>(1000);

		// Create a twitter client
		Client hosebirdClient = createTwitterClients(msgQueue);

		// Attempts to establish a connection.
		hosebirdClient.connect();

		// Create a kafka producer
		KafkaProducer<String, String> producer = createKafkaProducer();
		
		// Adding a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Stopping application");
			hosebirdClient.stop();
			producer.close();
		}));

		// Loops to send tweets to kafka on a different thread, or multiple different threads....
		while (!hosebirdClient.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				hosebirdClient.stop();
			}
			String topicName = "twitter_topic";
			if (msg != null) {
				logger.info(msg);
				producer.send(new ProducerRecord<String, String>(topicName, msg), new Callback() {
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if(exception != null) {
							logger.error("Error while sending twitter message to the topic" + topicName);
						}
					}
				});
			}
		}
		logger.info("End of application!");
	}

	private KafkaProducer<String, String> createKafkaProducer() {
		String bootstrapServer = "127.0.0.1:9092";
		
		Properties prop = new Properties();
		prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// TO have a safe producer where the writes are acknowledged, we should use below settings
		prop.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		prop.setProperty(ProducerConfig.ACKS_CONFIG	, "all");
		prop.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		prop.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
		
		// To have a High throughput producer, we should use below settings
		prop.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		prop.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); //32KB

		return new KafkaProducer<>(prop);
	}

	public Client createTwitterClients(BlockingQueue<String> msgQueue) {
		/**
		 * Declare the host you want to connect to, the endpoint, and
		 * authentication (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		// Creating a client
		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional:
																				// mainly
																				// for
																				// the
																				// logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));
		// .eventMessageQueue(eventQueue); // optional: use this if you want to
		// process client events

		Client hosebirdClient = builder.build();

		return hosebirdClient;
	}
}
