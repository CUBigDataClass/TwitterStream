/**
 * Created by connorboyd on 3/18/14.
 */

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterStream {

	public static void main(String[] args)
	{

		String consumerKey    = "";
		String consumerSecret = "";
		String accessToken    = "";
		String tokenSecret    = "";
		try
		{
			oauth(consumerKey, consumerSecret, accessToken, tokenSecret);
		}
		catch (Exception e)
		{
			System.err.println("Caught exception");
		}
	}


	public static void oauth(String consumerKey, String consumerSecret, String token, String secret) throws InterruptedException {
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		// add some track terms
		endpoint.trackTerms(Lists.newArrayList("craftbeer", "beer"));

		Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);

		// Create a new BasicClient. By default gzip is enabled.
		Client client = new ClientBuilder()
				.hosts(Constants.STREAM_HOST)
				.endpoint(endpoint)
				.authentication(auth)
				.processor(new StringDelimitedProcessor(queue))
				.build();

		// Establish a connection
		client.connect();
		FileWriter file = null;
		try {
			file = new FileWriter("beerTweets.json");
		}
		catch (IOException e)
		{
			System.err.println("Error opening output file");
			System.err.println( e.getMessage() );
			System.exit(-1);
		}
		// Do whatever needs to be done with messages
		for (int msgRead = 0; msgRead < 1000; msgRead++) {
			String msg = queue.take();
			try {
				file.write(msg);
			}
			catch (IOException e)
			{
				System.err.println("Error writing to output file");
				System.err.println( e.getMessage() );
			}
			System.out.println((msgRead + 1) + " completed");
		}
		try {
			file.close();
		}
		catch (IOException e)
		{
			System.err.println("Error closing output file");
			System.err.println( e.getMessage() );
		}
		client.stop();

	}
}
