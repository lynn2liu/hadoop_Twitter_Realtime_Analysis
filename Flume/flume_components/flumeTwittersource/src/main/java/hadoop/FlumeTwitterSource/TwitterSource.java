package hadoop.FlumeTwitterSource;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

/**
 * A Flume Source, which pulls data from Twitter's streaming API. Currently,
 * this only supports pulling from the sample API, and only gets new status
 * updates.
 */
public class TwitterSource extends AbstractSource implements EventDrivenSource, Configurable {
    // add two properties locations and follows to TwitterStream

    private final static Logger logger = LoggerFactory.getLogger(TwitterSource.class);

    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessTokenSecret;

    private String[] keywords;
    private long [] follows;
    private double [][] locations;

    private  TwitterStream twitterStream;

    /**
     * The initialization method for the Source. The context contains all the
     * Flume configuration info, and can be used to retrieve any configuration
     * values necessary to set up the Source.
     */
    public void configure(Context context) {
        consumerKey = context.getString(SourceConstants.CONSUMER_KEY_KEY);
        consumerSecret = context.getString(SourceConstants.CONSUMER_SECRET_KEY);
        accessToken = context.getString(SourceConstants.ACCESS_TOKEN_KEY);
        accessTokenSecret = context.getString(SourceConstants.ACCESS_TOKEN_SECRET_KEY);

        TwitterQueryParameterParser tqpp = new TwitterQueryParameterParser();
        String keywordString = context.getString(SourceConstants.KEYWORDS_KEY, "");
        keywords = tqpp.getKeywords(keywordString);

        String followString = context.getString(SourceConstants.FOLLOW_IDS_KEY, "");
        follows = tqpp.getFollows(followString);

        String locationString = context.getString(SourceConstants.LOCATIONS_KEY, "");
        locations = tqpp.getLocations(locationString);

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey(consumerKey)
                .setOAuthConsumerSecret(consumerSecret)
                .setOAuthAccessToken(accessToken)
                .setOAuthAccessTokenSecret(accessTokenSecret)
                .setJSONStoreEnabled(true);

        twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
    }

    /**
     * Start processing events. This uses the Twitter Streaming API to sample
     * Twitter, and process tweets.
     */
    @Override
    public void start() {
        // The channel is the piece of Flume that sits between the Source and Sink,
        // and is used to process events.
        final ChannelProcessor channel = getChannelProcessor();
        final Map<String, String> headers = new HashMap<String, String>();

        // The StatusListener is a twitter4j API, which can be added to a Twitter
        // stream, and will execute methods every time a message comes in through
        // the stream.
        StatusListener listener = new StatusListener(){
            // The onStatus method is executed every time a new tweet comes in.
            public void onStatus(Status status) {
                // The EventBuilder is used to build an event using the headers and
                // the raw JSON of a tweet
                // shouldn't log possibly sensitive customer data
                System.out.println(status.getUser().getName() + " : " + status.getText());
                logger.debug("Tweet arrived");

                headers.put("timestamp", String.valueOf(status.getCreatedAt().getTime()));
                Event event = EventBuilder.withBody(
                        DataObjectFactory.getRawJSON(status).getBytes(), headers);

                channel.processEvent(event);
            }

            // This listener will ignore everything except for new tweets
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
            public void onScrubGeo(long arg0, long arg1) { }
            public void onStallWarning(StallWarning arg0) { }

            public void onException(Exception ex) {
                logger.error("Error while listening to Twitter stream.", ex);
            }
        };

        logger.debug("Setting up Twitter sample stream using consumer key" + consumerKey +
                " and access token " + accessToken);

        twitterStream.addListener(listener);

        // Set up a filter to pull out industry relevant tweets with no keyword/ follow/ location
        if (keywords.length == 0 && follows.length == 0 && locations.length == 0) {
            logger.debug("Starting up Twitter sampling...");
            twitterStream.sample();

        } else {
            logger.debug("Starting up Twitter filtering...");

            FilterQuery query = new FilterQuery();
            if (keywords.length > 0) query.track(keywords);
            if (follows.length > 0) query.follow(follows);
            if (locations.length > 0) query.locations(locations);

            twitterStream.filter(query);
        }

        super.start();

    }


    @Override
    public void stop() {
        logger.debug("Shutting down Twitter sample stream...");
        twitterStream.shutdown();
        super.stop();
    }



}
