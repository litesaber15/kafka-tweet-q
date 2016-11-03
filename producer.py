from pykafka import KafkaClient
import tweepy
import json
from requests_aws4auth import AWS4Auth
from credentials import consumer_key, consumer_secret, access_token, access_token_secret

class StreamListener(tweepy.StreamListener):  
  def __init__(self):
    super(StreamListener, self).__init__()
    #filter by language to reduce possibility of non-ascii text
    self.supportedLang = ['en', 'es', 'fr', 'de', 'it', 'pt', 'ru', 'ar']
    #initialize kafka client
    client = KafkaClient(hosts="127.0.0.1:9092")
    #get topic from kafka client
    self.topic = client.topics['Tweets']

  def on_status(self, status):
  	# function that is called when a new tweet is received
    try:
    	#get json data of response object
      json_data = status._json

      #check if tweet is of supported language and has geo-location
      if json_data['coordinates'] is not None and json_data['lang'] in self.supportedLang:
        print('@'+json_data['user']['screen_name'])
        print(json_data['text'].lower().encode('ascii','ignore').decode('ascii'))
        
        #create JSON object of relevant content from response
        skimmed = {
                    'id': json_data['id'],
                    'time': json_data['timestamp_ms'],
                    'text': json_data['text'].lower().encode('ascii','ignore').decode('ascii'),
                    'coordinates': json_data['coordinates'],
                    'place': json_data['place'],
                    'handle': json_data['user']['screen_name']
                  }

        # add tweet to kafka
        with self.topic.get_sync_producer() as producer:
            producer.produce('Tweet from @{}: {}\n'.format(skimmed['handle'], skimmed['text']))

    except Exception as e:
      print(e)
      pass


#authentication of twitter streaming api
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

#initialize tweet streamer
streamer = tweepy.Stream(auth=auth, listener=StreamListener(), timeout=30000)

#filter for these terms in tweet text
terms = [ 
        'trump', 'usa', 'wanderlust'
        ,'movies','sports','music','finance','technology'
        ,'fashion','science','travel','health','cricket'
        ,'india', 'love', 'shit','bjp', 'aap', 'india'
        ,'epl', 'football','goal', '1-0' ]

#stream and add to queue
streamer.filter(None,terms)

