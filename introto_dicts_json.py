##
## Topic: Dictionaries and Tweets 
##

import pandas as pd

## Intro to Dictionaries
#
# Below is a sample "dictionary".  A dictionary maps a set of objects (keys) 
# to another set of objects (values). A Python dictionary is a mapping of 
# unique keys to values. The keys must be strings, but the values that the 
# keys map to can be any Python type (string, number, list, dictionary, ...).
# The keys and values are separated by ":", the key-value pairs are 
# separated by commas, and the entire dictionary is delimited by "{}".  
dict1 = {'item1': 'Dog',
         'number2': 273,
         'three': ['S','T','A','T','3250'],
         '4th entry': pd.Series([3,1,4,1,6]),
         'item5': 'Twitter'}

# We call one value at a time using "[]" as with a Series 
dict1['item1']
dict1['three']  # This value is a list
dict1['three'][2] # The 3rd list entry
dict1['4th entry'] # A Series
dict1  # The whole thing

# The value for a given key can be changed.
dict1['item5'] = 171
dict1

# We can add new key-value pairs
dict1['newkey'] = 'newvalue'
dict1

# We can also remove a key-value pair using 'del'
del dict1['number2']
dict1

# We can print out the keys
for key in dict1:
    print(key)
    
# Or print the values
for key in dict1:
    print(dict1[key])

# Dictionary values can also be dictionaries.
dict2 = {'time': '09:30',
         'dept': 'STAT',
         'numbers': {'3080': 'Data to Knowledge',
                     '3250': 'Data Analysis with Python',
                     '4630': 'Machine Learning'}}

dict2
dict2['dept']
dict2['numbers']
dict2['numbers']['3250'] # Refers to a key in the 'numbers' dictionary

## Tweets
#
# When we retrieve tweets from Twitter, they are sent to us in JSON
# format, which closely resembles how we define a dictionary.  Below is
# an example that has been converted and  formatted to make it a more 
# readable. 
tweet = {"created_at":"Sun Apr 01 15:03:37 +0000 2018",
         "id":980460683333722112,
         "id_str":"980460683333722112",
         "text":"@Steve2630Rock @919sports @BlueJays @KPILLAR4 @MLB @louisgregoire10  https:\/\/t.co\/pECh0qlrCv",
         "display_text_range":[68,68],
         "source":"\u003ca href=\"http:\/\/twitter.com\/download\/iphone\" rel=\"nofollow\"\u003eTwitter for iPhone\u003c\/a\u003e",
         "truncated":False,
         "in_reply_to_status_id":980458159025393665,
         "in_reply_to_status_id_str":"980458159025393665",
         "in_reply_to_user_id":3203376045,
         "in_reply_to_user_id_str":"3203376045",
         "in_reply_to_screen_name":"Steve2630Rock",
         "user":{"id":189155832,
                 "id_str":"189155832",
                 "name":"LP Guy \ud83c\udfce",
                 "screen_name":"LPGeek",
                 "location":"Longueuil, Qu\u00e9bec",
                 "url":"https:\/\/soundcloud.com\/louis-philippe-guy",
                 "description":"Produit \u00e0 Chicoutimi en 1984. Animateur @919sports de #Tribune919, r\u00e9alisateur de @JiCenLiberte. Sp\u00e9cialiste #F1. Descripteur Boxe. #IMFC #BlueJays et Papa de 2",
                 "translator_type":"none",
                 "protected":False,
                 "verified":False,
                 "followers_count":5261,
                 "friends_count":2470,
                 "listed_count":90,
                 "favourites_count":9972,
                 "statuses_count":32491,
                 "created_at":"Fri Sep 10 14:41:10 +0000 2010",
                 "utc_offset":-10800,
                 "time_zone":"Atlantic Time (Canada)",
                 "geo_enabled":True,
                 "lang":"fr",
                 "contributors_enabled":False,
                 "is_translator":False,
                 "profile_background_color":"000000",
                 "profile_background_image_url":"http:\/\/pbs.twimg.com\/profile_background_images\/634167409\/v9ws7bx1qi89bch52x8i.jpeg",
                 "profile_background_image_url_https":"https:\/\/pbs.twimg.com\/profile_background_images\/634167409\/v9ws7bx1qi89bch52x8i.jpeg",
                 "profile_background_tile":False,
                 "profile_link_color":"B81212",
                 "profile_sidebar_border_color":"C0DEED",
                 "profile_sidebar_fill_color":"DDEEF6",
                 "profile_text_color":"333333",
                 "profile_use_background_image":False,
                 "profile_image_url":"http:\/\/pbs.twimg.com\/profile_images\/948928458612240384\/0UeRZOD3_normal.jpg",
                 "profile_image_url_https":"https:\/\/pbs.twimg.com\/profile_images\/948928458612240384\/0UeRZOD3_normal.jpg",
                 "profile_banner_url":"https:\/\/pbs.twimg.com\/profile_banners\/189155832\/1494872256",
                 "default_profile":False,
                 "default_profile_image":False,
                 "following":None,
                 "follow_request_sent":None,
                 "notifications":None},
         "geo":None,
         "coordinates":None,
         "place":{"id":"36775d842cbec509",
                  "url":"https:\/\/api.twitter.com\/1.1\/geo\/id\/36775d842cbec509.json",
                  "place_type":"city",
                  "name":"Montr\u00e9al",
                  "full_name":"Montr\u00e9al, Qu\u00e9bec",
                  "country_code":"CA",
                  "country":"Canada",
                  "bounding_box":{"type":"Polygon",
                                  "coordinates":[[[-73.972965,45.410095],[-73.972965,45.705566],[-73.473085,45.705566],[-73.473085,45.410095]]]},
                  "attributes":{}},
         "contributors":None,
         "is_quote_status":False,
         "quote_count":0,
         "reply_count":0,
         "retweet_count":0,
         "favorite_count":0,
         "entities":{"hashtags":[],
                     "urls":[],
                     "user_mentions":[{"screen_name":"Steve2630Rock",
                                       "name":"steve rock",
                                       "id":3203376045,
                                       "id_str":"3203376045",
                                       "indices":[0,14]},
                                      {"screen_name":"919sports",
                                       "name":"91.9 Sports",
                                       "id":558687247,
                                       "id_str":"558687247",
                                       "indices":[15,25]},
                                      {"screen_name":"BlueJays",
                                       "name":"Toronto Blue Jays",
                                       "id":41468683,
                                       "id_str":"41468683",
                                       "indices":[26,35]},
                                      {"screen_name":"KPILLAR4",
                                       "name":"Kevin Pillar",
                                       "id":58990464,
                                       "id_str":"58990464",
                                       "indices":[36,45]},
                                      {"screen_name":"MLB",
                                       "name":"MLB",
                                       "id":18479513,
                                       "id_str":"18479513",
                                       "indices":[46,50]},
                                      {"screen_name":"louisgregoire10",
                                       "name":"louis gregoire",
                                       "id":4924303744,
                                       "id_str":"4924303744",
                                       "indices":[51,67]}],
                     "symbols":[],
                     "media":[{"id":980460678132707328,
                               "id_str":"980460678132707328",
                               "indices":[69,92],
                               "media_url":"http:\/\/pbs.twimg.com\/media\/DZtLysRWsAAk1OL.jpg",
                               "media_url_https":"https:\/\/pbs.twimg.com\/media\/DZtLysRWsAAk1OL.jpg",
                               "url":"https:\/\/t.co\/pECh0qlrCv",
                               "display_url":"pic.twitter.com\/pECh0qlrCv",
                               "expanded_url":"https:\/\/twitter.com\/LPGeek\/status\/980460683333722112\/photo\/1",
                               "type":"photo",
                               "sizes":{"thumb":{"w":150,
                                                 "h":150,
                                                 "resize":"crop"},
                                        "small":{"w":680,
                                                 "h":680,
                                                 "resize":"fit"},
                                        "large":{"w":694,
                                                 "h":694,
                                                 "resize":"fit"},
                                        "medium":{"w":694,
                                                  "h":694,
                                                  "resize":"fit"}}}]},
         "extended_entities":{"media":[{"id":980460678132707328,
                                        "id_str":"980460678132707328",
                                        "indices":[69,92],
                                        "media_url":"http:\/\/pbs.twimg.com\/media\/DZtLysRWsAAk1OL.jpg",
                                        "media_url_https":"https:\/\/pbs.twimg.com\/media\/DZtLysRWsAAk1OL.jpg",
                                        "url":"https:\/\/t.co\/pECh0qlrCv",
                                        "display_url":"pic.twitter.com\/pECh0qlrCv",
                                        "expanded_url":"https:\/\/twitter.com\/LPGeek\/status\/980460683333722112\/photo\/1",
                                        "type":"photo",
                                        "sizes":{"thumb":{"w":150,
                                                          "h":150,
                                                          "resize":"crop"},
                                                 "small":{"w":680,
                                                          "h":680,
                                                          "resize":"fit"},
                                                 "large":{"w":694,
                                                          "h":694,
                                                          "resize":"fit"},
                                                 "medium":{"w":694,
                                                           "h":694,
                                                           "resize":"fit"}}}]},
         "favorited":False,
         "retweeted":False,
         "possibly_sensitive":False,
         "filter_level":"low",
         "lang":"und",
         "timestamp_ms":"1522595017642"}
# This is the end of the tweet.  Here is a list of the keys:
for key in tweet:
    print(key)
    
# Some components:
tweet['text']

tweet['place'] # this is a dictionary
tweet['place']['full_name'] 

tweet['entities'] # this is a dictionary with various types of values
tweet['entities']['user_mentions'] # a list of dictionaries
tweet['entities']['user_mentions'][3] # the 4th list entry
tweet['entities']['user_mentions'][3]['name'] # 'name' from innermost dictionary

# There is a brief description for each part of the tweet at
#
#  https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object

#### Reading in tweets
#
# When we retrieve a set of tweets, the resulting file will contain one tweet
# per line -- a very long line!  Below is the original version of the above 
# tweet:
    
tweet = {"created_at":"Sun Apr 01 15:03:37 +0000 2018","id":980460683333722112,"id_str":"980460683333722112","text":"@Steve2630Rock @919sports @BlueJays @KPILLAR4 @MLB @louisgregoire10  https:\/\/t.co\/pECh0qlrCv","display_text_range":[68,68],"source":"\u003ca href=\"http:\/\/twitter.com\/download\/iphone\" rel=\"nofollow\"\u003eTwitter for iPhone\u003c\/a\u003e","truncated":false,"in_reply_to_status_id":980458159025393665,"in_reply_to_status_id_str":"980458159025393665","in_reply_to_user_id":3203376045,"in_reply_to_user_id_str":"3203376045","in_reply_to_screen_name":"Steve2630Rock","user":{"id":189155832,"id_str":"189155832","name":"LP Guy \ud83c\udfce","screen_name":"LPGeek","location":"Longueuil, Qu\u00e9bec","url":"https:\/\/soundcloud.com\/louis-philippe-guy","description":"Produit \u00e0 Chicoutimi en 1984. Animateur @919sports de #Tribune919, r\u00e9alisateur de @JiCenLiberte. Sp\u00e9cialiste #F1. Descripteur Boxe. #IMFC #BlueJays et Papa de 2","translator_type":"none","protected":false,"verified":false,"followers_count":5261,"friends_count":2470,"listed_count":90,"favourites_count":9972,"statuses_count":32491,"created_at":"Fri Sep 10 14:41:10 +0000 2010","utc_offset":-10800,"time_zone":"Atlantic Time (Canada)","geo_enabled":true,"lang":"fr","contributors_enabled":false,"is_translator":false,"profile_background_color":"000000","profile_background_image_url":"http:\/\/pbs.twimg.com\/profile_background_images\/634167409\/v9ws7bx1qi89bch52x8i.jpeg","profile_background_image_url_https":"https:\/\/pbs.twimg.com\/profile_background_images\/634167409\/v9ws7bx1qi89bch52x8i.jpeg","profile_background_tile":false,"profile_link_color":"B81212","profile_sidebar_border_color":"C0DEED","profile_sidebar_fill_color":"DDEEF6","profile_text_color":"333333","profile_use_background_image":false,"profile_image_url":"http:\/\/pbs.twimg.com\/profile_images\/948928458612240384\/0UeRZOD3_normal.jpg","profile_image_url_https":"https:\/\/pbs.twimg.com\/profile_images\/948928458612240384\/0UeRZOD3_normal.jpg","profile_banner_url":"https:\/\/pbs.twimg.com\/profile_banners\/189155832\/1494872256","default_profile":false,"default_profile_image":false,"following":null,"follow_request_sent":null,"notifications":null},"geo":null,"coordinates":null,"place":{"id":"36775d842cbec509","url":"https:\/\/api.twitter.com\/1.1\/geo\/id\/36775d842cbec509.json","place_type":"city","name":"Montr\u00e9al","full_name":"Montr\u00e9al, Qu\u00e9bec","country_code":"CA","country":"Canada","bounding_box":{"type":"Polygon","coordinates":[[[-73.972965,45.410095],[-73.972965,45.705566],[-73.473085,45.705566],[-73.473085,45.410095]]]},"attributes":{}},"contributors":null,"is_quote_status":false,"quote_count":0,"reply_count":0,"retweet_count":0,"favorite_count":0,"entities":{"hashtags":[],"urls":[],"user_mentions":[{"screen_name":"Steve2630Rock","name":"steve rock","id":3203376045,"id_str":"3203376045","indices":[0,14]},{"screen_name":"919sports","name":"91.9 Sports","id":558687247,"id_str":"558687247","indices":[15,25]},{"screen_name":"BlueJays","name":"Toronto Blue Jays","id":41468683,"id_str":"41468683","indices":[26,35]},{"screen_name":"KPILLAR4","name":"Kevin Pillar","id":58990464,"id_str":"58990464","indices":[36,45]},{"screen_name":"MLB","name":"MLB","id":18479513,"id_str":"18479513","indices":[46,50]},{"screen_name":"louisgregoire10","name":"louis gregoire","id":4924303744,"id_str":"4924303744","indices":[51,67]}],"symbols":[],"media":[{"id":980460678132707328,"id_str":"980460678132707328","indices":[69,92],"media_url":"http:\/\/pbs.twimg.com\/media\/DZtLysRWsAAk1OL.jpg","media_url_https":"https:\/\/pbs.twimg.com\/media\/DZtLysRWsAAk1OL.jpg","url":"https:\/\/t.co\/pECh0qlrCv","display_url":"pic.twitter.com\/pECh0qlrCv","expanded_url":"https:\/\/twitter.com\/LPGeek\/status\/980460683333722112\/photo\/1","type":"photo","sizes":{"thumb":{"w":150,"h":150,"resize":"crop"},"small":{"w":680,"h":680,"resize":"fit"},"large":{"w":694,"h":694,"resize":"fit"},"medium":{"w":694,"h":694,"resize":"fit"}}}]},"extended_entities":{"media":[{"id":980460678132707328,"id_str":"980460678132707328","indices":[69,92],"media_url":"http:\/\/pbs.twimg.com\/media\/DZtLysRWsAAk1OL.jpg","media_url_https":"https:\/\/pbs.twimg.com\/media\/DZtLysRWsAAk1OL.jpg","url":"https:\/\/t.co\/pECh0qlrCv","display_url":"pic.twitter.com\/pECh0qlrCv","expanded_url":"https:\/\/twitter.com\/LPGeek\/status\/980460683333722112\/photo\/1","type":"photo","sizes":{"thumb":{"w":150,"h":150,"resize":"crop"},"small":{"w":680,"h":680,"resize":"fit"},"large":{"w":694,"h":694,"resize":"fit"},"medium":{"w":694,"h":694,"resize":"fit"}}}]},"favorited":false,"retweeted":false,"possibly_sensitive":false,"filter_level":"low","lang":"und","timestamp_ms":"1522595017642"}

# The warning is because the words 'false', 'true', and 'null' appear without
# quotes, and are not recognized by Python.  The 'json.loads' function will
# change these to Python-friendly "False", "True", and "None".

# Start by loading the 'json' library
import json

# Read in the tweets and append them to 'tweetlist'
tweetlist = []
for line in open('tweetsample.json', 'r'): # Open the file of tweets
    tweetlist.append(json.loads(line))  # Add to 'tweetlist' after converting
    
tweetlist[0]  # The first tweet
type(tweetlist[0])

# Loop through the tweets
for tweet in tweetlist:
    print(tweet['id'])  # Print out the ID number of tweet
    print(tweet['user']['followers_count'])  # Print the number of followers



