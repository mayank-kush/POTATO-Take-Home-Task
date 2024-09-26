from flask import Flask, request, render_template
from pymongo import MongoClient
from datetime import datetime


app = Flask(__name__)

# Connect to MongoDB
mongo_uri = "mongodb+srv://ermayankkush2482:Mayank24@tweetdata.g4u39.mongodb.net/?retryWrites=true&w=majority&appName=Tweetdata"
client = MongoClient(mongo_uri)
db = client['Tweets'] 
collection = db['tweets_cleaned_large'] 

# function to extract hour from datetime
def get_hour_of_day(created_at):
    return created_at.hour if isinstance(created_at, datetime) else None

@app.route('/', methods=['GET', 'POST'])
def search_tweets():
    
    response_data = None
    error_message = None

    if request.method == 'POST':
        
        term = request.form.get('term')

        if not term:
            error_message = "Please provide a search term."
        else:
            # MongoDB query to find tweets containing the search term
            query = {"text": {"$regex": term, "$options": "i"}} 
            tweets = list(collection.find(query))

            if not tweets:
                error_message = f"No tweets found for term: {term}"
            else:
                # Initialize metrics
                tweet_count_by_day = {}
                unique_users = set()
                total_likes = 0
                place_ids = set()
                tweet_times_of_day_count = {} 
                user_tweet_count = {}

                
                for tweet in tweets:
                    # Count tweets by day
                    tweet_date = tweet['created_at'].strftime('%Y-%m-%d')
                    tweet_count_by_day[tweet_date] = tweet_count_by_day.get(tweet_date, 0) + 1

                    # Count unique users
                    unique_users.add(tweet['author_id'])

                    # Total likes
                    total_likes += tweet['like_count']

                    # Track place IDs (if available)
                    if tweet.get('place_id'):
                        place_ids.add(tweet['place_id'])

                    # Track and count the time of day 
                    tweet_time_of_day = get_hour_of_day(tweet['created_at'])
                    if tweet_time_of_day is not None:
                        tweet_times_of_day_count[tweet_time_of_day] = tweet_times_of_day_count.get(tweet_time_of_day, 0) + 1

                    # Count tweets per user
                    user = tweet['author_handle']
                    user_tweet_count[user] = user_tweet_count.get(user, 0) + 1

                # Calculate average likes
                average_likes = round(total_likes / len(tweets),1) if tweets else 0

                # user who posted the most tweets
                top_user = max(user_tweet_count, key=user_tweet_count.get)

                
                response_data = {
                    "total_tweets": len(tweets),
                    "tweets_by_day": dict(sorted(tweet_count_by_day.items())),
                    "unique_users": len(unique_users),
                    "average_likes": average_likes,
                    "place_ids": list(place_ids),
                    "tweet_times_of_day_count": dict(sorted(tweet_times_of_day_count.items())),
                    "top_user": top_user,
                    "top_user_tweet_count": user_tweet_count[top_user]
                }

    return render_template('index.html', response_data=response_data, error_message=error_message)


if __name__ == '__main__':
    app.run(debug=True)
