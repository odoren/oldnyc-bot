import requests
import boto3
import json
import logging
import tweepy
import random
import os


# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
log_filename = 'posting.log'
# Stream handler
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
# File handler
file_handler = logging.FileHandler(log_filename)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Get credentials from external file
credentials_path = '';
with open(credentials_path) as c:
	credentials = json.load(c)
	aws_bucket = credentials['aws_bucket']
	twitter_consumer_key = credentials['twitter_consumer_key']
	twitter_consumer_secret = credentials['twitter_consumer_secret']
	twitter_access_token = credentials['twitter_access_token']
	twitter_access_token_secret = credentials['twitter_access_token_secret']

# Twitter auth
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)


# Generate text content of tweet and format for tweepy API
def compile_post_text(place):
	place_desc = place['place']
	neighborhood = place['neighborhood']
	year = place['year']
	status = "%s: %s (%s)" % (neighborhood, place_desc, year)

	return status


# Find place image(s) in S3 and format for tweepy API
def compile_post_media(place):
	filenames = [image['image_name'] for image in place['images'][:4]]
	files = []

	# Download from S3 to local tmp directory
	s3 = boto3.resource('s3')
	bucket = s3.Bucket(aws_bucket)
	for filename in filenames:
		key = "oldnyc-bot/images/%s" % filename
		local_path = "tmp/%s" % filename

		try:
			bucket.download_file(key, local_path)
			files.append(local_path)
		except:
			logger.info('Image not found for place id %s.', place['place_id'])
			return None

	media_ids = [api.media_upload(file).media_id_string for file in files]
	os.system('rm -rf tmp/*')
	return media_ids


# Randomly select a place object and remove it from content.json
def select_place():
	with open('places.json') as f:
		data = json.load(f)

	if not data:
		logger.info('No more places available. Reset places.json.')
	else:
		choice = random.choice(data)
		logger.info("Selected id %s for posting.", choice['place_id'])
		data.remove(choice)
		logger.info("%d selections remaining in places.json.", len(data))

		with open('places.json', 'w') as f:
	 		json.dump(data, f, indent=4, sort_keys=True)

		return choice


def post_tweet():
	place = select_place()
	status = compile_post_text(place)
	media_ids = compile_post_media(place)

	if status and media_ids:
		try:
			api.update_status(status=status, media_ids=media_ids)
			logger.info("Tweet successfully posted for place id %s.", place['place_id'])
		except:
			logger.info("Unable to post tweet for place id %s.", place['place_id'])


def main():
	post_tweet()


if __name__ == "__main__":
	main()