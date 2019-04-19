import requests
import boto3
import json
import logging
import tweepy
import random
import os


# Logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
log_filename = 'logs/posting.log'
# Stream handler
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
# File handler
file_handler = logging.FileHandler(log_filename)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Define Twitter credentials for posting
consumer_key = os.environ['TWITTER_CONSUMER_KEY']
consumer_secret = os.environ['TWITTER_CONSUMER_SECRET']
access_token = os.environ['TWITTER_ACCESS_TOKEN']
access_token_secret = os.environ['TWITTER_TOKEN_SECRET']
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)



def compile_post_text(place):
	place_name = place['place']
	neighborhood = place['neighborhood_curr']
	year = place['year']
	status = "%s: %s (%s)" % (neighborhood, place_name, year)

	return status


def compile_post_media(place):
	filenames = [image['image_name'] for image in place['images'][:4]]
	files = []

	# Download from S3 to local tmp directory
	s3 = boto3.resource('s3')
	bucket = s3.Bucket(os.environ['AWS_BUCKET'])
	for filename in filenames:
		key = "oldnyc-bot/images/%s" % filename
		local_path = "tmp/%s" % filename

		try:
			bucket.download_file(key, local_path)
			files.append(local_path)
		except:
			logger.info('Image not found for place id %s.', place['family_id'])
			return None

	media_ids = [api.media_upload(file).media_id_string for file in files]
	os.system('rm -rf tmp/*')
	return media_ids


def post_tweet():
	place = select_place()

	status = compile_post_text(place)
	media_ids = compile_post_media(place)

	if status and media_ids:
		try:
			api.update_status(status=status, media_ids=media_ids)
			logger.info("Tweet successfully posted for place id %s.", place['family_id'])
		except:
			logger.info("Unable to post tweet for place id %s.", place['family_id'])


def select_place():
	with open('content.json') as f:
		data = json.load(f)

	if not data:
		logger.info('No more places available. Resetting content.json...')
		# Reset content.json
		# Wait
		# Call select_place() again
	else:
		choice = random.choice(data)
		logger.info("Selected id %s for posting.", choice['family_id'])
		data.remove(choice)
		logger.info("%d selections remaining in content.json.", len(data))

		with open('content.json', 'w') as f:
	 		json.dump(data, f, indent=4, sort_keys=True)

		return choice



def main():
	post_tweet()


if __name__ == "__main__":
	main()

