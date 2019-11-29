import urllib
import requests
import boto3
import pandas as pd
import json
import os
import argparse
import logging


# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
log_filename = 'processing.log'
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

# Environment variables
aws_bucket = os.environ['AWS_BUCKET']
google_maps_api_key = os.environ['GOOGLE_MAPS_API_KEY']


# Converts source data CSV to Pandas data frame to facilitate maniuplation of values and columns
# Generates processed.csv for readable output / manual debugging
def import_source_data_csv(file):
	df = pd.read_csv(file)

	df.rename(columns={
		'$.data.imageId': 'place_id',
		'$.data.folder': 'place',
		'validSince': 'year',
		'$.data.text': 'description',
		'$.data.url': 'oldnyc_url',
		'$.data.imageUrl': 'oldnyc_image_url',
		'$.data.uuid': 'nypl_uuid',
		'$.data.nyplUrl': 'nypl_url',
		}, inplace=True)
	df['lat'] = df.apply(lambda row: json.loads(row['geometry'])['coordinates'][1], axis=1)
	df['lng'] = df.apply(lambda row: json.loads(row['geometry'])['coordinates'][0], axis=1)
	df['image_name'] = df.apply(lambda row: row['oldnyc_image_url'].split('/')[-1], axis=1)
	df['description'] = df.apply(lambda row: '' if row['description'] == '0' else row['description'], axis=1)
	df.drop(['name', 'type', 'validUntil', 'geometry'], axis=1, inplace=True)

	return df


# Generates main json document with post-ready data
def generate_place_objects(df):
	place_objects = (df.groupby(['place_id', 'borough', 'year', 'place', 'description', 'lat', 'lng', 'nypl_uuid', 'nypl_url'], as_index=False)
			.apply(lambda x: x[['id', 'image_name', 'oldnyc_url', 'oldnyc_image_url']].to_dict('r'))
			.reset_index()
			.rename(columns={0:'images'})
			.to_json(force_ascii=False, orient='records'))

	return place_objects


# Get nieghborhood from geo coordinates using Google Maps API reverse geocode endpoint
def reverse_geocode(place, neighborhood_dict):
	place_id = str(place['place_id'])
	lat = str(place['lat'])
	lng = str(place['lng'])

	url = 'https://maps.googleapis.com/maps/api/geocode/json'
	payload = {
	    'result_type': 'neighborhood',
		'latlng': lat + ',' + lng,
		'key': google_maps_api_key
	}
	headers = {
		'Content-Type': 'application/json'
	}

	try: 
		r = requests.get(url, params=payload, headers=headers)
		neighborhood = json.loads(r.text)['results'][0]['formatted_address'].split(',')[0]
		place['neighborhood_original'] = neighborhood
		logger.info('Retrived neighborhood for id %s [%s,%s]: %s', place_id, lat, lng, neighborhood)
		# Check for neighborhood transform
		if neighborhood in neighborhood_dict:
			place['neighborhood'] = neighborhood_dict[neighborhood]
			logger.info('Neighborhod transformed from %s to %s', neighborhood, neighborhood_dict[neighborhood])
		else:
			place['neighborhood'] = neighborhood
	except:
		logger.info('Could not get neighborhood for id %s at coordinates [%s,%s]', place_id, lat, lng)
		return ''


# Save image to local directory 
def download_image_local(image_name, image_url):
	path = 'images/'

	try:
		image = requests.get(image_url).content
		f = open(path + image_name, 'wb')
		f.write(image)
		f.close()
		logger.info('Saved image from %s', image_url)
	except:
		logger.info('Could not save image from %s', image_url)


# API stream of source images to dedicated S3 bucket
def download_image_s3(bucket, image_name, image_url):
	key = 'oldnyc-bot/images/' + image_name

	try:
		r = requests.get(image_url, stream=True)
		bucket.upload_fileobj(r.raw, key)
		logger.info('Saved image to %s/%s', bucket.name, key)
	except:
		logger.info('Could not save image from %s', image_url)


def main():
	parser = argparse.ArgumentParser(description='Process oldnyc source data')
	parser.add_argument('--source-file', required=True)
	parser.add_argument('--download-to-S3', action='store_true', dest='download_to_s3', default=False)
	parser.add_argument('--reverse-geocode', action='store_true', dest='reverse_geocode', default=False)
	args = parser.parse_args()

	logger.info("Importing source data...")
	df = import_source_data_csv(args.source_file)

	# Loop through dataframe to download source images and store to S3 
	if args.download_to_s3 == True:
		logger.info("Downloading images from source...")
		s3 = boto3.resource('s3')
		bucket = s3.Bucket(aws_bucket)
		for index, row in df.iterrows():
			download_image_s3(bucket, row['image_name'], row['oldnyc_image_url'])

	# Generate JSON file from dataframe
	logger.info("Generating place objects...")
	place_objects = json.loads(generate_place_objects(df))

	places = []
	if args.reverse_geocode == True:
		# Append neighborhood data to JSON place records
		logger.info("Populating neighborhood data...")
		# Load neighborhood dict
		with open('neighborhood_dict.json') as f:
			neighborhood_dict = json.load(f)	
		for place in place_objects:
			reverse_geocode(place, neighborhood_dict)
			places.append(place)
	else:
		places = place_objects # Save JSON without neighborhood data

	# Save final JSON file
	with open('places.json', 'w', encoding='utf-8') as json_file:
		json.dump(places, json_file, indent=4, sort_keys=True)

	logger.info("Done.")


if __name__ == "__main__":
	main()

