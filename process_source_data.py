import urllib
import requests
import boto3
import pandas as pd
import json
import os
import sys
import logging


# Logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
log_filename = 'logs/processing.log'
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


# Converts source data CSV to Pandas data frame to facilitate maniuplation of values and columns
# Generates processed.csv for readable output / manual debugging
def import_source_data_csv(file):
	df = pd.read_csv(file)
    
	df.rename(columns={'validSince': 'year', '$.data.imageId': 'family_id', '$.data.imageUrl': 'original_image_url'}, inplace=True)  
	df['borough'] = df.apply(lambda row: row['name'].split(': ')[0], axis=1)
	df['place'] = df.apply(lambda row: row['name'].split(': ')[1], axis=1)
	df['coordinates'] = df.apply(lambda row: json.loads(row['geometry'])['coordinates'], axis=1)
	df['neighborhood_hist'] = df.apply(lambda row: get_neighborhood(row['id'], row['coordinates']), axis=1)
	df['neighborhood_curr'] = df.apply(lambda row: neighborhood_transform(row['neighborhood_hist']), axis=1)
	df['image_name'] = df.apply(lambda row: row['original_image_url'].split('/')[-1], axis=1)
	df.drop(['name', 'type', 'validUntil', '$.data.uuid', '$.data.text', '$.data.folder', '$.data.url', '$.data.nyplUrl', 'geometry'], axis=1, inplace=True)
	    
	df.to_csv('logs/processed.csv', encoding='utf-8', index=False)
	return df


# Retrieves neighborhood name from coordinates via Google Maps API
def get_neighborhood(place_id, coordinates):
	lon = str(coordinates[0])
	lat = str(coordinates[1])

	url = 'https://maps.googleapis.com/maps/api/geocode/json'
	payload = {
	    'result_type': 'neighborhood',
		'latlng': lat + ',' + lon,
		'key': os.environ['GOOGLE_MAPS_API_KEY']
	}
	headers = {
		'Content-Type': 'application/json'
	}
	try: 
		r = requests.get(url, params=payload, headers=headers)
		neighborhood = json.loads(r.text)['results'][0]['formatted_address'].split(',')[0]
		logger.info('Retrived neighborhood for id %s [%s,%s]: %s', place_id, lat, lon, neighborhood)
		return neighborhood
	except:
		logger.info('Could not get neighborhood for id %s at coordinates [%s,%s].', place_id, lat, lon)
		return ''


# Manual transformation of obsolete neighborhood names to modern usage
def neighborhood_transform(neighborhood):
	with open('src/neighborhood_dict.json') as f:
    	neighborhood_dict = json.load(f)

		if neighborhood in neighborhood_dict:
			return neighborhood_dict[neighborhood]
		else:
			return neighborhood


# Generates main json document with post-ready data
def generate_content_json(df):
	content = (df.groupby(['family_id', 'borough', 'neighborhood_hist', 'neighborhood_curr', 'year', 'place'], as_index=False)
			.apply(lambda x: x[['id', 'original_image_url', 'image_name']].to_dict('r'))
			.reset_index()
			.rename(columns={0:'images'})
			.to_json(force_ascii=False, orient='records'))

	with open('content.json', 'w', encoding='utf-8') as file:
		json.dump(json.loads(content), file, indent=4, sort_keys=True)


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
	logger.info("Importing source data...")
	df = import_source_data_csv(sys.argv[1])

	logger.info("Downloading images from source platform...")
	s3 = boto3.resource('s3')
	bucket = s3.Bucket(os.environ['AWS_BUCKET'])
	for index, row in df.iterrows():
		download_image_s3(bucket, row['image_name'], row['original_image_url'])

	logger.info("Generating content json file...")
	generate_content_json(df)

	logger.info("Done.")


if __name__ == "__main__":
	main()



