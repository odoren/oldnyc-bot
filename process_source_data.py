import urllib
import requests
import boto3
import pandas as pd
import json
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


# Flickr credentials for retrieving neighborhood data
flickr_key = 'f5c33286ba52899be026f8d09e5e0ab4'
flickr_secret = 'ba3dd7afdebf2c06'
flickr_endpoint = 'https://api.flickr.com/services'


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

def get_neighborhood(place_id, coordinates):
	lon = str(coordinates[0])
	lat = str(coordinates[1])

	url = flickr_endpoint + '/rest/'
	payload = {
		'method': 'flickr.places.findByLatLon',
		'format': 'json',
		'api_key': flickr_key,
		'lon': lon,
		'lat': lat,
		'accuracy': '16',
		'nojsoncallback': '1'
	}
	headers = {
		'Content-Type': 'application/json'
	}

	try: 
		r = requests.get(url, params=payload, headers=headers)
		neighborhood = json.loads(r.text)['places']['place'][0]['woe_name']
		logger.info('Retrived neighborhood for id %s [%s,%s]: %s', place_id, lat, lon, neighborhood)
		return neighborhood
	except:
		logger.info('Could not get neighborhood for id %s at coordinates [%s,%s].', place_id, lat, lon)
		return ''

def neighborhood_transform(neighborhood):
	neighborhood_dict = {
		'Georgetown': 'Bergen Beach',
		'Madison': 'Sheepshead Bay',
		'Rugby': 'East Flatbush',
		'New Lots': 'East New York',
		'Adelphi': 'Clinton Hill',
		'City Line': 'East New York',
		'Remsen Village': 'Canarsie',
		'Weeksville': 'Crown Heights',
		'Ocean Hill': 'Bedford-Stuyvesant',
		'Bedford': 'Bedford-Stuyvesant',
		'Farragut': 'East Flatbush',
		'Little Odessa': 'Brighton Beach',
		'Blissville': 'Sunnyside',
		'Morgan Avenue': 'East Williamsburg',
		'Wingate': 'Prospect Lefferts Gardens',
		'Paerdegat': 'Canarsie',
		'Starrett City': 'East New York',
		'Northwestern Brooklyn': 'Dumbo',
		'Haberman': 'East Williamsburg',
		'Little Poland': 'Greenpoint',
		'Plum Beach': 'Sheepshead Bay'
	}

	if neighborhood in neighborhood_dict:
		return neighborhood_dict[neighborhood]
	else:
		return neighborhood

# generate main json document with information needed for posting
def generate_content_json(df):
	content = (df.groupby(['family_id', 'borough', 'neighborhood_hist', 'neighborhood_curr', 'year', 'place'], as_index=False)
			.apply(lambda x: x[['id', 'original_image_url', 'image_name']].to_dict('r'))
			.reset_index()
			.rename(columns={0:'images'}))

	with open('content.json', 'w', encoding='utf-8') as file:
		content.to_json(file, force_ascii=False, orient='records')

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
	bucket = s3.Bucket('odoren-aws')
	for index, row in df.iterrows():
		download_image_s3(bucket, row['image_name'], row['original_image_url'])
		#download_image_local(row['image_name'], row['original_image_url'])
		

	logger.info("Generating content json file...")
	generate_content_json(df)

	logger.info("Done.")


if __name__ == "__main__":
	main()



