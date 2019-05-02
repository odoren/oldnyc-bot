# oldnyc-bot

**oldnyc-bot** is a Twitter application that generates automated posts of geo-tagged historical images of New York City street corners. The images are sourced from the New York Public Library's [*Photographic Views of New York City, 1870s-1970s*](https://digitalcollections.nypl.org/collections/photographic-views-of-new-york-city-1870s-1970s-from-the-collections-of-the-ne-2#/?tab=about) collection and are accessed via the [OldNYC](https://www.oldnyc.org/) data package provided on the NYPL's [Space/Time Directory](http://spacetime.nypl.org/#data-oldnyc). This project is not affiliated with the NYPL or OldNYC and makes use only of materials that have been provisioned for the pubic domain, as outlined [here](https://www.nypl.org/help/about-nypl/legal-notices/website-terms-and-conditions).

## Build

oldnyc-bot runs on Python 3 standard libraries, as well as [boto3](https://github.com/boto/boto3) for streaming images via S3 and [tweepy](https://github.com/tweepy/tweepy) for posting of message and media content to Twitter. Additionally, the bot makes use of the [Flickr places API](https://www.flickr.com/services/api/flickr.places.findByLatLon.html) to gather qualitative neighborhood information from OldNYC's geographic coordinates. 

## How to run

Coming soon...

## Todo
* Modify the `select_place()` method to re-generate the `content.json` file when all available places have been posted.
* Add functionality to append additional place objects to `content.json` while the bot is running.
* Implement push notifications for error logging.
