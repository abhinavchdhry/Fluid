import requests, json
from bs4 import BeautifulSoup as bs

categories = [
	'automotive',
 	'auto-and-home-improvement',
 	'cruise-travel',
	'beauty-and-spas',
 	'baby-kids-and-toys',
 	'flights-and-transportation',
	'food-and-drink',
 	'collectibles',
 	'hotels-and-accommodations',
	'health-and-fitness',
 	'electronics',
 	'tour-travel',
	'home-improvement',
 	'entertainment-and-media', 	 
	'personal-services',
 	'for-the-home', 	 
	'retail',
 	'groceries-household-and-pets',
	'things-to-do',
 	'health-and-beauty',
	'jewelry-and-watches',
	'mens-clothing-shoes-and-accessories',
	'sports-and-outdoors',
	'womens-clothing-shoes-and-accessories'
]

def get_all_US_divisions():
	divs_endpoint = 'https://partner-api.groupon.com/division.json'
	r = requests.get(divs_endpoint)
	if r.status_code == 200:
		divs = r.json()['divisions']
		div_ids = [d['id'] for d in divs]
	else:
		div_ids = []
	return div_ids

divisions = get_all_US_divisions()

deals_endpoint = 'https://partner-api.groupon.com/deals.json?'
tsToken = 'US_AFF_0_201236_212556_0'

def process_deal_object_json(deal):
	if 'uuid' in deal and 'title' in deal and 'highlightsHtml' in deal:
		outobj = {}
		outobj['id'] = deal['uuid']
		outobj['title'] = deal['title']
#		print(deal['highlightsHtml'])
		if deal['highlightsHtml'] is not None:
			body = bs(deal['highlightsHtml'], 'lxml').text
		else:
			body = ""
		outobj['body'] = body
		
		if 'tags' in deal:
			t = deal['tags']
			tagstring = ''
			for i in range(len(t)):
				tagstring += t[i]['name']
				if i != len(t) - 1:
					tagstring += ', '
		outobj['tags'] = tagstring
		return outobj
	else:
		return None

def get_deals_for_division_and_category(division_id, cat_id, count, fp):
	url = deals_endpoint + 'tsToken=' + tsToken + '&division_id=' + division_id + '&filters=category:' + cat_id + '&offset=0&limit=' + str(count)
	res = requests.get(url)
	if res.status_code == 200:
		print('Status code 200 for cat:' + cat_id)
		deals = res.json()['deals']
		print(len(deals), " deals returned")
		for d in deals:
			obj = process_deal_object_json(d)
			if obj is not None:
				obj['category'] = cat_id
				json.dump(obj, fp)
				fp.write('\n')
			

# Main
for div in divisions:
	print('Current division: ' + div)
	try:
		fp = open('../ads/' + div + '.data', 'w')
	except:
		print('Failed to open file for ' + div + '!')
		continue

	for cat in categories:
		get_deals_for_division_and_category(div, cat, 1000, fp)
	fp.flush()
	fp.close()
	print('------------------------------------\n\n')
