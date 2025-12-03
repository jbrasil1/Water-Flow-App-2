from flask import Flask, render_template, request
import requests
from dateutil import parser
import re
import math

app = Flask(__name__)

counties = sorted([
    "Alameda", "Alpine", "Amador", "Butte", "Calaveras", "Colusa", "Contra Costa",
    "Del Norte", "El Dorado", "Fresno", "Glenn", "Humboldt", "Imperial", "Inyo",
    "Kern", "Kings", "Lake", "Lassen", "Los Angeles", "Madera", "Marin", "Mariposa",
    "Mendocino", "Merced", "Modoc", "Mono", "Monterey", "Napa", "Nevada", "Orange",
    "Placer", "Plumas", "Riverside", "Sacramento", "San Benito", "San Bernardino",
    "San Diego", "San Francisco", "San Joaquin", "San Luis Obispo", "San Mateo",
    "Santa Barbara", "Santa Clara", "Santa Cruz", "Shasta", "Sierra", "Siskiyou",
    "Solano", "Sonoma", "Stanislaus", "Sutter", "Tehama", "Trinity", "Tulare",
    "Tuolumne", "Ventura", "Yolo", "Yuba"
])

county_to_fips = {
    "Alameda": "06001", "Alpine": "06003", "Amador": "06005", "Butte": "06007",
    "Calaveras": "06009", "Colusa": "06011", "Contra Costa": "06013", "Del Norte": "06015",
    "El Dorado": "06017", "Fresno": "06019", "Glenn": "06021", "Humboldt": "06023",
    "Imperial": "06025", "Inyo": "06027", "Kern": "06029", "Kings": "06031", "Lake": "06033",
    "Lassen": "06035", "Los Angeles": "06037", "Madera": "06039", "Marin": "06041",
    "Mariposa": "06043", "Mendocino": "06045", "Merced": "06047", "Modoc": "06049",
    "Mono": "06051", "Monterey": "06053", "Napa": "06055", "Nevada": "06057",
    "Orange": "06059", "Placer": "06061", "Plumas": "06063", "Riverside": "06065",
    "Sacramento": "06067", "San Benito": "06069", "San Bernardino": "06071",
    "San Diego": "06073", "San Francisco": "06075", "San Joaquin": "06077",
    "San Luis Obispo": "06079", "San Mateo": "06081", "Santa Barbara": "06083",
    "Santa Clara": "06085", "Santa Cruz": "06087", "Shasta": "06089", "Sierra": "06091",
    "Siskiyou": "06093", "Solano": "06095", "Sonoma": "06097", "Stanislaus": "06099",
    "Sutter": "06101", "Tehama": "06103", "Trinity": "06105", "Tulare": "06107",
    "Tuolumne": "06109", "Ventura": "06111", "Yolo": "06113", "Yuba": "06115"
}

def parse_rdb(text):
    lines = text.splitlines()
    data = []
    headers = None
    in_data = False
    for line in lines:
        if line.startswith('#'):
            continue
        if not in_data:
            if '\t' in line and 'site_no' in line:
                headers = line.split('\t')
                in_data = True
            continue
        # Skip format line
        if all(part.strip().endswith(('s', 'd', 'n')) for part in line.split('\t') if part.strip()):
            continue
        if in_data and line.strip():
            parts = line.split('\t')
            if len(parts) == len(headers):
                data.append(dict(zip(headers, [p.strip() for p in parts])))
    return data

def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

@app.route('/')
def index():
    return render_template('index.html', counties=counties)

@app.route('/locations')
def locations():
    county = request.args.get('county')
    if not county or county not in county_to_fips:
        return "Invalid county", 400
    fips = county_to_fips[county]
    url = f"https://waterservices.usgs.gov/nwis/site/?format=rdb&countyCd={fips}&outputDataTypeCd=all&siteStatus=all"
    resp = requests.get(url)
    sites = parse_rdb(resp.text)
    
    unique_sites = {}
    for site in sites:
        site_no = site.get('site_no')
        if site_no and site_no not in unique_sites:
            unique_sites[site_no] = site
    
    site_nos_list = list(unique_sites.keys())
    has_data_set = set()
    batch_size = 100  # USGS allows up to 100 sites per request
    for batch in chunk_list(site_nos_list, batch_size):
        if not batch:
            continue
        site_nos = ','.join(batch)
        iv_url = f"https://waterservices.usgs.gov/nwis/iv/?format=json&sites={site_nos}&period=P1D&siteStatus=all"
        iv_resp = requests.get(iv_url)
        if iv_resp.status_code != 200:
            continue
        try:
            iv_data = iv_resp.json()
        except requests.exceptions.JSONDecodeError:
            continue
        time_series = iv_data.get('value', {}).get('timeSeries', [])
        for ts in time_series:
            site_no = ts['sourceInfo']['siteCode'][0]['value']
            values = ts['values'][0]['value']
            if values and any(float(v['value']) != -999999 for v in values):
                has_data_set.add(site_no)
    
    sites_with_status = []
    for site in unique_sites.values():
        site_no = site['site_no']
        has_data = site_no in has_data_set
        sites_with_status.append({
            'station_nm': site['station_nm'],
            'site_no': site_no,
            'has_data': has_data
        })
    
    # Filter to only stations with names that include words (contain at least one space)
    filtered_sites = [s for s in sites_with_status if ' ' in s['station_nm']]
    
    # Sort: sites with data first
    filtered_sites.sort(key=lambda x: not x['has_data'])
    
    # Pagination
    per_page = 20
    page = request.args.get('page', 1, type=int)
    total = len(filtered_sites)
    total_pages = math.ceil(total / per_page)
    start = (page - 1) * per_page
    end = start + per_page
    sites_page = filtered_sites[start:end]
    
    return render_template('page1.html', county=county, sites=sites_page, page=page, total_pages=total_pages)

@app.route('/data')
def data():
    site_no = request.args.get('site')
    county = request.args.get('county')
    if not site_no:
        return "No site selected", 400

    # Get site info
    site_url = f"https://waterservices.usgs.gov/nwis/site/?format=rdb&sites={site_no}"
    site_resp = requests.get(site_url)
    sites = parse_rdb(site_resp.text)
    site_info = sites[0] if sites else {}
    station_name = site_info.get('station_nm', 'Unknown Station')

    # Get real-time data
    iv_url = f"https://waterservices.usgs.gov/nwis/iv/?format=json&sites={site_no}&period=P7D"
    resp = requests.get(iv_url)
    if resp.status_code != 200:
        time_series = []
    else:
        try:
            json_data = resp.json()
            time_series = json_data.get('value', {}).get('timeSeries', [])
        except requests.exceptions.JSONDecodeError:
            time_series = []

    parameters = []
    for ts in time_series:
        name = ts['variable']['variableDescription']
        code = ts['variable']['variableCode'][0]['value']
        unit = ts['variable']['unit']['unitCode']
        values = ts['values'][0]['value']
        if not values:
            continue
        latest = values[-1]
        value = latest['value']
        if float(value) == -999999.0:
            continue
        dt = parser.isoparse(latest['dateTime'])
        timestamp = dt.strftime('%b %d, %Y - %I:%M %p %Z')
        qualifiers = latest.get('qualifiers', [])
        status = 'Provisional' if any(q.startswith('P') for q in qualifiers) else 'Approved'

        parameters.append({
            'parameter': name,
            'code': code,
            'value': value,
            'unit': unit,
            'timestamp': timestamp,
            'status': status
        })

    return render_template(
        'page2.html',
        site_no=site_no,
        station_name=station_name,
        county=county,
        parameters=parameters
    )

if __name__ == '__main__':
    app.run(debug=True)