#!bin/python
import re
import sys
import urllib2
import datetime
import time
import os
import md5
import argparse
import ConfigParser, os
import logging
import models
import geoutil

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('arrests')
logger.setLevel(logging.DEBUG)
sh = logging.StreamHandler(stream=sys.stderr)
sh.setLevel(logging.DEBUG)
sh.setFormatter(formatter)
logger.addHandler(sh)

for i in ['geoutil']:
  l = logging.getLogger(i)
  l.setLevel(logging.DEBUG)
  l.addHandler(sh)

etagcache = '.arrests.dat'
geocode_lookups = 0
cached_lookups = 0

LNAME = 0
FNAME = 1
MNAME = 2
AGE = 3
DATE = 4
CHARGE = 5
DESCRIP = 6
ADDRESS = 7

def get_etag(url):
  request = urllib2.Request(url)
  request.get_method = lambda : 'HEAD'
  response = urllib2.urlopen(request)
  return response.headers.getheaders('etag')[0]

def write_lastetag(etag):
  cachefh = open(etagcache, 'wb')
  cachefh.write(etag)
  cachefh.close()

def read_lastetag():
  lasttag = None
  if os.path.exists(etagcache):
    cachefh = open(etagcache, 'rb')
    lasttag = cachefh.readline()
    cachefh.close()
  return lasttag

def geturl_tofile(url, fn):
  r = urllib2.urlopen(url)
  rescache = open(".arrests.cache.tmp", "wb")
  rescache.write(r.read())
  rescache.close()

def geturl_cached(url):
  lasttag = read_lastetag()
  curtag = get_etag(url)

  if curtag != lasttag:
    logger.debug("need to refresh arrests data")
    geturl_tofile(url, '.arrests.cache.tmp')
    write_lastetag(curtag)
    os.rename('.arrests.cache.tmp', '.arrests.cache')
  else:
    logger.debug("cache still fresh, reusing")

  rescache = open(".arrests.cache", "rb")
  return rescache

def main():

  parser = argparse.ArgumentParser()
  parser.add_argument("--geocode", "-g", action="store_true", help="geocode arrestee address", default=False)
  parser.add_argument("--sort", "-s", help="geocode arrestee address", default='date')
  parser.add_argument("--configuration", "-c", help="use valued from configuration file FILE", default="arrests.cfg")
  parser.add_argument("--home", help="specify origin address for arrestee residence distance")
  parser.add_argument("--latitude", type=float, help="specify origin lat/long for arrestee residence distance")
  parser.add_argument("--longitude", type=float, help="specify origin lat/long for arrestee residence distance")
  parser.add_argument("--limit", type=int, help="limit processing to first n arrests, useful for limitig geocode lookups", default=25000)
  parser.add_argument("--api_key", help="use googlemaps api key KEY")

  args = parser.parse_args()

  logger.info("loading config")
  config = ConfigParser.ConfigParser()
  config.readfp(open('arrests.cfg'))
#  config.read(['site.cfg', os.path.expanduser('~/.myapp.cfg')])

  logger.info("fetching arrest data")
  url = "http://www.fairfaxcounty.gov/police/crime/arrest.txt"
  r = geturl_cached(url)
  headers = r.readline()
  
  api_key = config.get('googlemaps', 'api_key')
  if args.api_key:
    api_key = args.api_key

  widths = [40, 20, 40, 5, 30, 25, 50, 100]
  offsets = [0]
  for i in widths:
    offsets.append(offsets[-1] + i)

  arrests = []
  count = 0

  logger.debug("limiting to %d records" % args.limit)

  session = models.get_session()

  new_arrests = 0

  while count < args.limit:
    line = r.readline()
    if len(line) == 0:
      break

    count += 1   
    f = []
    arrest = {}
    offset = 0
    for i in widths:
      f.append(line[offset:offset+i].strip())
      offset += i

    (is_new, charge) = models.get_or_create(session,
                                            models.Charge, 
                                            name=f[CHARGE], 
                                            description=f[DESCRIP])
      
    (is_new, address) = models.get_or_create(session,
                                             models.Address,
                                             address=f[ADDRESS])

    # geo_api_error = 1
    # if not models.have_geocoding(session, address):
    #   raise Exception('whoops missed an address')
    #   try:
    #     (lat, lon) = geoutil.get_coord(address.address, False)
    #     geo_api_error = 0
    #   except geoutil.InvalidAddress, x:
    #     lat=0.0
    #     lon=0.0

    #    geo = models.add_geocoding(session,
    #                              address=address,
    #                              latitude=lat, 
    #                              longitude=lon, 
    #                              error=geo_api_error)        
      
    (is_new, arrestee) = models.get_or_create(session,
                                              models.Arrestee,
                                              lname = f[LNAME],
                                              fname = f[FNAME],
                                              mname = f[MNAME],
                                              age = f[AGE],
                                              address_id = address.id)
 
    date = time.strftime("%s", time.strptime(f[DATE], '%m/%d/%Y'))
    
    (is_new, arrest) = models.get_or_create(session,
                                            models.Arrest,
                                            date=date, 
                                            charge=charge, 
                                            arrestee=arrestee)
    if is_new:
      new_arrests += 1
      session.add(arrest)
    
  session.commit()
  print "Found %d new arrest records" % new_arrests
  
#    if 1:

      # arrest['name'] = "'%s, %s %s'" % (ucfwords(f[LNAME]),
      #                                   ucfwords(f[FNAME]),
      #                                   ucfwords(f[MNAME]))
      # arrest['date_str'] = f[DATE]
      # try:
      #   arrest['date'] = time.strptime(f[DATE], '%m/%d/%Y')
      # except Exception, x:
      #   logger.error("Couldn't parse date in line %s" % line)
      #   next

      # arrest['chargedesc'] = "'" + ucfirst(f[DESCRIP]).strip() + "'"
      # arrest['dist'] = get_dist(f, args.geocode)
      # m = md5.new()
      # m.update(line)
      # arrest['md5'] = m.digest()      
      # arrests.append(arrest)

#    except Exception, x:
#      print("problem converting a record: %s:%s" % (x, line))
  
#  fmt = "{:<4.2f} {:<10} {:<40} {:<40}"
  
#  sortkey = lambda x: x[args.sort]
#  for r in sorted(arrests, key = sortkey):
#    print fmt.format(r['dist'], r['date_str'], r['name'], r['chargedesc'])

#  print "Would need %d/%d lookups" % (geocode_lookups, cached_lookups)
1
if __name__ == "__main__":
  main()
