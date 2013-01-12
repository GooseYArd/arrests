#!bin/python
import re
import sys
import urllib2
import struct
import datetime
import time
import pickle
import os
import dbm
import md5

from pygeocoder import Geocoder
from math import *
import argparse
import ConfigParser, os
import googlemaps
import logging
import models

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('arrests')
logger.setLevel(logging.DEBUG)
sh = logging.StreamHandler(stream=sys.stderr)
sh.setLevel(logging.DEBUG)
sh.setFormatter(formatter                )
logger.addHandler(sh)

etagcache = '.arrests.dat'
geocode_lookups = 0
cached_lookups = 0
addrdb = dbm.open('addr.db', 'c')
baddrdb = dbm.open('baddr.db', 'c')
logger.debug("address database contains %d entries" % len(addrdb))
api_key = None
last_api_req = datetime.datetime.now()

homecoord = [0,0]

LNAME = 0
FNAME = 1
MNAME = 2
AGE = 3
DATE = 4
CHARGE = 5
DESCRIP = 6
ADDRESS = 7

def calcDist(A, B):
  distance = (sin(radians(A[0])) *
              sin(radians(B[0])) +
              cos(radians(A[0])) *
              cos(radians(B[0])) *
              cos(radians(A[1] - B[1])))
  distance = (degrees(acos(distance))) * 69.09
  return distance

def rfc822date(ts):
  print ts
  return time.strptime("%a, %d %b %Y %H:%M:%S GMT", ts)

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


def get_coord(rec, do_geocoding = False):

  global api_key
  global geocode_lookups
  global cached_lookups
  global addrdb
  global homecoord
  global last_api_req

  address = rec[7]

  if addrdb.has_key(address):
    logger.debug("address found in cache")
    cached_lookups += 1
    coord = [ float(x) for x in addrdb[address].split(":") ]
    return coord
  
  logger.debug('address not found in cache')
  geocode_lookups += 1
  targetcoord = (0.0, 0.0)
  min_d = datetime.timedelta(milliseconds=1005)

  if baddrdb.has_key(address):
    logger.debug("known bad address")
    return targetcoord

  if do_geocoding:    
    gmaps = googlemaps.GoogleMaps(api_key)
    attempt = 0
    while attempt < 3:
      logger.debug('making attempt %d for %s' % (attempt, address))    
      try:
        now = datetime.datetime.now()
        age = now - last_api_req
        if age < min_d:
          n = min_d - age
          logger.debug("must sleep for %f seconds" % n.total_seconds())
          time.sleep(n.total_seconds())
        targetcoord = gmaps.address_to_latlng(address)
        last_api_req = now
        addrdb[address] = "%s:%s" % (targetcoord[0], targetcoord[1])
        break
      except googlemaps.GoogleMapsError, x:
        last_api_req = now
        logger.debug("do_geocoding: exception: %s" % x.message)
        if str(x.message) == "602":
          logger.debug("got a 602, tagging this as a bad address")
          baddrdb[address] = 'true'
          return targetcoord
        elif str(x.message) == "620":
          attempt += 1
          if attempt == 3:
            raise x
          else:
            logger.debug("waiting 10 seconds to retry request")
            time.sleep(10)
  
  return targetcoord

def get_dist(rec, do_geocoding=False):
  targetcoord = get_coord(rec, do_geocoding)
  return calcDist(homecoord, targetcoord)

def ucfirst(s):
  if len(s):
    return s[0].upper() + s[1:].lower()
  return ""

def ucfwords(s):
  sep = " "
  return sep.join([ ucfirst(x) for x in s.split(" ") ])


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
  
  global geocode_lookups
  global cached_lookups
  global homecoord
  global api_key

  api_key = config.get('home', 'api_key')
  if args.api_key:
    api_key = args.api_key

  if args.geocode and not api_key:
    logger.error("Can't do geocoding withhout a googlemaps api_key")
    sys.exit(1)


  if args.home:
      homecoord = get_coord(args.home)
  else:
    homecoord[0] = config.getfloat('home', 'latitude')
    homecoord[1] = config.getfloat('home', 'longitude')
    if args.latitude:
      homecoord = [args.latitude, args.longitude]



  widths = [40, 20, 40, 5, 30, 25, 50, 100]
  offsets = [0]
  for i in widths:
    offsets.append(offsets[-1] + i)

  arrests = []
  count = 0

  logger.debug("limiting to %d records" % args.limit)

  session = models.get_session()
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

    charge = models.get_or_add_charge(session, f[CHARGE], f[DESCRIP])
    arrestee = models.get_or_add_arrestee(session, f[LNAME], f[FNAME], f[MNAME], f[ADDRESS], f[AGE])
    date = time.strftime("%s", time.strptime(f[DATE], '%m/%d/%Y'))
    
    kwargs = { 'date' : date,
               'charge' : charge.id,
               'arrestee' : arrestee.id }
    
    arrest = models.Arrest(**kwargs)
    session.add(arrest)

  session.commit()

#    try:
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
