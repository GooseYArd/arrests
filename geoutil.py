import googlemaps
import logging

from pygeocoder import Geocoder
from math import *

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('arrests')
logger.setLevel(logging.DEBUG)
sh = logging.StreamHandler(stream=sys.stderr)
sh.setLevel(logging.DEBUG)
sh.setFormatter(formatter                )
logger.addHandler(sh)

addrdb = dbm.open('addr.db', 'c')
baddrdb = dbm.open('baddr.db', 'c')
logger.debug("address database contains %d entries" % len(addrdb))
api_key = None
last_api_req = datetime.datetime.now()

homecoord = [0,0]

def calcDist(A, B):
  distance = (sin(radians(A[0])) *
              sin(radians(B[0])) +
              cos(radians(A[0])) *
              cos(radians(B[0])) *
              cos(radians(A[1] - B[1])))
  distance = (degrees(acos(distance))) * 69.09
  return distance

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
