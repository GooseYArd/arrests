import googlemaps
import logging
import sys
import dbm
import datetime
import time
from math import sin, cos, radians, degrees, acos

logger = logging.getLogger('geocoding')
addrdb = dbm.open('addr.db', 'c')
baddrdb = dbm.open('baddr.db', 'c')
logger.debug("address database contains %d entries" % len(addrdb))
global api_key 

last_api_req = datetime.datetime.now()

def calcDist(A, B):
  distance = (sin(radians(A[0])) *
              sin(radians(B[0])) +
              cos(radians(A[0])) *
              cos(radians(B[0])) *
              cos(radians(A[1] - B[1])))
  distance = (degrees(acos(distance))) * 69.09
  return distance

class InvalidAddress(Exception): pass
class TooManyRequests(Exception): pass

def api_wait():
  global last_api_req
  min_d = datetime.timedelta(milliseconds=1010)
  now = datetime.datetime.now()
  age = now - last_api_req
  if age < min_d:
    n = min_d - age
    logger.debug("must sleep for %f seconds" % n.total_seconds())
    time.sleep(n.total_seconds())
  last_api_req = now

def get_coord(address, api_key, noop=True):
  min_d = datetime.timedelta(milliseconds=1005)
  gmaps = googlemaps.GoogleMaps(api_key)
  global last_api_req

  try:
    api_wait()
    targetcoord = gmaps.address_to_latlng(address)
  except googlemaps.GoogleMapsError, x:
    last_api_req = datetime.datetime.now()
    logger.debug("do_geocoding: exception: %s" % x.message)
    if str(x.message) == "602":
      logger.debug("got a 602, tagging this as a bad address")
      raise InvalidAddress()
    elif str(x.message) == "620":
      raise TooManyRequests()
  
  return targetcoord
