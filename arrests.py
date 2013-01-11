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

etagcache = '.arrests.dat'
geocode_lookups = 0
cached_lookups = 0
addrdb = dbm.open('addr.db', 'c')

homecoord = [0,0]

LNAME = 0
FNAME = 1
MNAME = 2
DATE = 4
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
    print "need to refresh arrests data"
    geturl_tofile(url, '.arrests.cache.tmp')
    write_lastetag(curtag)
    os.rename('.arrests.cache.tmp', '.arrests.cache')

  rescache = open(".arrests.cache", "rb")
  return rescache

def get_coord(rec, do_geocoding = False):

  global geocode_lookups
  global cached_lookups
  global addrdb
  global homecoord

  address = rec[7]

  if addrdb.has_key(address):
    cached_lookups += 1
    coord = [ float(x) for x in addrdb[address].split(":") ]
    return coord

  geocode_lookups += 1
  targetcoord = homecoord

  if do_geocoding:
    try:
      r = Geocoder.geocode(address)
      targetcoord = (results[0].coordinates[0],
                     results[0].coordinates[1])
    except Exception, x:
      pass
  
  addrdb[address] = "%s:%s" % targetcoord
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
  args = parser.parse_args()

  config = ConfigParser.ConfigParser()
  config.readfp(open('arrests.cfg'))
#  config.read(['site.cfg', os.path.expanduser('~/.myapp.cfg')])

  url = "http://www.fairfaxcounty.gov/police/crime/arrest.txt"
  r = geturl_cached(url)
  headers = r.readline()
  
  global geocode_lookups
  global cached_lookups
  global homecoord

  widths = [40, 20, 40, 5, 30, 25, 50, 100]
  offsets = [0]
  for i in widths:
    offsets.append(offsets[-1] + i)

  if args.home:
      homecoord = get_coord(args.home)
  else:
    homecoord[0] = config.getfloat('home', 'latitude')
    homecoord[1] = config.getfloat('home', 'longitude')
    if args.latitude:
      homecoord = [args.latitude, args.longitude]

  arrests = []
  for line in r:
    f = []
    arrest = {}
    offset = 0
    for i in widths:
      f.append(line[offset:offset+i].strip())
      offset += i

#    try:
    if 1:
      arrest['name'] = "'%s, %s %s'" % (ucfwords(f[LNAME]),
                                        ucfwords(f[FNAME]),
                                        ucfwords(f[MNAME]))
      arrest['date_str'] = f[DATE]
      arrest['date'] = time.strptime(f[DATE], '%m/%d/%Y')
      arrest['chargedesc'] = "'" + ucfirst(f[DESCRIP]).strip() + "'"
      arrest['dist'] = get_dist(f, args.geocode)
      m = md5.new()
      m.update(line)
      arrest['md5'] = m.digest()
      
      arrests.append(arrest)

#    except Exception, x:
#      print("problem converting a record: %s:%s" % (x, line))
#      raise x
#      sys.exit(1)

  fmt = "{:<4.2f} {:<10} {:<40} {:<40}"
  
  sortkey = lambda x: x[args.sort]
  for r in sorted(arrests, key = sortkey):
    print fmt.format(r['dist'], r['date_str'], r['name'], r['chargedesc'])

  print "Would need %d/%d lookups" % (geocode_lookups, cached_lookups)

if __name__ == "__main__":
  main()
