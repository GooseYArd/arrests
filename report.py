import models
from datetime import datetime
import time
from models import Arrest, Charge, Arrestee, Address, Geocoding
import geoutil
import argparse
import ConfigParser


def ucfirst(s):
  if len(s):
    return s[0].upper() + s[1:].lower()
  return ""

def ucfwords(s):
  sep = " "
  return sep.join([ ucfirst(x) for x in s.split(" ") ])

def rfc822date(ts):
  return time.strptime("%a, %d %b %Y %H:%M:%S GMT", ts)


parser = argparse.ArgumentParser()
parser.add_argument("--sort", "-s", help="geocode arrestee address", default='date')
parser.add_argument("--configuration", "-c", help="use valued from configuration file FILE", default="arrests.cfg")
parser.add_argument("--home", help="specify origin address for arrestee residence distance")
parser.add_argument("--latitude", type=float, help="specify origin lat/long for arrestee residence distance")
parser.add_argument("--longitude", type=float, help="specify origin lat/long for arrestee residence distance")
parser.add_argument("--limit", type=int, help="limit processing to first n arrests, useful for limitig geocode lookups", default=25000)
args = parser.parse_args()

config = ConfigParser.ConfigParser()
config.readfp(open('arrests.cfg'))
#  config.read(['site.cfg', os.path.expanduser('~/.myapp.cfg')])


home = (config.getfloat('home', 'latitude'),
        config.getfloat('home', 'longitude'))

if args.home:
  home = get_coord(api_key=api_key, address=args.home)

if args.latitude:
  home = (args.latitude, args.longitude)

session = models.get_session()
q = session.query(Arrest, Charge, Arrestee, Address, Geocoding)
q = q.filter(Charge.id==Arrest.charge_id)
q = q.filter(Arrestee.id==Arrest.arrestee_id)
q = q.filter(Address.id==Arrestee.address_id)
q = q.filter(Geocoding.id==Arrestee.address_id)

fmt = '{:<6.1f} {:<10} {:<20} {:<40} {:<40}'

for r in q.all():
  sep = ", "
  print fmt.format(
    geoutil.calcDist(home, (r.Geocoding.latitude, r.Geocoding.longitude)),
    datetime.fromtimestamp(r.Arrest.date).strftime("%m/%d/%Y"),
    sep.join((ucfirst(r.Arrestee.lname), ucfirst(r.Arrestee.fname))),
    ucfirst(r.Charge.description)[:40],
    ucfwords(r.Address.address),
    )




