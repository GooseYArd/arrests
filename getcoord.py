import models
from models import Address, Geocoding

address = '2548 SYLVAN MOOR LN, WOODBRIDGE, VA'
session = models.get_session()
addrid = session.query(Address).filter(Address.address==address).one().id

q = session.query(Geocoding)
q = q.filter(Geocoding.address_id == addrid).one()

print q

#for r in q.all():
#    print r

