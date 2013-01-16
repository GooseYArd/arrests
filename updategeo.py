#!bin/python
import models
import time
import dbm
from sqlalchemy import not_
from models import Address, Geocoding

session = models.get_session()

q = session.query(Address).filter(~Address.geocoding.any())
#q = session.query(Address).filter(Address.geocoding.any())

for i in q:
    print i


# addr = models.get_or_create(session,
#                             models.Address,
#                             address=addrtxt)
# geo = models.get_or_create(session,
#                            models.Geocoding,
#                            address_id=addr.id,
#                            latitude=lat, 
#                            longitude=lon, 
#                            error=0)
# session.commit()
