#!bin/python
import models
import time
import dbm

session = models.get_session()
addrdb = dbm.open('addr.db', 'c')
baddrdb = dbm.open('baddr.db', 'c')

for addrtxt in addrdb.keys():
    (lat, lon) = addrdb[addrtxt].split(":")
    addr = models.get_or_create(session,
                                models.Address,
                                address=addrtxt)
    geo = models.get_or_create(session,
                               models.Geocoding,
                               address_id=addr.id,
                               latitude=lat, 
                               longitude=lon, 
                               error=0)

for addrtxt in baddrdb.keys():
    addr = models.get_or_create(session,
                                models.Address,
                                address=addrtxt)
    geo = models.get_or_create(session,
                               models.Geocoding,
                               address_id=addr.id,
                               latitude=0.0, 
                               longitude=0.0, 
                               error=1)

session.commit()
