from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy import create_engine
from sqlalchemy import Sequence
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, backref

import dbm
import sys

Base = declarative_base()

@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()
  
class Address(Base):
    __tablename__ = 'addresses'
    id = Column(Integer, Sequence('address_id_seq'), primary_key=True)
    latitude = Column(String)
    longitude = Column(String)
    address = Column(String, nullable=False, unique=True)
    arrestee = relationship("Arrestee", backref=backref('arrestees', order_by=id))

    def __init__(self, address, latitude=None, longitude=None):
        self.address = address
        self.latitude = latitude
        self.longitude = longitude

    def __repr__(self):
        return "<Address('%s')>" % self.address

class Arrestee(Base):
  __tablename__ = 'arrestees'

  id = Column(Integer, Sequence('arrestee_id_seq'), primary_key=True)
  fname = Column(String(50))
  lname = Column(String(50))
  mname = Column(String(50))
  birthyear = Column(Integer)
  address = Column(Integer, ForeignKey('addresses.id'))

  def __repr__(self):
      return "<Arrestee('%s','%s','%s')>" % (self.lname, self.fname, self.mname)

class Charge(Base):
    __tablename__ = 'charges'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(String, nullable=False) 
    arrests = relationship("Arrest", backref=backref('arrests', order_by=id))

    def __repr__(self):
        return "<Charge('%d', '%s','%s')>" % (self.id, self.name, self.description)

class Arrest(Base):
  __tablename__ = 'arrests'
  id = Column(Integer, Sequence('arrest_id_seq'), primary_key=True)
  date = Column(Integer)
  arrestee = Column(Integer, ForeignKey('arrestees.id'))
  charge = Column(Integer, ForeignKey('charges.id'))

  def __repr__(self):
      return "<Arrest('%s','%s','%s')>" % (self.arrestee, 
                                           self.date, 
                                           self.charge)

class Geocoding(Base):
    __tablename__ = 'geocoding'
    id = Column(Integer, Sequence('arrest_id_seq'), primary_key=True)
    address = Column(Integer, ForeignKey('addresses.id'))
    error = Column(Integer, nullable=False)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    
def get_session():
    engine = create_engine('sqlite:///arrests.db', echo=False)
    Base.metadata.create_all(engine) 
    Session = sessionmaker(bind=engine)
    return Session()

def get_or_add_charge(session, name, description):

    if session.query(Charge).filter_by(name=name).count() > 0:
        charge = session.query(Charge).filter_by(name=name, description=description).one()
    else:
        charge = Charge(name=name, description=description)
        session.add(charge)
        session.flush()

    return charge

def get_or_add_geocoding(session, address, latitude, longitude, error):
    if session.query(Geocoding).filter_by(address=address.id).count() > 0:
        geocoding = session.query(Geocoding).filter_by(address=address.id).one()
    else:
        geocoding = Geocoding(address=address.id, 
                              latitude=latitude,
                              longitude=longitude,
                              error=error)
        session.add(geocoding)
        session.flush()

    return geocoding

def get_or_add_address(session, addrtxt):
    if session.query(Address).filter_by(address=addrtxt).count() > 0:
        address = session.query(Address).filter_by(address=addrtxt).one()
    else:
        address = Address(addrtxt)
        session.add(address)
        session.flush()
    return address
 
def get_or_add_arrestee(session, lname, fname, mname, address, age):
    if session.query(Arrestee).filter_by(lname=lname,mname=mname,fname=fname).count() > 0:
        arrestee = session.query(Arrestee).filter_by(lname=lname,mname=mname,fname=fname).one()
    else:
        arrestee = Arrestee(fname=fname,mname=mname,lname=lname,address=address.id)
        session.add(arrestee)
        session.flush()

    return arrestee

if __name__ == "__main__":
  
    session = get_session()
    
    for address in addrdb.keys():
        (lat,lon) = addrdb[address].split(":")
        address = Address(address, lat, lon)
        session.add(address)

    session.commit()
    sys.exit(0)

    addrtxt = '13949 Valley Country Dr, Chantilly VA 20151'
    charge = '1.2.3'
    charge_descrip = 'lewd pelvic thrusting'
    fname = "Raymond"
    mname = "Andrew"
    lname = "Bailey"
    age = 38

    if session.query(Arrestee).filter_by(lname=lname,mname=mname,fname=fname).count() > 0:
        arrestee = session.query(Arrestee).filter_by(lname=lname,mname=mname,fname=fname).one()
    else:
        arrestee = Arrestee(fname=fname,mname=mname,lname=lname,address=address.id)
        session.add(arrestee)

    session.commit()

#  if not address:
#  myaddr = Address()
#  session.add(myaddr)
#  session.commit()
#  me = Arrestee(
#  post = BlogPost("Wendy's Blog Post", "This is a test", wendy)
#  session.add(post)



