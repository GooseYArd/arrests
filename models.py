from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine
from sqlalchemy import Sequence
from sqlalchemy.orm import sessionmaker
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, backref

import dbm
import sys

Base = declarative_base()
  
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

  def __init__(self, lname, fname, mname, address):
    self.lname = lname
    self.fname = fname
    self.mname = mname
    self.address = address

  def __repr__(self):
      return "<Arrestee('%s','%s','%s')>" % (self.lname, self.fname, self.mname)


class Charge(Base):
    __tablename__ = 'charges'
    id = Column(Integer, Sequence('charge_id_seq'), primary_key=True)

    charge = Column(String, nullable=False)
    charge_descrip = Column(String, nullable=False)

    arrests = relationship("Arrest", backref=backref('arrests', order_by=id))

    def __init__(self, charge, charge_descrip):
        self.charge = charge
        self.charge_descrip = charge_descrip

    def __repr__(self):
        return "<Charge('%s','%s')>" % (self.charge, self.charge_descrip)


class Arrest(Base):
  __tablename__ = 'arrests'
  id = Column(Integer, Sequence('arrest_id_seq'), primary_key=True)
  date = Column(Integer)
  arrestee = Column(Integer, ForeignKey('arrestees.id'))
  charge = Column(Integer, ForeignKey('charges.id'))

  def __init__(self, date, arrestee, charge):
    self.date = date
    self.arrestee = arrestee
    self.charge = charge
    print "IN THE CONSTRUCTOR: %s" % self

  def __repr__(self):
    return "<Arrestee('%s','%s','%s')>" % (self.arrestee, self.date, self.charge)

def get_session():
    engine = create_engine('sqlite:///arrests.db', echo=False)
    Base.metadata.create_all(engine) 
    Session = sessionmaker(bind=engine)
    return Session()

def get_or_add_charge(session, charge, charge_descrip):
    if session.query(Charge).filter_by(charge=charge).count() > 0:
        charge = session.query(Charge).filter_by(charge=charge).one()
    else:
        charge = Charge(charge, charge_descrip)
        session.add(charge)
    return charge
 
def get_or_add_arrestee(session, lname,fname,mname,addrtxt,age):

    if session.query(Address).filter_by(address=addrtxt).count() > 0:
        address = session.query(Address).filter_by(address=addrtxt).one()
    else:
        address = Address(addrtxt)
        session.add(address)

    if session.query(Arrestee).filter_by(lname=lname,mname=mname,fname=fname).count() > 0:
        arrestee = session.query(Arrestee).filter_by(lname=lname,mname=mname,fname=fname).one()
    else:
        arrestee = Arrestee(fname=fname,mname=mname,lname=lname,address=address.id)
        session.add(arrestee)

    return arrestee

if __name__ == "__main__":
  
    session = get_session()
    
    addrdb = dbm.open('addr.db', 'c')
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



