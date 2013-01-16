from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy import create_engine
from sqlalchemy import Sequence
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship, backref

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
    address = Column(String, nullable=False, unique=True)

    #arrestee = relationship("Arrestee", backref=backref('arrestees', order_by=id))
    geocoding = relationship("Geocoding", backref=backref('geocoding', order_by=id))

class Arrestee(Base):
  __tablename__ = 'arrestees'

  id = Column(Integer, Sequence('arrestee_id_seq'), primary_key=True)
  fname = Column(String(50))
  lname = Column(String(50))
  mname = Column(String(50))
  age = Column(Integer)
  address_id = Column(Integer, ForeignKey('addresses.id'))

  #address = relationship("Address", backref=backref('addresses', order_by=id))

class Charge(Base):
    __tablename__ = 'charges'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(String, nullable=False) 
#    arrests = relationship("Arrest", backref=backref('arrests', order_by=id))

    def __repr__(self):
        return "<Charge('%d', '%s','%s')>" % (self.id, self.name, self.description)

class Arrest(Base):
  __tablename__ = 'arrests'
  id = Column(Integer, Sequence('arrest_id_seq'), primary_key=True)
  date = Column(Integer)
  charge_id = Column(Integer, ForeignKey('charges.id'))
  charge = relationship("Charge", backref=backref('charges', order_by=id))  
  arrestee_id = Column(Integer, ForeignKey('arrestees.id'))
  arrestee = relationship("Arrestee", backref=backref("arrestees", order_by=id))
 
class Geocoding(Base):
    __tablename__ = 'geocodings'
    id = Column(Integer, 
                Sequence('geocoding_id_seq'), 
                primary_key=True)

    address_id = Column(Integer, ForeignKey('addresses.id'))

    #address = relationship("Address", backref=backref('addresses', order_by=id))

    error = Column(Integer, nullable=False)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)

def get_session():
    engine = create_engine('sqlite:///arrests.db', echo=False)
    Base.metadata.create_all(engine) 
    Session = sessionmaker(bind=engine)
    return Session()

def get_or_create(session, model, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return (False, instance)
    else:
        instance = model(**kwargs)
        session.add(instance)
        session.flush()
        return (True, instance)

