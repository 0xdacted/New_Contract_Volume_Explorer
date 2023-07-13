from sqlalchemy import Column, String, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class CurrentToken(Base):
    __tablename__ = "current_tokens"

    contract_address = Column(String, primary_key=True)
    first_seen = Column(DateTime)
    volume = Column(Float)

class OldToken(Base):
    __tablename__ = "old_tokens"

    contract_address = Column(String, primary_key=True)
    first_seen = Column(DateTime)
    volume = Column(Float)
