from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class AnggaranMaintenance(Base):
    __tablename__ = "anggaran_maintenance"
    id = Column(Integer, primary_key=True, index=True)
    ru = Column(String(20))          # RU II, RU III, ..., All RUs
    tahun = Column(Integer)          # 2018 - 2025
    kategori = Column(String(50))    # RUTIN, NON RUTIN, TURN AROUND, OVERHAUL, TOTAL
    tipe = Column(String(10))        # RKAP, PLAN, AKTUAL
    nilai_usd = Column(Float)        # Nilai dalam USD