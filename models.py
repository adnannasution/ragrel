from sqlalchemy import Column, Integer, String, Float, Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class KPIData(Base):
    __tablename__ = "kpi_maintenance"
    id = Column(Integer, primary_key=True, index=True)
    id_kpi = Column(String(50))
    ru = Column(String(100))
    area = Column(String(100))
    kategori = Column(String(100))
    nama_kpi = Column(String(255))
    deskripsi = Column(Text)
    satuan = Column(String(20))
    target = Column(Float)
    realisasi = Column(Float)
    pencapaian = Column(String(50))
    status = Column(String(50))
    analisis = Column(Text)
    rekomendasi = Column(Text)
