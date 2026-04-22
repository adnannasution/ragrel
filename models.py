from sqlalchemy import Column, Integer, String, Float, Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# ─── 1. REKAP ANGGARAN MAINTENANCE (tidak diubah) ────────────────────────────
class AnggaranMaintenance(Base):
    __tablename__ = "anggaran_maintenance"
    id        = Column(Integer, primary_key=True, index=True)
    ru        = Column(String(20))
    tahun     = Column(Integer)
    kategori  = Column(String(50))
    tipe      = Column(String(10))
    nilai_usd = Column(Float)

# ─── 2. PIPELINE INSPECTION ──────────────────────────────────────────────────
class PipelineInspection(Base):
    __tablename__ = "pipeline_inspection"
    id                      = Column(Integer, primary_key=True, index=True)
    refinery_unit           = Column(String(100))
    area                    = Column(String(100))
    unit                    = Column(String(100))
    tag_number              = Column(String(255))
    last_inspection_date    = Column(String(50))
    next_inspection_date    = Column(String(50))
    fluida_service          = Column(String(100))
    nps                     = Column(String(20))
    from_location           = Column(String(255))
    to_location             = Column(String(255))
    last_measured_thickness = Column(Float)
    rem_life_years          = Column(Float)
    jumlah_temporary_repair = Column(Integer)
    remarks                 = Column(Text)
    bulan                   = Column(String(20))
    tahun                   = Column(Integer)

# ─── 3. ROTOR MONITORING ─────────────────────────────────────────────────────
class RotorMonitoring(Base):
    __tablename__ = "rotor_monitoring"
    id                     = Column(Integer, primary_key=True, index=True)
    no                     = Column(Integer)
    refinery_unit          = Column(String(100))
    bulan                  = Column(String(20))
    rotor                  = Column(String(100))
    program                = Column(String(100))
    brand                  = Column(String(100))
    status_readiness_spare = Column(String(100))
    status_workplan        = Column(String(100))
    detail_status_workplan = Column(Text)
    keterangan             = Column(Text)
    action_plan_category   = Column(String(100))
    external_resource      = Column(String(100))
    no_irkap               = Column(String(100))
    finish_date_eksekusi   = Column(String(50))
    readiness_rotor        = Column(Integer)
    last_update            = Column(String(50))

# ─── 4. ATG MONITORING ───────────────────────────────────────────────────────
class ATGMonitoring(Base):
    __tablename__ = "atg_monitoring"
    id                      = Column(Integer, primary_key=True, index=True)
    refinery_unit           = Column(String(100))
    tag_no_tangki           = Column(String(100))
    tag_no_atg              = Column(String(100))
    status_atg              = Column(String(100))
    status_interkoneksi_atg = Column(String(100))
    cert_no_atg             = Column(String(255))
    date_expired_atg        = Column(String(50))
    remark                  = Column(Text)
    rtl                     = Column(String(50))
    action_plan_category    = Column(String(100))
    external_resource       = Column(String(100))
    no_irkap                = Column(String(100))
    status_rtl              = Column(String(50))
    month_update            = Column(String(50))

# ─── 5a. BAD ACTOR MONITORING ────────────────────────────────────────────────
class BadActorMonitoring(Base):
    __tablename__ = "bad_actor_monitoring"
    id                    = Column(Integer, primary_key=True, index=True)
    ru                    = Column(String(50))
    tag_number            = Column(String(255))
    status                = Column(String(100))
    problem               = Column(Text)
    action_plan           = Column(Text)
    category_action_plan  = Column(String(100))
    progress              = Column(Text)
    target_date           = Column(String(50))
    periode               = Column(String(50))
    action_plan_category  = Column(String(100))
    external_resource     = Column(String(10))
    no_irkap              = Column(Text)
    action_plan_remark    = Column(Text)

# ─── 5b. ICU (INTEGRITY CONCERN UNIT) ────────────────────────────────────────
class ICUMonitoring(Base):
    __tablename__ = "icu_monitoring"
    id              = Column(Integer, primary_key=True, index=True)
    update_date     = Column(String(50))
    ru              = Column(String(50))
    severity        = Column(String(50))
    equipment       = Column(Text)
    problem         = Column(Text)
    action_plan     = Column(Text)
    no_irkap        = Column(Text)
    progress        = Column(Text)
    target_year     = Column(String(20))

# ─── 6. METERING MONITORING ──────────────────────────────────────────────────
class MeteringMonitoring(Base):
    __tablename__ = "metering_monitoring"
    id                    = Column(Integer, primary_key=True, index=True)
    refinery_unit         = Column(String(100))
    tag_number            = Column(String(100))
    status_metering       = Column(String(100))
    cert_no_metering      = Column(String(255))
    date_expired_metering = Column(String(50))
    remark                = Column(Text)
    rtl                   = Column(String(50))
    action_plan_category  = Column(String(100))
    external_resource     = Column(String(100))
    no_irkap              = Column(String(100))
    status_rtl            = Column(String(50))
    month_update          = Column(String(50))