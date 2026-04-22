from sqlalchemy import Column, Integer, Float, Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# ─── 1. REKAP ANGGARAN MAINTENANCE ───────────────────────────────────────────
class AnggaranMaintenance(Base):
    __tablename__ = "anggaran_maintenance"
    id        = Column(Integer, primary_key=True, index=True)
    ru        = Column(Text)
    tahun     = Column(Integer)
    kategori  = Column(Text)
    tipe      = Column(Text)
    nilai_usd = Column(Float)

# ─── 2. PIPELINE INSPECTION ──────────────────────────────────────────────────
class PipelineInspection(Base):
    __tablename__ = "pipeline_inspection"
    id                      = Column(Integer, primary_key=True, index=True)
    refinery_unit           = Column(Text)
    area                    = Column(Text)
    unit                    = Column(Text)
    tag_number              = Column(Text)
    last_inspection_date    = Column(Text)
    next_inspection_date    = Column(Text)
    fluida_service          = Column(Text)
    nps                     = Column(Text)
    from_location           = Column(Text)
    to_location             = Column(Text)
    last_measured_thickness = Column(Float)
    rem_life_years          = Column(Float)
    jumlah_temporary_repair = Column(Integer)
    remarks                 = Column(Text)
    bulan                   = Column(Text)
    tahun                   = Column(Integer)

# ─── 3. ROTOR MONITORING ─────────────────────────────────────────────────────
class RotorMonitoring(Base):
    __tablename__ = "rotor_monitoring"
    id                     = Column(Integer, primary_key=True, index=True)
    no                     = Column(Integer)
    refinery_unit          = Column(Text)
    bulan                  = Column(Text)
    rotor                  = Column(Text)
    program                = Column(Text)
    brand                  = Column(Text)
    status_readiness_spare = Column(Text)
    status_workplan        = Column(Text)
    detail_status_workplan = Column(Text)
    keterangan             = Column(Text)
    action_plan_category   = Column(Text)
    external_resource      = Column(Text)
    no_irkap               = Column(Text)
    finish_date_eksekusi   = Column(Text)
    readiness_rotor        = Column(Integer)
    last_update            = Column(Text)

# ─── 4. ATG MONITORING ───────────────────────────────────────────────────────
class ATGMonitoring(Base):
    __tablename__ = "atg_monitoring"
    id                      = Column(Integer, primary_key=True, index=True)
    refinery_unit           = Column(Text)
    tag_no_tangki           = Column(Text)
    tag_no_atg              = Column(Text)
    status_atg              = Column(Text)
    status_interkoneksi_atg = Column(Text)
    cert_no_atg             = Column(Text)
    date_expired_atg        = Column(Text)
    remark                  = Column(Text)
    rtl                     = Column(Text)
    action_plan_category    = Column(Text)
    external_resource       = Column(Text)
    no_irkap                = Column(Text)
    status_rtl              = Column(Text)
    month_update            = Column(Text)

# ─── 5a. BAD ACTOR MONITORING ────────────────────────────────────────────────
class BadActorMonitoring(Base):
    __tablename__ = "bad_actor_monitoring"
    id                    = Column(Integer, primary_key=True, index=True)
    ru                    = Column(Text)
    tag_number            = Column(Text)
    status                = Column(Text)
    problem               = Column(Text)
    action_plan           = Column(Text)
    category_action_plan  = Column(Text)
    progress              = Column(Text)
    target_date           = Column(Text)
    periode               = Column(Text)
    action_plan_category  = Column(Text)
    external_resource     = Column(Text)
    no_irkap              = Column(Text)
    action_plan_remark    = Column(Text)

# ─── 5b. ICU (INTEGRITY CONCERN UNIT) ────────────────────────────────────────
class ICUMonitoring(Base):
    __tablename__ = "icu_monitoring"
    id                   = Column(Integer, primary_key=True, index=True)
    report_date          = Column(Text)
    ru                   = Column(Text)
    icu_status           = Column(Text)
    tag_no               = Column(Text)
    issue                = Column(Text)
    mitigation           = Column(Text)
    mitigasi_category    = Column(Text)
    mitigation_external  = Column(Text)
    irkap_mitigation     = Column(Text)
    remark_mitigation    = Column(Text)
    permanent_solution   = Column(Text)
    solution_category    = Column(Text)
    solution_external    = Column(Text)
    irkap_solution       = Column(Text)
    remark_solution      = Column(Text)
    progress             = Column(Text)
    info                 = Column(Text)
    target_closed        = Column(Text)

# ─── 6. METERING MONITORING ──────────────────────────────────────────────────
class MeteringMonitoring(Base):
    __tablename__ = "metering_monitoring"
    id                    = Column(Integer, primary_key=True, index=True)
    refinery_unit         = Column(Text)
    tag_number            = Column(Text)
    status_metering       = Column(Text)
    cert_no_metering      = Column(Text)
    date_expired_metering = Column(Text)
    remark                = Column(Text)
    rtl                   = Column(Text)
    action_plan_category  = Column(Text)
    external_resource     = Column(Text)
    no_irkap              = Column(Text)
    status_rtl            = Column(Text)
    month_update          = Column(Text)