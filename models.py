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

# ─── 7. PROGRAM KERJA ATG ────────────────────────────────────────────────────
class ProgramKerjaATG(Base):
    __tablename__ = "program_kerja_atg"
    id                   = Column(Integer, primary_key=True, index=True)
    refinery_unit        = Column(Text)
    type                 = Column(Text)
    atg_eksisting        = Column(Text)
    program_2024         = Column(Text)
    prokja               = Column(Text)
    action_plan_category = Column(Text)
    external_resource    = Column(Text)
    no_irkap             = Column(Text)
    target               = Column(Text)
    month_update         = Column(Text)

# ─── 8. PAF (Plant Availability Factor) ──────────────────────────────────────
class PAF(Base):
    __tablename__ = "paf"
    id               = Column(Integer, primary_key=True, index=True)
    month_update     = Column(Text)
    type             = Column(Text)
    ru               = Column(Text)
    target_realisasi = Column(Text)
    color            = Column(Text)
    value            = Column(Float)
    plan_unplan      = Column(Text)
    type2            = Column(Text)
    month            = Column(Text)
    value2           = Column(Float)
    ru2              = Column(Text)
    target           = Column(Float)
    code_current     = Column(Integer)

# ─── 9. ZERO CLAMP MONITORING ────────────────────────────────────────────────
class ZeroClamp(Base):
    __tablename__ = "zero_clamp"
    id                              = Column(Integer, primary_key=True, index=True)
    no                              = Column(Integer)
    ru                              = Column(Text)
    area                            = Column(Text)
    unit                            = Column(Text)
    tag_no_ln                       = Column(Text)
    services                        = Column(Text)
    description                     = Column(Text)
    type_damage                     = Column(Text)
    posisi                          = Column(Text)
    type_perbaikan                  = Column(Text)
    tanggal_dipasang                = Column(Text)
    tanggal_dilepas                 = Column(Text)
    tanggal_rencana_perbaikan       = Column(Text)
    no_irkap                        = Column(Text)
    status                          = Column(Text)
    remarks                         = Column(Text)

# ─── 10. ISSUE PAF ───────────────────────────────────────────────────────────
class IssuePAF(Base):
    __tablename__ = "issue_paf"
    id           = Column(Integer, primary_key=True, index=True)
    type         = Column(Text)
    ru           = Column(Text)
    date         = Column(Text)
    issue        = Column(Text)
    month_update = Column(Text)
    code_current = Column(Integer)

# ─── 11. POWER & STEAM ───────────────────────────────────────────────────────
class PowerStream(Base):
    __tablename__ = "power_stream"
    id               = Column(Integer, primary_key=True, index=True)
    refinery_unit    = Column(Text)
    type_equipment   = Column(Text)
    equipment        = Column(Text)
    status_operation = Column(Text)
    status_n0        = Column(Text)
    unit_measurement = Column(Text)
    desain           = Column(Float)
    kapasitas_max    = Column(Float)
    average_actual   = Column(Float)
    remark           = Column(Text)
    date_update      = Column(Text)
    month_update     = Column(Text)
    code_current     = Column(Integer)

# ─── 12. JUMLAH EQUIPMENT UTL ────────────────────────────────────────────────
class JumlahEqpUTL(Base):
    __tablename__ = "jumlah_eqp_utl"
    id               = Column(Integer, primary_key=True, index=True)
    refinery_unit    = Column(Text)
    type_equipment   = Column(Text)
    status_equipment = Column(Text)
    jumlah           = Column(Integer)
    month_update     = Column(Text)
    code_current     = Column(Integer)

# ─── 13. CRITICAL EQUIPMENT UTL ──────────────────────────────────────────────
class CriticalEqpUTL(Base):
    __tablename__ = "critical_eqp_utl"
    id                     = Column(Integer, primary_key=True, index=True)
    refinery_unit          = Column(Text)
    type_equipment         = Column(Text)
    highlight_issue        = Column(Text)
    corrective_action      = Column(Text)
    target_corrective      = Column(Text)
    traffic_corrective     = Column(Text)
    mitigasi_action        = Column(Text)
    target_mitigasi        = Column(Text)
    traffic_mitigasi       = Column(Text)
    month_update           = Column(Text)
    code_current           = Column(Integer)

# ─── 14. CRITICAL EQUIPMENT PRIMARY & SECONDARY ───────────────────────────────
class CriticalEqpPrimSec(Base):
    __tablename__ = "critical_eqp_prim_sec"
    id                = Column(Integer, primary_key=True, index=True)
    refinery_unit     = Column(Text)
    unit_proses       = Column(Text)
    equipment         = Column(Text)
    highlight_issue   = Column(Text)
    corrective_action = Column(Text)
    target_corrective = Column(Text)
    traffic_corrective= Column(Text)
    mitigasi_action   = Column(Text)
    target_mitigasi   = Column(Text)
    traffic_mitigasi  = Column(Text)
    month_update      = Column(Text)
    code_current      = Column(Integer)

# ─── 15. MONITORING OPERASI ──────────────────────────────────────────────────
class MonitoringOperasi(Base):
    __tablename__ = "monitoring_operasi"
    id                     = Column(Integer, primary_key=True, index=True)
    refinery_unit          = Column(Text)
    unit_proses            = Column(Text)
    unit                   = Column(Text)
    unit_measurement       = Column(Text)
    design                 = Column(Float)
    minimal_capacity       = Column(Float)
    plant_readiness        = Column(Float)
    remark                 = Column(Text)
    type_limitasi_process  = Column(Text)
    equipment_process      = Column(Text)
    limitasi_alert_process = Column(Text)
    mitigasi_process       = Column(Text)
    target_sts             = Column(Float)
    actual                 = Column(Float)
    type_limitasi_sts      = Column(Text)
    equipment_sts          = Column(Text)
    limitasi_alert_sts     = Column(Text)
    mitigasi_sts           = Column(Text)
    month_update           = Column(Text)
    code_current           = Column(Integer)

# ─── 16. INSPECTION PLAN ─────────────────────────────────────────────────────
class InspectionPlan(Base):
    __tablename__ = "inspection_plan"
    id                    = Column(Integer, primary_key=True, index=True)
    refinery_unit         = Column(Text)
    area                  = Column(Text)
    unit                  = Column(Text)
    tag_no_ln             = Column(Text)
    type_equipment        = Column(Text)
    type_inspection       = Column(Text)
    type_pekerjaan        = Column(Text)
    due_date              = Column(Text)
    due_year              = Column(Integer)
    plan_date             = Column(Text)
    plan_year             = Column(Integer)
    actual_date           = Column(Text)
    actual_year           = Column(Integer)
    update_date           = Column(Text)
    result_remaining_life = Column(Float)
    result_visual         = Column(Text)
    visual_lainnya        = Column(Text)
    result_lainnya        = Column(Text)
    grand_result          = Column(Text)