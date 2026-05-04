from sqlalchemy import Column, Integer, Float, Text, Numeric
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
    nilai_usd = Column(Numeric(18, 2))

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
    report_date          = Column(Date)
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

# ─── 17. TKDN ─────────────────────────────────────────────────────────────────
class TKDN(Base):
    __tablename__ = "tkdn"
    id            = Column(Integer, primary_key=True, index=True)
    refinery_unit = Column(Text)
    bulan         = Column(Text)
    nominal       = Column(Numeric(18, 2))
    kdn           = Column(Numeric(18, 2))
    persentase    = Column(Float)
    tahun         = Column(Integer)

# ─── 18. RCPS REKOMENDASI ─────────────────────────────────────────────────────
class RCPSRekomendasi(Base):
    __tablename__ = "rcps_rekomendasi"
    id                      = Column(Integer, primary_key=True, index=True)
    no                      = Column(Integer)
    kilang                  = Column(Text)
    rcps                    = Column(Text)
    rcps_no                 = Column(Text)
    judul_rcps              = Column(Text)
    link_rcps               = Column(Text)
    rekomendasi             = Column(Text)
    description             = Column(Text)
    traffic                 = Column(Text)
    pic                     = Column(Text)
    target                  = Column(Text)
    recommendation_category = Column(Text)
    external_resource       = Column(Text)
    no_irkap                = Column(Text)
    remark                  = Column(Text)

# ─── 19. RCPS ─────────────────────────────────────────────────────────────────
class RCPS(Base):
    __tablename__ = "rcps"
    id              = Column(Integer, primary_key=True, index=True)
    kilang          = Column(Text)
    traffic         = Column(Text)
    sum_of_progress = Column(Integer)
    link            = Column(Text)
    disiplin        = Column(Text)
    date            = Column(Text)
    judul_rcps      = Column(Text)
    rcps_no         = Column(Text)
    criticallity    = Column(Text)

# ─── 20. BOC (Basis of Comparison) ───────────────────────────────────────────
class BOC(Base):
    __tablename__ = "boc"
    id              = Column(Integer, primary_key=True, index=True)
    ru              = Column(Text)
    area            = Column(Text)
    unit            = Column(Text)
    equipment       = Column(Text)
    grup_equipment  = Column(Text)
    qr_code         = Column(Text)
    rfid            = Column(Text)
    status          = Column(Text)
    frequency       = Column(Integer)
    running_hours   = Column(Float)
    mttr            = Column(Float)
    mtbf            = Column(Float)
    hasil           = Column(Text)

# ─── 21. READINESS JETTY ─────────────────────────────────────────────────────
class ReadinessJetty(Base):
    __tablename__ = "readiness_jetty"
    id                       = Column(Integer, primary_key=True, index=True)
    refinery_unit            = Column(Text)
    area                     = Column(Text)
    unit                     = Column(Text)
    tag_no                   = Column(Text)
    status_operation         = Column(Text)
    no_tuks                  = Column(Text)
    expired_tuks             = Column(Text)
    status_tuks              = Column(Text)
    no_ijin_ops              = Column(Text)
    expired_ijin_ops         = Column(Text)
    status_ijin_ops          = Column(Text)
    no_isps                  = Column(Text)
    expired_isps             = Column(Text)
    status_isps              = Column(Text)
    status_struktur          = Column(Text)
    remark_struktur          = Column(Text)
    status_trestle           = Column(Text)
    remark_trestle           = Column(Text)
    status_mla               = Column(Text)
    remark_mla               = Column(Text)
    status_fire_protection   = Column(Text)
    remark_fire_protection   = Column(Text)
    month_update             = Column(Text)

# ─── 22. WORKPLAN JETTY ──────────────────────────────────────────────────────
class WorkplanJetty(Base):
    __tablename__ = "workplan_jetty"
    id                   = Column(Integer, primary_key=True, index=True)
    refinery_unit        = Column(Text)
    area                 = Column(Text)
    unit                 = Column(Text)
    tag_no               = Column(Text)
    item                 = Column(Text)
    status_item          = Column(Text)
    remark               = Column(Text)
    rtl_action_plan      = Column(Text)
    action_plan_category = Column(Text)
    external_resource    = Column(Text)
    no_irkap             = Column(Text)
    target               = Column(Text)
    keterangan           = Column(Text)
    status_rtl           = Column(Text)
    month_update         = Column(Text)

# ─── 23. READINESS TANK ──────────────────────────────────────────────────────
class ReadinessTank(Base):
    __tablename__ = "readiness_tank"
    id                        = Column(Integer, primary_key=True, index=True)
    refinery_unit             = Column(Text)
    area                      = Column(Text)
    unit                      = Column(Text)
    tag_number                = Column(Text)
    type_tangki               = Column(Text)
    service_tangki            = Column(Text)
    prioritas                 = Column(Text)
    status_operational        = Column(Text)
    cert_no_atg               = Column(Text)
    date_expired_atg          = Column(Text)
    atg_certification_validity= Column(Text)
    coi_date_expired          = Column(Text)
    no_coi                    = Column(Text)
    status_coi                = Column(Text)
    internal_inspection       = Column(Text)
    plan_internal_inspection  = Column(Text)
    status_atg                = Column(Text)
    remark_atg                = Column(Text)
    status_grounding          = Column(Text)
    status_shell_course       = Column(Text)
    remark_shell_course       = Column(Text)
    status_roof               = Column(Text)
    remark_roof               = Column(Text)
    status_cathodic           = Column(Text)
    remark_cathodic           = Column(Text)
    month_update              = Column(Text)

# ─── 24. WORKPLAN TANK ───────────────────────────────────────────────────────
class WorkplanTank(Base):
    __tablename__ = "workplan_tank"
    id                   = Column(Integer, primary_key=True, index=True)
    unit                 = Column(Text)
    tag_no               = Column(Text)
    item                 = Column(Text)
    remark               = Column(Text)
    rtl_action_plan      = Column(Text)
    action_plan_category = Column(Text)
    external_resource    = Column(Text)
    no_irkap             = Column(Text)
    target               = Column(Text)
    keterangan           = Column(Text)
    status_rtl           = Column(Text)
    month_update         = Column(Text)

# ─── 25. READINESS SPM ───────────────────────────────────────────────────────
class ReadinessSPM(Base):
    __tablename__ = "readiness_spm"
    id                        = Column(Integer, primary_key=True, index=True)
    refinery_unit             = Column(Text)
    area                      = Column(Text)
    unit                      = Column(Text)
    tag_no                    = Column(Text)
    status_operation          = Column(Text)
    no_laik_operasi           = Column(Text)
    expired_laik_operasi      = Column(Text)
    status_laik_operasi       = Column(Text)
    no_ijin_spl               = Column(Text)
    expired_ijin_spl          = Column(Text)
    status_ijin_spl           = Column(Text)
    status_mbc                = Column(Text)
    remark_mbc                = Column(Text)
    status_lds                = Column(Text)
    remark_lds                = Column(Text)
    status_mooring_hawser     = Column(Text)
    remark_mooring_hawser     = Column(Text)
    status_floating_hose      = Column(Text)
    remark_floating_hose      = Column(Text)
    status_cathodic_spl       = Column(Text)
    status_cathodic_spm       = Column(Text)
    month_update              = Column(Text)

# ─── 26. SPM WORKPLAN ────────────────────────────────────────────────────────
class SPMWorkplan(Base):
    __tablename__ = "spm_workplan"
    id                   = Column(Integer, primary_key=True, index=True)
    refinery_unit        = Column(Text)
    area                 = Column(Text)
    unit                 = Column(Text)
    tag_no               = Column(Text)
    item                 = Column(Text)
    remark               = Column(Text)
    rtl_action_plan      = Column(Text)
    action_plan_category = Column(Text)
    external_resource    = Column(Text)
    no_irkap             = Column(Text)
    target               = Column(Text)
    keterangan           = Column(Text)
    status_rtl           = Column(Text)
    month_update         = Column(Text)
# ─── 27. IRKAP PROGRAM KERJA ─────────────────────────────────────────────────
class IrkapProgram(Base):
    __tablename__ = "irkap_program"
    id                          = Column(Integer, primary_key=True, index=True)
    refinery_unit               = Column(Text)
    disiplin                    = Column(Text)
    kategori_rkap               = Column(Text)
    material_jasa               = Column(Text)
    highlevel_planning_note     = Column(Text)
    referensi_prokja_sebelumnya = Column(Text)
    no_program_kerja            = Column(Text)
    equipment_tag_no            = Column(Text)
    type_equipment              = Column(Text)
    detail_type_equipment       = Column(Text)
    program_kerja               = Column(Text)
    step_plan_today             = Column(Text)
    detail_step_plan_today      = Column(Text)
    step_actual_today           = Column(Text)
    detail_step_actual_today    = Column(Text)
    status_step                 = Column(Text)
    start_plan                  = Column(Text)
    finish_plan                 = Column(Text)
    status_prognosa             = Column(Text)
    kelompok_biaya              = Column(Text)
    nilai_anggaran_idr          = Column(Numeric(18, 2))
    nilai_anggaran_usd          = Column(Numeric(18, 2))
    top_risk                    = Column(Text)
    asset_integrity             = Column(Text)

# ─── 28. IRKAP ACTUAL STEP ───────────────────────────────────────────────────
class IrkapActual(Base):
    __tablename__ = "irkap_actual"
    id                           = Column(Integer, primary_key=True, index=True)
    no                           = Column(Integer)
    no_program                   = Column(Text)
    kategori_rkap                = Column(Text)
    program_asset_integrity      = Column(Text)
    refinery_unit                = Column(Text)
    area                         = Column(Text)
    unit_process                 = Column(Text)
    tag_no                       = Column(Text)
    dasar_pengusulan             = Column(Text)
    rekomendasi                  = Column(Text)
    program_kerja                = Column(Text)
    disiplin                     = Column(Text)
    kategory_trigger             = Column(Text)
    kelompok_sasaran_rk          = Column(Text)
    kel_biaya                    = Column(Text)
    note                         = Column(Text)
    release_type                 = Column(Text)
    jadwal_pelaksanaan           = Column(Text)
    jadwal_cost                  = Column(Text)
    jadwal_cash                  = Column(Text)
    strategy_penyelesaian        = Column(Text)
    failure_impact               = Column(Text)
    high_level_planning_note     = Column(Text)
    referensi_prokja_sebelumnya  = Column(Text)
    cost_center                  = Column(Text)
    cost_element                 = Column(Text)
    wbs_number                   = Column(Text)
    anggaran_idr                 = Column(Numeric(18, 2))
    anggaran_usd                 = Column(Numeric(18, 2))
    anggaran_equivalent_idr      = Column(Numeric(18, 2))
    probability_class            = Column(Text)
    probability_likelyhood       = Column(Text)
    economic_usd                 = Column(Text)
    health_safety                = Column(Text)
    environment                  = Column(Text)
    ram_criticality              = Column(Text)
    material_jasa                = Column(Text)
    sumber_harga                 = Column(Text)
    # Step 1 — Notifikasi
    actual_start1                = Column(Text)
    actual_finish1               = Column(Text)
    comp1                        = Column(Float)
    notif_no                     = Column(Text)
    # Step 2 — Work Notification
    actual_start2                = Column(Text)
    actual_finish2               = Column(Text)
    comp2                        = Column(Float)
    # Step 3
    actual_start3                = Column(Text)
    actual_finish3               = Column(Text)
    comp3                        = Column(Float)
    wo_no                        = Column(Text)
    # Step 4
    actual_start4                = Column(Text)
    actual_finish4               = Column(Text)
    comp4                        = Column(Float)
    ro_no                        = Column(Text)
    # Step 5
    actual_start5                = Column(Text)
    actual_finish5               = Column(Text)
    comp5                        = Column(Float)
    # Step 6 — PR
    actual_start6                = Column(Text)
    actual_finish6               = Column(Text)
    comp6                        = Column(Float)
    pr                           = Column(Text)
    # Step 7 — RFQ
    actual_start7                = Column(Text)
    actual_finish7               = Column(Text)
    comp7                        = Column(Float)
    rfq                          = Column(Text)
    # Step 8 — PO
    actual_start8                = Column(Text)
    actual_finish8               = Column(Text)
    comp8                        = Column(Float)
    po                           = Column(Text)
    # Step 9 — GR
    actual_start9                = Column(Text)
    actual_finish9               = Column(Text)
    comp9                        = Column(Float)
    gr_no                        = Column(Text)
    # Step 10 — GI
    actual_start10               = Column(Text)
    actual_finish10              = Column(Text)
    comp10                       = Column(Float)
    gi_no                        = Column(Text)
    # Step 11
    actual_start11               = Column(Text)
    actual_finish11              = Column(Text)
    comp11                       = Column(Float)
    # Step 12
    actual_start12               = Column(Text)
    actual_finish12              = Column(Text)
    comp12                       = Column(Float)
    # Step 13 — SA
    actual_start13               = Column(Text)
    actual_finish13              = Column(Text)
    comp13                       = Column(Float)
    sa_no                        = Column(Text)
    # Step 14
    actual_start14               = Column(Text)
    actual_finish14              = Column(Text)
    comp14                       = Column(Float)
    # Step 15
    actual_start15               = Column(Text)
    actual_finish15              = Column(Text)
    comp15                       = Column(Float)
    # Summary
    current_step                 = Column(Integer)
    status_step                  = Column(Text)
    status_prognosa              = Column(Text)

# ─── 29. MASTER DATA EQUIPMENT (IH08) ────────────────────────────────────────
class MasterDataEquipment(Base):
    __tablename__ = "master_data_equipment"
    id                          = Column(Integer, primary_key=True, index=True)
    criticality                 = Column(Text)       # Criticallity
    equipment                   = Column(Text)       # Equipment
    functional_location         = Column(Text)       # Functional Location
    maintenance_plant           = Column(Text)       # Maintenance plant
    location                    = Column(Text)       # Location
    cost_center                 = Column(Text)       # Cost Center
    wbs_element                 = Column(Text)       # WBS element
    main_work_center            = Column(Text)       # Main work center
    planner_group               = Column(Text)       # Planner group
    planning_plant              = Column(Text)       # Planning plant
    catalog_profile             = Column(Text)       # Catalog profile
    equipment_category          = Column(Text)       # Equipment category
    description                 = Column(Text)       # Description of Technical Object
    manufacturer                = Column(Text)       # Manufacturer of asset
    model_type                  = Column(Text)       # Model/Type
    serial_number               = Column(Text)       # Serial Number
    changed_by                  = Column(Text)       # Changed by
    changed_on                  = Column(Text)       # Changed on
    created_by                  = Column(Text)       # Created by
    created_on                  = Column(Text)       # Created on
    technical_obj_type          = Column(Text)       # Technical obj. type
    manufact_serial_number      = Column(Text)       # ManufactSerialNumber
    manufacturer_drawing_number = Column(Text)       # Manufacturer drawing number
    manufacturer_part_number    = Column(Text)       # Manufacturer part number
    material                    = Column(Text)       # Material
    material_1                  = Column(Text)       # Material.1
    material_description        = Column(Text)       # Material Description
    order_no                    = Column(Text)       # Order
    size_dimension              = Column(Text)       # Size/dimension
    sort_field_ata              = Column(Text)       # Sort Field / ATA 100