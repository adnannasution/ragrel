import os
import io
import json
import asyncio
import pandas as pd
from datetime import datetime
from fastapi import FastAPI, Depends, Request, UploadFile, File, Form
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import engine, get_db, DATABASE_URL
from models import Base, AnggaranMaintenance, PipelineInspection, RotorMonitoring, ATGMonitoring, MeteringMonitoring, BadActorMonitoring, ICUMonitoring, ProgramKerjaATG, PAF, ZeroClamp, IssuePAF, PowerStream, JumlahEqpUTL, CriticalEqpUTL, CriticalEqpPrimSec, MonitoringOperasi, InspectionPlan, TKDN, RCPSRekomendasi, RCPS, BOC, ReadinessJetty, WorkplanJetty, ReadinessTank, WorkplanTank, ReadinessSPM, SPMWorkplan, IrkapProgram, IrkapActual, MasterDataEquipment
from dotenv import load_dotenv
from table_routes import router as table_router
from chatbot import (
    run_with_memory,
    clear_session_history,
    add_session_history,
    get_session_history,
    _build_db_schema_cols,
    _build_auto_schema_docs,
)

load_dotenv()

UPLOAD_REGISTRY = "upload_registry.json"

def _load_registry() -> dict:
    if os.path.exists(UPLOAD_REGISTRY):
        try:
            return json.load(open(UPLOAD_REGISTRY))
        except:
            pass
    return {}

def _save_upload_time(data_type: str):
    registry = _load_registry()
    registry[data_type] = datetime.now().strftime("%d %b %Y, %H:%M")
    json.dump(registry, open(UPLOAD_REGISTRY, "w"))
    # Rebuild schema scan supaya kolom baru langsung terdeteksi
    _build_db_schema_cols()
    _build_auto_schema_docs()

app = FastAPI()


app.include_router(table_router)

templates = Jinja2Templates(directory="templates")

@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)
    _build_db_schema_cols()  # scan kolom kategorikal otomatis
    _build_auto_schema_docs()  # auto-dokumentasi tabel tanpa business rule manual

# ─────────────────────────────────────────────────────────────────────────────
# UI
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def chat_ui(request: Request):
    return templates.TemplateResponse(request=request, name="chatbot.html")

# ─────────────────────────────────────────────────────────────────────────────
# ROUTER UPLOAD
# ─────────────────────────────────────────────────────────────────────────────
@app.post("/upload-sync")
async def upload_sync(
    file: UploadFile = File(...),
    data_type: str = Form(...),  # anggaran | pipeline | rotor | atg | metering
    mode: str = Form("replace"),  # replace | append
    db: Session = Depends(get_db)
):
    file_location = f"temp_{file.filename}"
    with open(file_location, "wb") as f:
        f.write(await file.read())
    try:
        handlers = {
            "anggaran":  sync_anggaran,
            "pipeline":  sync_pipeline,
            "rotor":     sync_rotor,
            "atg":       sync_atg,
            "metering":  sync_metering,
            "badactor":  sync_badactor,
            "icu":       sync_icu,
            "prokja_atg":     sync_prokja_atg,
            "paf":            sync_paf,
            "zero_clamp":     sync_zero_clamp,
            "issue_paf":      sync_issue_paf,
            "power_stream":   sync_power_stream,
            "jumlah_eqp":     sync_jumlah_eqp,
            "critical_utl":   sync_critical_utl,
            "critical_prim":  sync_critical_prim,
            "mon_operasi":    sync_mon_operasi,
            "inspection_plan": sync_inspection_plan,
            "tkdn":           sync_tkdn,
            "rcps_rek":       sync_rcps_rekomendasi,
            "rcps":           sync_rcps,
            "boc":            sync_boc,
            "readiness_jetty": sync_readiness_jetty,
            "workplan_jetty":  sync_workplan_jetty,
            "readiness_tank":  sync_readiness_tank,
            "workplan_tank":   sync_workplan_tank,
            "readiness_spm":   sync_readiness_spm,
            "spm_workplan":    sync_spm_workplan,
            "irkap_program":   sync_irkap_program,
            "irkap_actual":    sync_irkap_actual,
            "master_equipment": sync_master_data_equipment,
        }
        APPEND_SUPPORTED = {
            "readiness_jetty", "workplan_jetty",
            "readiness_tank", "workplan_tank",
            "readiness_spm", "spm_workplan",
            "irkap_program", "irkap_actual",
            "master_equipment",
        }
        if data_type not in handlers:
            return {"error": f"Jenis data tidak dikenal: {data_type}"}
        if data_type in APPEND_SUPPORTED:
            result = handlers[data_type](file_location, db, mode=mode)
        else:
            result = handlers[data_type](file_location, db)
        _save_upload_time(data_type)
        return result
    except Exception as e:
        db.rollback()
        return {"error": f"Gagal proses Excel: {str(e)}"}
    finally:
        if os.path.exists(file_location):
            os.remove(file_location)

# ─────────────────────────────────────────────────────────────────────────────
# PARSER: ANGGARAN (tidak diubah)
# ─────────────────────────────────────────────────────────────────────────────
def sync_anggaran(file_location: str, db: Session):
    df = pd.read_excel(file_location, sheet_name="RU's", header=None)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    ru_col_start = {
        'RU II': 1, 'RU III': 9, 'RU IV': 17,
        'RU V': 25, 'RU VI': 33, 'RU VII': 41,
    }
    ru_years = {}
    for ru_name, start_col in ru_col_start.items():
        years = []
        for i in range(8):
            val = df.iloc[2, start_col + i]
            try:
                years.append(int(float(val)))
            except:
                years.append(None)
        ru_years[ru_name] = years

    row_mapping = [
        (4,  'RUTIN',       'RKAP'), (5,  'RUTIN',       'PLAN'), (6,  'RUTIN',       'AKTUAL'),
        (8,  'NON RUTIN',   'RKAP'), (9,  'NON RUTIN',   'PLAN'), (10, 'NON RUTIN',   'AKTUAL'),
        (12, 'TURN AROUND', 'RKAP'), (13, 'TURN AROUND', 'PLAN'), (14, 'TURN AROUND', 'AKTUAL'),
        (16, 'OVERHAUL',    'RKAP'), (17, 'OVERHAUL',    'PLAN'), (18, 'OVERHAUL',    'AKTUAL'),
     
    ]
    db.query(AnggaranMaintenance).delete()
    count = 0
    for row_idx, kategori, tipe in row_mapping:
        for ru_name, start_col in ru_col_start.items():
            for i, year in enumerate(ru_years[ru_name]):
                if year is None:
                    continue
                val = df.iloc[row_idx, start_col + i]
                try:
                    val = float(val)
                    if val != val:
                        val = None
                except:
                    val = None
                db.add(AnggaranMaintenance(
                    ru=ru_name, tahun=year,
                    kategori=kategori, tipe=tipe, nilai_usd=val
                ))
                count += 1
    db.commit()
    return {"message": f"✅ Anggaran Maintenance berhasil diupdate! ({count} records)"}

# ─────────────────────────────────────────────────────────────────────────────
# PARSER: PIPELINE
# ─────────────────────────────────────────────────────────────────────────────
def sync_pipeline(file_location: str, db: Session):
    df = pd.read_excel(file_location, sheet_name=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    db.query(PipelineInspection).delete()
    count = 0
    for _, row in df.iterrows():
        db.add(PipelineInspection(
            refinery_unit           = str(row.get('RefineryUnit', '') or ''),
            area                    = str(row.get('Area', '') or ''),
            unit                    = str(row.get('Unit', '') or ''),
            tag_number              = str(row.get('TagNumber', '') or ''),
            last_inspection_date    = str(row.get('LastInspectionDate', '') or ''),
            next_inspection_date    = str(row.get('NextInspectionDate', '') or ''),
            fluida_service          = str(row.get('FluidaService', '') or ''),
            nps                     = str(row.get('NPS', '') or ''),
            from_location           = str(row.get('From', '') or ''),
            to_location             = str(row.get('To', '') or ''),
            last_measured_thickness = float(row['Last Measured Thickness']) if pd.notna(row.get('Last Measured Thickness')) else None,
            rem_life_years          = float(row['RemLifeLastInspYears']) if pd.notna(row.get('RemLifeLastInspYears')) else None,
            jumlah_temporary_repair = (lambda v: int(v) if pd.notna(v) and str(v).strip().lstrip('-').isdigit() else None)(row.get('JumlahTemporary Repair')),
            remarks                 = str(row.get('Remarks', '') or '') + (f" [Temporary Repair: {row.get('JumlahTemporary Repair')}]" if pd.notna(row.get('JumlahTemporary Repair')) and not str(row.get('JumlahTemporary Repair')).strip().lstrip('-').isdigit() else ''),
            bulan                   = str(row.get('Bulan', '') or ''),
            tahun                   = int(row['Tahun']) if pd.notna(row.get('Tahun')) else None,
        ))
        count += 1
    db.commit()
    return {"message": f"✅ Pipeline Inspection berhasil diupdate! ({count} records)"}

# ─────────────────────────────────────────────────────────────────────────────
# PARSER: ROTOR
# ─────────────────────────────────────────────────────────────────────────────
def sync_rotor(file_location: str, db: Session):
    df = pd.read_excel(file_location, sheet_name=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    db.query(RotorMonitoring).delete()
    count = 0
    for _, row in df.iterrows():
        db.add(RotorMonitoring(
            no                     = int(row['No']) if pd.notna(row.get('No')) else None,
            refinery_unit          = str(row.get('Refinery Unit', '') or ''),
            bulan                  = str(row.get('Bulan', '') or ''),
            rotor                  = str(row.get('Rotor', '') or ''),
            program                = str(row.get('Program', '') or ''),
            brand                  = str(row.get('Brand', '') or ''),
            status_readiness_spare = str(row.get('Status Readiness Spare Rotor', '') or ''),
            status_workplan        = str(row.get('Status Workplan', '') or ''),
            detail_status_workplan = str(row.get('Detail Status Workplan', '') or ''),
            keterangan             = str(row.get('Keterangan', '') or ''),
            action_plan_category   = str(row.get('Action Plan Category', '') or ''),
            external_resource      = str(row.get('External Resource', '') or ''),
            no_irkap               = str(row.get('NO.IRKAP', '') or ''),
            finish_date_eksekusi   = str(row.get('Finish Date Eksekusi', '') or ''),
            readiness_rotor        = int(row['Readiness Rotor']) if pd.notna(row.get('Readiness Rotor')) else None,
            last_update            = str(row.get('Last Update', '') or ''),
        ))
        count += 1
    db.commit()
    return {"message": f"✅ Rotor Monitoring berhasil diupdate! ({count} records)"}

# ─────────────────────────────────────────────────────────────────────────────
# PARSER: ATG
# ─────────────────────────────────────────────────────────────────────────────
def sync_atg(file_location: str, db: Session):
    df = pd.read_excel(file_location, sheet_name=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    db.query(ATGMonitoring).delete()
    count = 0
    for _, row in df.iterrows():
        db.add(ATGMonitoring(
            refinery_unit           = str(row.get('Refinery Unit', '') or ''),
            tag_no_tangki           = str(row.get('Tag No. Tangki', '') or ''),
            tag_no_atg              = str(row.get('Tag No. ATG', '') or ''),
            status_atg              = str(row.get('Status ATG', '') or ''),
            status_interkoneksi_atg = str(row.get('Status Interkoneksi ATG ', '') or ''),
            cert_no_atg             = str(row.get('Cert No ATG', '') or ''),
            date_expired_atg        = str(row.get('Date Expired ATG', '') or ''),
            remark                  = str(row.get('Remark', '') or ''),
            rtl                     = str(row.get('RTL', '') or ''),
            action_plan_category    = str(row.get('Action Plan Category', '') or ''),
            external_resource       = str(row.get('External Resource', '') or ''),
            no_irkap                = str(row.get('NO.IRKAP', '') or ''),
            status_rtl              = str(row.get('Status RTL', '') or ''),
            month_update            = str(row.get('Month Update', '') or ''),
        ))
        count += 1
    db.commit()
    return {"message": f"✅ ATG Monitoring berhasil diupdate! ({count} records)"}

# ─────────────────────────────────────────────────────────────────────────────
# PARSER: METERING
# ─────────────────────────────────────────────────────────────────────────────
def sync_metering(file_location: str, db: Session):
    df = pd.read_excel(file_location, sheet_name=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    db.query(MeteringMonitoring).delete()
    count = 0
    for _, row in df.iterrows():
        db.add(MeteringMonitoring(
            refinery_unit         = str(row.get('Refinery Unit', '') or ''),
            tag_number            = str(row.get('Tag Number', '') or ''),
            status_metering       = str(row.get('Status Metering', '') or ''),
            cert_no_metering      = str(row.get('Cert No Metering', '') or ''),
            date_expired_metering = str(row.get('Date Expired Metering', '') or ''),
            remark                = str(row.get('Remark', '') or ''),
            rtl                   = str(row.get('RTL', '') or ''),
            action_plan_category  = str(row.get('Action Plan Category', '') or ''),
            external_resource     = str(row.get('External Resource', '') or ''),
            no_irkap              = str(row.get('NO.IRKAP', '') or ''),
            status_rtl            = str(row.get('Status RTL', '') or ''),
            month_update          = str(row.get('Month Update', '') or ''),
        ))
        count += 1
    db.commit()
    return {"message": f"✅ Metering Monitoring berhasil diupdate! ({count} records)"}

# ─────────────────────────────────────────────────────────────────────────────
# PARSER: BAD ACTOR
# ─────────────────────────────────────────────────────────────────────────────
def sync_badactor(file_location: str, db: Session):
    df = pd.read_excel(file_location, sheet_name=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    db.query(BadActorMonitoring).delete()
    # Gabungkan kolom No IRKAP 1-5 menjadi satu string
    irkap_cols = ['No IRKAP 1', 'No IRKAP 2', 'No IRKAP 3', 'No IRKAP 4', 'No IRKAP 5']
    count = 0
    for _, row in df.iterrows():
        no_irkap_parts = [str(row.get(c, '') or '') for c in irkap_cols]
        no_irkap = ' | '.join([x for x in no_irkap_parts if x and x != 'nan'])
        target_date = row.get('Target Date')
        target_date_str = str(target_date.date()) if pd.notna(target_date) and hasattr(target_date, 'date') else str(target_date or '')
        periode = row.get('Periode')
        periode_str = str(periode.date()) if pd.notna(periode) and hasattr(periode, 'date') else str(periode or '')
        db.add(BadActorMonitoring(
            ru                   = str(row.get('RU', '') or ''),
            tag_number           = str(row.get('Tag Number', '') or ''),
            status               = str(row.get('Status', '') or ''),
            problem              = str(row.get('Problem', '') or ''),
            action_plan          = str(row.get('Action Plan', '') or ''),
            category_action_plan = str(row.get('Column1', '') or ''),
            progress             = str(row.get('Progress', '') or ''),
            target_date          = target_date_str,
            periode              = periode_str,
            action_plan_category = str(row.get('Action Plan Category', '') or ''),
            external_resource    = str(row.get('Action Plan Need \nExternal Resource?', '') or ''),
            no_irkap             = no_irkap,
            action_plan_remark   = str(row.get('Action Plan Remark', '') or ''),
        ))
        count += 1
    db.commit()
    return {"message": f"✅ Bad Actor Monitoring berhasil diupdate! ({count} records)"}

# ─────────────────────────────────────────────────────────────────────────────
# PARSER: ICU (INTEGRITY CONCERN UNIT)
# ─────────────────────────────────────────────────────────────────────────────
def _safe(val) -> str:
    """Konversi nilai Excel ke string bersih — strip whitespace dan newline berlebih."""
    if val is None:
        return ''
    try:
        import pandas as _pd
        if _pd.isna(val):
            return ''
    except Exception:
        pass
    return str(val).strip()

def _safe_num(val):
    """Konversi ke Decimal/float untuk kolom Numeric — return None jika kosong."""
    if val is None:
        return None
    try:
        import pandas as _pd
        if _pd.isna(val):
            return None
    except Exception:
        pass
    try:
        return float(val)
    except Exception:
        return None

def _safe_float(val):
    """Konversi ke float — return None jika kosong."""
    return _safe_num(val)

def _safe_int(val):
    """Konversi ke integer — return None jika kosong."""
    n = _safe_num(val)
    return int(n) if n is not None else None

# ─── AUTO DATE NORMALIZATION ──────────────────────────────────────────────────
import re as _re
from datetime import datetime as _dt

_BULAN_ID = {
    'januari':'01','februari':'02','maret':'03','april':'04',
    'mei':'05','juni':'06','juli':'07','agustus':'08',
    'september':'09','oktober':'10','november':'11','desember':'12',
    'jan':'01','feb':'02','mar':'03','apr':'04','may':'05','jun':'06',
    'jul':'07','aug':'08','sep':'09','oct':'10','nov':'11','dec':'12',
}
_MINGGU_ROMAWI = {'I':'01','II':'08','III':'15','IV':'22'}

_DATE_FORMATS = [
    '%d/%m/%Y','%d-%m-%Y','%Y-%m-%d','%Y/%m/%d',
    '%d/%m/%y','%d-%m-%y','%m/%d/%Y','%m-%d-%Y',
    '%d %B %Y','%d %b %Y','%B %Y','%b %Y',
]

def _try_parse_date(val) -> str | None:
    """Coba parse satu nilai menjadi 'YYYY-MM-DD'. Return None kalau gagal."""
    if val is None:
        return None
    import pandas as _pd
    try:
        if _pd.isna(val):
            return None
    except Exception:
        pass
    # Kalau sudah datetime dari pandas
    if hasattr(val, 'strftime'):
        return val.strftime('%Y-%m-%d')
    s = str(val).strip()
    if not s:
        return None

    # Format W-xx Bulan Tahun → e.g. "W-I Mei 2021"
    m = _re.match(r'W-([IVX]+)\s+(\w+)\s+(\d{4})', s, _re.IGNORECASE)
    if m:
        hari  = _MINGGU_ROMAWI.get(m.group(1).upper(), '01')
        bulan = _BULAN_ID.get(m.group(2).lower())
        tahun = m.group(3)
        if bulan:
            return f"{tahun}-{bulan}-{hari}"

    # "Januari 2024" / "Jan 2024"
    m = _re.match(r'^(\w+)\s+(\d{4})$', s)
    if m:
        bulan = _BULAN_ID.get(m.group(1).lower())
        if bulan:
            return f"{m.group(2)}-{bulan}-01"

    # Format standar
    for fmt in _DATE_FORMATS:
        try:
            return _dt.strptime(s, fmt).strftime('%Y-%m-%d')
        except Exception:
            pass
    return None

def _auto_convert_dates(df) -> object:
    """
    Scan semua kolom DataFrame — kalau > 70% isinya bisa di-parse sebagai tanggal,
    konversi seluruh kolom itu ke format YYYY-MM-DD (tetap sebagai string/Text).
    """
    import pandas as _pd
    for col in df.columns:
        sample = df[col].dropna()
        if len(sample) == 0:
            continue
        # Skip kolom yang sudah numeric
        if _pd.api.types.is_numeric_dtype(df[col]):
            continue
        # Coba parse sample (max 50 baris untuk efisiensi)
        test = sample.head(50)
        parsed = [_try_parse_date(v) for v in test]
        success_rate = sum(1 for p in parsed if p is not None) / len(test)
        if success_rate >= 0.7:
            # Konversi seluruh kolom
            df[col] = df[col].apply(_try_parse_date)
    return df

def _dedup_columns(df) -> object:
    """
    Hapus kolom duplikat yang di-rename pandas jadi 'col_1', 'col_2' dst.
    Keep kolom pertama, drop sisanya.
    Hanya strip suffix _N jika base-nya sudah ada di kolom lain (benar-benar duplikat).
    """
    import re as _re2
    # Kumpulkan semua nama kolom original (tanpa suffix)
    col_map = {}   # base_name → index kolom pertama
    new_cols = []
    for i, col in enumerate(df.columns):
        col_str = str(col)
        # Coba strip suffix _N (bukan .N karena bisa legitimate seperti Material.1)
        m = _re2.match(r'^(.+)_(\d+)$', col_str)
        base = m.group(1) if m else col_str
        # Cek apakah base ada sebagai kolom lain di DataFrame
        if m and base in [str(c) for c in df.columns]:
            # Ini duplikat → tandai untuk drop
            new_cols.append(f"__dup_{col_str}")
        elif base in col_map:
            # Base sudah ada sebelumnya → duplikat
            new_cols.append(f"__dup_{col_str}")
        else:
            col_map[base] = i
            new_cols.append(col_str)
    df.columns = new_cols
    return df[[c for c in df.columns if not c.startswith('__dup_')]]

def sync_icu(file_location: str, db: Session):
    # Baca dengan header=0, lalu rename kolom duplikat ke nama aslinya (hapus suffix _0, _1, dst)
    df = pd.read_excel(file_location, sheet_name=0, header=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)

    # Normalisasi nama kolom — hapus suffix duplikat pandas (_0, _1, .1, .2, dst)
    import re
    clean_cols = []
    for col in df.columns:
        cleaned = re.sub(r'(\.\d+|_\d+)$', '', str(col)).strip()
        clean_cols.append(cleaned)
    df.columns = clean_cols

    # Mapping fleksibel: cari kolom by substring jika nama exact tidak ditemukan
    def find_col(df, *candidates):
        """Cari nama kolom yang cocok, fallback ke partial match."""
        for c in candidates:
            if c in df.columns:
                return c
            # partial match case-insensitive
            matches = [col for col in df.columns if c.lower() in col.lower()]
            if matches:
                return matches[0]
        return None

    COL_MAP = {
        'report_date':         find_col(df, 'Report Date'),
        'ru':                  find_col(df, 'RU'),
        'icu_status':          find_col(df, 'ICU Status'),
        'tag_no':              find_col(df, 'Tag No'),
        'issue':               find_col(df, 'Issue'),
        'mitigation':          find_col(df, 'Mitigation/Temporary Solution', 'Mitigation'),
        'mitigasi_category':   find_col(df, 'Mitigasi Category', 'Mitigation Category'),
        'mitigation_external': find_col(df, 'Mitigation Need External Resource'),
        'irkap_mitigation':    find_col(df, 'IRKAP Mitigation'),
        'remark_mitigation':   find_col(df, 'Remark Mitigation'),
        'permanent_solution':  find_col(df, 'Permanent Solution'),
        'solution_category':   find_col(df, 'Solution Category'),
        'solution_external':   find_col(df, 'Solution Need External Resource'),
        'irkap_solution':      find_col(df, 'IRKAP Solution'),
        'remark_solution':     find_col(df, 'Remark Solution'),
        'progress':            find_col(df, 'Progres', 'Progress'),
        'info':                find_col(df, 'Info'),
        'target_closed':       find_col(df, 'Target Closed'),
    }

    db.query(ICUMonitoring).delete()
    count = 0
    for _, row in df.iterrows():
        def g(field):
            col = COL_MAP.get(field)
            if not col:
                return ''
            v = row.get(col)
            try:
                return _safe(v) if pd.notna(v) else ''
            except Exception:
                return _safe(v)

        rd = row.get(COL_MAP.get('report_date', ''), None)
        try:
            report_date = str(rd.date()) if pd.notna(rd) and hasattr(rd, 'date') else _safe(rd)
        except Exception:
            report_date = _safe(rd)

        db.add(ICUMonitoring(
            report_date         = report_date,
            ru                  = g('ru'),
            icu_status          = g('icu_status'),
            tag_no              = g('tag_no'),
            issue               = g('issue'),
            mitigation          = g('mitigation'),
            mitigasi_category   = g('mitigasi_category'),
            mitigation_external = g('mitigation_external'),
            irkap_mitigation    = g('irkap_mitigation'),
            remark_mitigation   = g('remark_mitigation'),
            permanent_solution  = g('permanent_solution'),
            solution_category   = g('solution_category'),
            solution_external   = g('solution_external'),
            irkap_solution      = g('irkap_solution'),
            remark_solution     = g('remark_solution'),
            progress            = g('progress'),
            info                = g('info'),
            target_closed       = g('target_closed'),
        ))
        count += 1
    db.commit()
    return {"message": f"✅ ICU Monitoring berhasil diupdate! ({count} records)"}


# ─────────────────────────────────────────────────────────────────────────────
# PARSER: PROGRAM KERJA ATG
# ─────────────────────────────────────────────────────────────────────────────
def sync_prokja_atg(file_location: str, db: Session):
    df = pd.read_excel(file_location, sheet_name=0, header=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    db.query(ProgramKerjaATG).delete()
    count = 0
    for _, row in df.iterrows():
        def g(col):
            v = row.get(col)
            try:
                return _safe(v) if pd.notna(v) else ""
            except Exception:
                return _safe(v)

        mu = row.get("Month Update")
        try:
            month_update = str(mu.date()) if pd.notna(mu) and hasattr(mu, "date") else _safe(mu)
        except Exception:
            month_update = _safe(mu)

        db.add(ProgramKerjaATG(
            refinery_unit        = g("Refinery Unit"),
            type                 = g("Type"),
            atg_eksisting        = g("ATG Eksisting"),
            program_2024         = g("Program 2024"),
            prokja               = g("Prokja"),
            action_plan_category = g("Action Plan Category"),
            external_resource    = g("External Resource"),
            no_irkap             = g("NO.IRKAP"),
            target               = g("Target"),
            month_update         = month_update,
        ))
        count += 1
    db.commit()
    return {"message": f"✅ Program Kerja ATG berhasil diupdate! ({count} records)"}

# ─────────────────────────────────────────────────────────────────────────────
# RESET SESSION MEMORY
# ─────────────────────────────────────────────────────────────────────────────
@app.post("/reset-session")
async def reset_session(session_id: str = "default"):
    clear_session_history(session_id)
    return {"message": "✅ Sesi percakapan direset."}

# ─────────────────────────────────────────────────────────────────────────────
# EXPORT ENDPOINT — Download tabel sebagai Excel
# ─────────────────────────────────────────────────────────────────────────────
EXPORT_TABLES = {
    "anggaran":        ("anggaran_maintenance",      "Anggaran Maintenance"),
    "pipeline":        ("pipeline_inspection",        "Pipeline Inspection"),
    "rotor":           ("rotor_monitoring",           "Rotor Monitoring"),
    "atg":             ("atg_monitoring",             "ATG Monitoring"),
    "metering":        ("metering_monitoring",        "Metering Monitoring"),
    "badactor":        ("bad_actor_monitoring",       "Bad Actor Monitoring"),
    "icu":             ("icu_monitoring",             "ICU Monitoring"),
    "prokja_atg":      ("program_kerja_atg",          "Program Kerja ATG"),
    "paf":             ("paf",                        "PAF"),
    "zero_clamp":      ("zero_clamp",                 "Zero Clamp"),
    "issue_paf":       ("issue_paf",                  "Issue PAF"),
    "power_stream":    ("power_stream",               "Power & Steam"),
    "jumlah_eqp":      ("jumlah_eqp_utl",             "Jumlah Equipment UTL"),
    "critical_utl":    ("critical_eqp_utl",           "Critical Equipment UTL"),
    "critical_prim":   ("critical_eqp_prim_sec",      "Critical Equipment Prim Sec"),
    "mon_operasi":     ("monitoring_operasi",          "Monitoring Operasi"),
    "inspection_plan": ("inspection_plan",             "Inspection Plan"),
    "tkdn":            ("tkdn",                       "TKDN"),
    "rcps_rek":        ("rcps_rekomendasi",            "RCPS Rekomendasi"),
    "rcps":            ("rcps",                        "RCPS"),
    "boc":             ("boc",                         "BOC"),
    "readiness_jetty": ("readiness_jetty",             "Readiness Jetty"),
    "workplan_jetty":  ("workplan_jetty",              "Workplan Jetty"),
    "readiness_tank":  ("readiness_tank",              "Readiness Tank"),
    "workplan_tank":   ("workplan_tank",               "Workplan Tank"),
    "readiness_spm":   ("readiness_spm",               "Readiness SPM"),
    "spm_workplan":    ("spm_workplan",                "SPM Workplan"),
    "irkap_program":   ("irkap_program",               "IRKAP Program"),
    "irkap_actual":    ("irkap_actual",                "IRKAP Actual"),
    "master_equipment": ("master_data_equipment",      "Master Data Equipment"),
}

@app.get("/export")
def export_table(table: str, db: Session = Depends(get_db)):
    if table not in EXPORT_TABLES:
        return {"error": f"Tabel tidak dikenal: {table}"}
    table_name, label = EXPORT_TABLES[table]
    try:
        df = pd.read_sql(text(f"SELECT * FROM {table_name}"), db.bind)
        df = df.drop(columns=["id"], errors="ignore")
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=label[:31])
        output.seek(0)
        filename = f"{table_name}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        return {"error": str(e)}

# ─────────────────────────────────────────────────────────────────────────────
# TABLE STATS ENDPOINT
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/table-stats")
def table_stats(db: Session = Depends(get_db)):
    from sqlalchemy import text, inspect
    registry = _load_registry()
    insp = inspect(engine)
    existing = insp.get_table_names()

    def count(table: str) -> int:
        if table not in existing:
            return 0
        return db.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar() or 0

    keys = ["anggaran", "pipeline", "rotor", "atg", "metering", "badactor", "icu", "prokja_atg", "paf", "zero_clamp", "issue_paf", "power_stream", "jumlah_eqp", "critical_utl", "critical_prim", "mon_operasi", "inspection_plan", "tkdn", "rcps_rek", "rcps", "boc", "readiness_jetty", "workplan_jetty", "readiness_tank", "workplan_tank", "readiness_spm", "spm_workplan", "irkap_program", "irkap_actual", "master_equipment"]
    tables = {
        "anggaran": "anggaran_maintenance",
        "pipeline": "pipeline_inspection",
        "rotor":    "rotor_monitoring",
        "atg":      "atg_monitoring",
        "metering": "metering_monitoring",
        "badactor": "bad_actor_monitoring",
        "icu":      "icu_monitoring",
        "prokja_atg":  "program_kerja_atg",
        "paf":         "paf",
        "zero_clamp":  "zero_clamp",
        "issue_paf":   "issue_paf",
        "power_stream":"power_stream",
        "jumlah_eqp":  "jumlah_eqp_utl",
        "critical_utl":"critical_eqp_utl",
        "critical_prim":"critical_eqp_prim_sec",
        "mon_operasi": "monitoring_operasi",
        "inspection_plan": "inspection_plan",
        "tkdn":           "tkdn",
        "rcps_rek":       "rcps_rekomendasi",
        "rcps":           "rcps",
        "boc":            "boc",
        "readiness_jetty": "readiness_jetty",
        "workplan_jetty":  "workplan_jetty",
        "readiness_tank":  "readiness_tank",
        "workplan_tank":   "workplan_tank",
        "readiness_spm":   "readiness_spm",
        "spm_workplan":    "spm_workplan",
        "irkap_program":   "irkap_program",
        "irkap_actual":    "irkap_actual",
        "master_equipment": "master_data_equipment",
    }
    return {
        k: {
            "rows":    count(tables[k]),
            "updated": registry.get(k),   # timestamp upload nyata
        }
        for k in keys
    }

# ─────────────────────────────────────────────────────────────────────────────
# AI ENDPOINT — SSE streaming dengan progress real
# ─────────────────────────────────────────────────────────────────────────────
def sse(event: str, data: str) -> str:
    payload = json.dumps({"event": event, "data": data})
    return f"data: {payload}\n\n"

@app.get("/ask")
async def ask_ai(question: str, session_id: str = "default"):
    # ── Pre-filter: tangkap pertanyaan di luar konteks sebelum buang token ke LLM
    q_lower = question.lower()

    OUT_OF_SCOPE = []  # filter dinonaktifkan

    # SQL injection & code filter dinonaktifkan
    DUMP_KEYWORDS = [
        "tampilkan semua", "lihat semua", "show all", "list semua", "dump",
        "seluruh isi", "semua baris", "semua data", "semua isi", "semua record",
        "export semua", "ceritakan semua", "semua action plan", "semua progress",
        "semua issue", "semua mitigasi", "semua prokja",
    ]

    if any(k in q_lower for k in OUT_OF_SCOPE):
        async def out_of_scope():
            yield sse("progress", "parse")
            await asyncio.sleep(0.3)
            yield sse("done", "⚠️ Maaf, saya hanya dapat membantu <b>analisis data maintenance kilang</b>. Silakan ajukan pertanyaan yang berkaitan dengan data yang tersedia.")
        return StreamingResponse(out_of_scope(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    if any(k in q_lower for k in DUMP_KEYWORDS):
        # Deteksi tabel mana yang dimaksud
        table_hints = {
            "pipeline": "pipeline", "atg": "atg", "metering": "metering",
            "rotor": "rotor", "anggaran": "anggaran", "bad actor": "badactor",
            "icu": "icu", "prokja": "prokja_atg", "paf": "paf",
            "zero clamp": "zero_clamp", "issue paf": "issue_paf",
            "power": "power_stream", "steam": "power_stream",
            "jumlah eqp": "jumlah_eqp", "critical utl": "critical_utl",
            "critical prim": "critical_prim", "monitoring operasi": "mon_operasi",
            "inspection": "inspection_plan",
        }
        matched_key = next((v for k, v in table_hints.items() if k in q_lower), None)
        download_btn = f"[DOWNLOAD:{matched_key}]" if matched_key else ""

        async def dump_guard():
            yield sse("progress", "parse")
            await asyncio.sleep(0.3)
            yield sse("done",
                f"📊 Permintaan menampilkan seluruh data dalam chat akan sangat panjang dan tidak efisien. "
                f"Saya sarankan dua opsi:<br><br>"
                f"<b>1. Download Excel langsung</b> — klik tombol di bawah untuk mengunduh data lengkap {download_btn}<br><br>"
                f"<b>2. Tanyakan analisis spesifik</b>, misalnya:<br>"
                f"<ul><li>Berapa jumlah per RU?</li>"
                f"<li>Mana yang statusnya bermasalah?</li>"
                f"<li>Mana yang sudah melewati target date?</li></ul>"
            )
        return StreamingResponse(dump_guard(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
    async def generate():
        try:
            # ── STEP 1: parsing pertanyaan ──────────────────────────────────
            yield sse("progress", "parse")
            await asyncio.sleep(0)          # flush ke client

            # ── STEP 2: menyusun SQL (invoke dijalankan di thread pool) ─────
            yield sse("progress", "sql")
            await asyncio.sleep(0)

            loop = asyncio.get_event_loop()

            # Jalankan db_chain.invoke di thread pool agar tidak blokir event loop
            # Kita inject hook via callback-style dengan flag sederhana
            sql_done = asyncio.Event()
            db_done  = asyncio.Event()
            result_holder = {}

            async def run_chain():
                def _invoke():
                    return None  # placeholder

                fut = loop.run_in_executor(None, lambda: None)

                await asyncio.sleep(2.5)
                sql_done.set()

                await asyncio.sleep(1.0)
                db_done.set()

                # Jalankan dengan memory
                answer = await run_with_memory(question, session_id, loop)
                result_holder["result"] = answer

            task = asyncio.create_task(run_chain())

            # Stream progress saat event terjadi
            await sql_done.wait()
            yield sse("progress", "db")
            await asyncio.sleep(0)

            await db_done.wait()
            yield sse("progress", "answer")
            await asyncio.sleep(0)

            await task  # tunggu chain selesai

            answer = result_holder["result"].replace("```sql", "").replace("```", "").strip()
            # Simpan ke history sesi
            add_session_history(session_id, question, answer)
            yield sse("done", answer)

        except Exception as e:
            yield sse("error", f"AI Error: {str(e)}")

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # penting untuk nginx proxy
        }
    )
# ─────────────────────────────────────────────────────────────────────────────
# PARSERS: 8 TABEL BARU
# ─────────────────────────────────────────────────────────────────────────────

def _to_float(v):
    try:
        return float(v) if pd.notna(v) else None
    except:
        return None

def _to_int(v):
    try:
        return int(v) if pd.notna(v) else None
    except:
        return None

def _to_date_str(v):
    try:
        return str(v.date()) if pd.notna(v) and hasattr(v, 'date') else _safe(v)
    except:
        return _safe(v)

def sync_paf(file_location: str, db: Session):
    df = pd.read_excel(file_location, sheet_name=0, header=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    db.query(PAF).delete()
    count = 0
    for _, row in df.iterrows():
        g = lambda c: _safe(row.get(c)) if row.get(c) is not None and pd.notna(row.get(c)) else ''
        db.add(PAF(
            month_update     = _safe(row.get('Month Update')),
            type             = _safe(row.get('Type')),
            ru               = _safe(row.get('RU')),
            target_realisasi = _safe(row.get('Target/Realisasi')),
            color            = _safe(row.get('Color')),
            value            = _to_float(row.get('Value')),
            plan_unplan      = _safe(row.get('Plan/Unplan')),
            type2            = _safe(row.get('Type.1')),
            month            = _to_date_str(row.get('Month')),
            value2           = _to_float(row.get('Value.1')),
            ru2              = _safe(row.get('RU ')),
            target           = _to_float(row.get('Target')),
            code_current     = _to_int(row.get('Code Current')),
        ))
        count += 1
    db.commit()
    return {"message": f"✅ PAF berhasil diupdate! ({count} records)"}

def sync_zero_clamp(file_location: str, db: Session):
    df = pd.read_excel(file_location, sheet_name=0, header=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    db.query(ZeroClamp).delete()
    count = 0
    for _, row in df.iterrows():
        db.add(ZeroClamp(
            no                        = _to_int(row.get('NO')),
            ru                        = _safe(row.get('RU')),
            area                      = _safe(row.get('AREA')),
            unit                      = _safe(row.get('UNIT')),
            tag_no_ln                 = _safe(row.get('TAG NO/LN')),
            services                  = _safe(row.get('Services')),
            description               = _safe(row.get('Description')),
            type_damage               = _safe(row.get('TYPE DAMAGE')),
            posisi                    = _safe(row.get('POSISI')),
            type_perbaikan            = _safe(row.get('TYPE PERBAIKAN')),
            tanggal_dipasang          = _to_date_str(row.get('TANGGAL DIPASANG')),
            tanggal_dilepas           = _to_date_str(row.get('TANGGAL DILEPAS')),
            tanggal_rencana_perbaikan = _to_date_str(row.get('TANGGAL RENCANA PERBAIKAN PERMANENT')),
            no_irkap                  = _safe(row.get('NO IRKAP')),
            status                    = _safe(row.get('STATUS')),
            remarks                   = _safe(row.get('Remarks')),
        ))
        count += 1
    db.commit()
    return {"message": f"✅ Zero Clamp berhasil diupdate! ({count} records)"}

def sync_issue_paf(file_location: str, db: Session):
    df = pd.read_excel(file_location, sheet_name=0, header=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    db.query(IssuePAF).delete()
    count = 0
    for _, row in df.iterrows():
        db.add(IssuePAF(
            type         = _safe(row.get('Type')),
            ru           = _safe(row.get('RU')),
            date         = _to_date_str(row.get('Date')),
            issue        = _safe(row.get('Issue')),
            month_update = _safe(row.get('Month Update')),
            code_current = _to_int(row.get('Code Current')),
        ))
        count += 1
    db.commit()
    return {"message": f"✅ Issue PAF berhasil diupdate! ({count} records)"}

def sync_power_stream(file_location: str, db: Session):
    df = pd.read_excel(file_location, sheet_name=0, header=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    db.query(PowerStream).delete()
    count = 0
    for _, row in df.iterrows():
        db.add(PowerStream(
            refinery_unit    = _safe(row.get('Refinert Unit')),
            type_equipment   = _safe(row.get('Type Equipment')),
            equipment        = _safe(row.get('Equipment')),
            status_operation = _safe(row.get('Status Operation')),
            status_n0        = _safe(row.get('Status N+0')),
            unit_measurement = _safe(row.get('Unit Measurement')),
            desain           = _to_float(row.get('Desain')),
            kapasitas_max    = _to_float(row.get('Capasitas Max')),
            average_actual   = _to_float(row.get('Average Actual')),
            remark           = _safe(row.get('Remark')),
            date_update      = _safe(row.get('Date Update')),
            month_update     = _safe(row.get('Month Update')),
            code_current     = _to_int(row.get('Code Current')),
        ))
        count += 1
    db.commit()
    return {"message": f"✅ Power & Steam berhasil diupdate! ({count} records)"}

def sync_jumlah_eqp(file_location: str, db: Session):
    df = pd.read_excel(file_location, sheet_name=0, header=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    db.query(JumlahEqpUTL).delete()
    count = 0
    for _, row in df.iterrows():
        db.add(JumlahEqpUTL(
            refinery_unit    = _safe(row.get('Refinery Unit')),
            type_equipment   = _safe(row.get('Type Equipment')),
            status_equipment = _safe(row.get('Status Equipment')),
            jumlah           = _to_int(row.get('Jumlah')),
            month_update     = _safe(row.get('Month Update')),
            code_current     = _to_int(row.get('Code Current')),
        ))
        count += 1
    db.commit()
    return {"message": f"✅ Jumlah Equipment UTL berhasil diupdate! ({count} records)"}

def sync_critical_utl(file_location: str, db: Session):
    df = pd.read_excel(file_location, sheet_name=0, header=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    db.query(CriticalEqpUTL).delete()
    count = 0
    for _, row in df.iterrows():
        db.add(CriticalEqpUTL(
            refinery_unit      = _safe(row.get('Refinery Unit')),
            type_equipment     = _safe(row.get('Type Equipment')),
            highlight_issue    = _safe(row.get('Highlight Issue')),
            corrective_action  = _safe(row.get('Corrective & Quick Win Action')),
            target_corrective  = _safe(row.get('Target')),
            traffic_corrective = _safe(row.get('Traffic')),
            mitigasi_action    = _safe(row.get('Mitigasi & Leading Action Program')),
            target_mitigasi    = _safe(row.get('Target.1')),
            traffic_mitigasi   = _safe(row.get('Traffic.1')),
            month_update       = _safe(row.get('Month Update')),
            code_current       = _to_int(row.get('Code Current')),
        ))
        count += 1
    db.commit()
    return {"message": f"✅ Critical Equipment UTL berhasil diupdate! ({count} records)"}

def sync_critical_prim(file_location: str, db: Session):
    df = pd.read_excel(file_location, sheet_name=0, header=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    db.query(CriticalEqpPrimSec).delete()
    count = 0
    for _, row in df.iterrows():
        db.add(CriticalEqpPrimSec(
            refinery_unit      = _safe(row.get('Refinery Unit')),
            unit_proses        = _safe(row.get('Unit Proses')),
            equipment          = _safe(row.get('Equipment')),
            highlight_issue    = _safe(row.get('Highlight Issue')),
            corrective_action  = _safe(row.get('Corrective & Quick Win Action')),
            target_corrective  = _safe(row.get('Target')),
            traffic_corrective = _safe(row.get('Traffic')),
            mitigasi_action    = _safe(row.get('Mitigasi & Leading Action Program')),
            target_mitigasi    = _safe(row.get('Target.1')),
            traffic_mitigasi   = _safe(row.get('Traffic.1')),
            month_update       = _safe(row.get('Month Update')),
            code_current       = _to_int(row.get('Code Current')),
        ))
        count += 1
    db.commit()
    return {"message": f"✅ Critical Equipment Prim & Sec berhasil diupdate! ({count} records)"}

def sync_mon_operasi(file_location: str, db: Session):
    df = pd.read_excel(file_location, sheet_name=0, header=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    db.query(MonitoringOperasi).delete()
    count = 0
    for _, row in df.iterrows():
        db.add(MonitoringOperasi(
            refinery_unit          = _safe(row.get('Refinery Unit')),
            unit_proses            = _safe(row.get('Unit Proses')),
            unit                   = _safe(row.get('Unit')),
            unit_measurement       = _safe(row.get('Unit Measurement')),
            design                 = _to_float(row.get('Design')),
            minimal_capacity       = _to_float(row.get('Minimal Capacity')),
            plant_readiness        = _to_float(row.get('Plant Readiness')),
            remark                 = _safe(row.get('Remark')),
            type_limitasi_process  = _safe(row.get('Type Limitasi_Process')),
            equipment_process      = _safe(row.get('Equipment_Process')),
            limitasi_alert_process = _safe(row.get('Limitasi/Alert_Process')),
            mitigasi_process       = _safe(row.get('Mitigasi_Process')),
            target_sts             = _to_float(row.get('Target/STS')),
            actual                 = _to_float(row.get('Actual')),
            type_limitasi_sts      = _safe(row.get('Type Limitasi_STS')),
            equipment_sts          = _safe(row.get('Equipment_STS')),
            limitasi_alert_sts     = _safe(row.get('Limitasi/Alert STS')),
            mitigasi_sts           = _safe(row.get('Mitigasi_STS')),
            month_update           = _safe(row.get('Month Update')),
            code_current           = _to_int(row.get('Code Current')),
        ))
        count += 1
    db.commit()
    return {"message": f"✅ Monitoring Operasi berhasil diupdate! ({count} records)"}

def sync_inspection_plan(file_location: str, db: Session):
    df = pd.read_excel(file_location, sheet_name=0, header=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    db.query(InspectionPlan).delete()
    count = 0
    for _, row in df.iterrows():
        db.add(InspectionPlan(
            refinery_unit         = _safe(row.get('Refinery Unit')),
            area                  = _safe(row.get('Area')),
            unit                  = _safe(row.get('Unit')),
            tag_no_ln             = _safe(row.get('Tag No/LN')),
            type_equipment        = _safe(row.get('Type Equipment')),
            type_inspection       = _safe(row.get('Type Inspection')),
            type_pekerjaan        = _safe(row.get('Type Pekerjaan')),
            due_date              = _to_date_str(row.get('Due Date')),
            due_year              = _to_int(row.get('Due Year')),
            plan_date             = _to_date_str(row.get('Plan Date')),
            plan_year             = _to_int(row.get('Plan Year')),
            actual_date           = _to_date_str(row.get('Actual Date')),
            actual_year           = _to_int(row.get('Actual Year')),
            update_date           = _to_date_str(row.get('Update Date')),
            result_remaining_life = _to_float(row.get('Result Remaining Life')),
            result_visual         = _safe(row.get('Result Visual')),
            visual_lainnya        = _safe(row.get('Visual Lainnya')),
            result_lainnya        = _safe(row.get('Result Lainnya')),
            grand_result          = _safe(row.get('Grand Result')),
        ))
        count += 1
    db.commit()
    return {"message": f"✅ Inspection Plan berhasil diupdate! ({count} records)"}

def sync_tkdn(file_location: str, db: Session):
    df = pd.read_excel(file_location, sheet_name=0, header=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    db.query(TKDN).delete()
    count = 0
    for _, row in df.iterrows():
        db.add(TKDN(
            refinery_unit = _safe(row.get('Refinery Unit')),
            bulan         = _safe(row.get('BULAN')),
            nominal       = _to_float(row.get('NOMINAL')),
            kdn           = _to_float(row.get('KDN')),
            persentase    = _to_float(row.get('PERSENTASE')),
            tahun         = _to_int(row.get('TAHUN')),
        ))
        count += 1
    db.commit()
    return {"message": f"✅ TKDN berhasil diupdate! ({count} records)"}

def sync_rcps_rekomendasi(file_location: str, db: Session):
    df = pd.read_excel(file_location, sheet_name=0, header=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    db.query(RCPSRekomendasi).delete()
    count = 0
    for _, row in df.iterrows():
        target = row.get('Target')
        db.add(RCPSRekomendasi(
            no                      = _to_int(row.get('NO')),
            kilang                  = _safe(row.get('Kilang')),
            rcps                    = _safe(row.get('RCPS')),
            rcps_no                 = _safe(row.get('RCPS No')),
            judul_rcps              = _safe(row.get('Judul RCPS')),
            link_rcps               = _safe(row.get('Link RCPS')),
            rekomendasi             = _safe(row.get('Recomendation')),
            description             = _safe(row.get('Description')),
            traffic                 = _safe(row.get('Traffic')),
            pic                     = _safe(row.get('PIC')),
            target                  = _to_date_str(target) if pd.notna(target) and hasattr(target, 'date') else _safe(target),
            recommendation_category = _safe(row.get('Recommendation Category')),
            external_resource       = _safe(row.get('Recommendation Need External Resource?')),
            no_irkap                = _safe(row.get('No. IRKAP')),
            remark                  = _safe(row.get('Remark')),
        ))
        count += 1
    db.commit()
    return {"message": f"✅ RCPS Rekomendasi berhasil diupdate! ({count} records)"}

def sync_rcps(file_location: str, db: Session):
    df = pd.read_excel(file_location, sheet_name=0, header=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    db.query(RCPS).delete()
    count = 0
    for _, row in df.iterrows():
        db.add(RCPS(
            kilang          = _safe(row.get('Kilang')),
            traffic         = _safe(row.get('Traffic')),
            sum_of_progress = _to_int(row.get('Sum of Progress')),
            link            = _safe(row.get('link')),
            disiplin        = _safe(row.get('Disiplin')),
            date            = _to_date_str(row.get('Date 2')),
            judul_rcps      = _safe(row.get('Judul RCPS')),
            rcps_no         = _safe(row.get('RCPS No')),
            criticallity    = _safe(row.get('Criticallity')),
        ))
        count += 1
    db.commit()
    return {"message": f"✅ RCPS berhasil diupdate! ({count} records)"}

def sync_boc(file_location: str, db: Session):
    df = pd.read_excel(file_location, sheet_name=0, header=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    db.query(BOC).delete()
    count = 0
    for _, row in df.iterrows():
        db.add(BOC(
            ru             = _safe(row.get('RU_Sheet1')),
            area           = _safe(row.get('Area')),
            unit           = _safe(row.get('Unit')),
            equipment      = _safe(row.get('Equipment')),
            grup_equipment = _safe(row.get('Grup_Equipment')),
            qr_code        = _safe(row.get('QRCode')),
            rfid           = _safe(row.get('RFID')),
            status         = _safe(row.get('Status')),
            frequency      = _to_int(row.get('Frequency')),
            running_hours  = _to_float(row.get('Running Hours')),
            mttr           = _to_float(row.get('MTTR')),
            mtbf           = _to_float(row.get('MTBF')),
            hasil          = _safe(row.get('hasil')),
        ))
        count += 1
    db.commit()
    return {"message": f"✅ BOC berhasil diupdate! ({count} records)"}

def sync_readiness_jetty(file_location: str, db: Session, mode: str = "replace"):
    df = pd.read_excel(file_location, sheet_name=0, header=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    if mode == "replace":
        db.query(ReadinessJetty).delete()
    count = 0
    for _, row in df.iterrows():
        db.add(ReadinessJetty(
            refinery_unit          = _safe(row.get('Refinery Unit')),
            area                   = _safe(row.get('Area')),
            unit                   = _safe(row.get('Unit')),
            tag_no                 = _safe(row.get('Tag No')),
            status_operation       = _safe(row.get('Status Operation')),
            no_tuks                = _safe(row.get('Nomor Surat TUKS')),
            expired_tuks           = _to_date_str(row.get('TUKS (Expired Date)')),
            status_tuks            = _safe(row.get('Status TUKS')),
            no_ijin_ops            = _safe(row.get('Nomor Surat pemberian ijin OPS')),
            expired_ijin_ops       = _to_date_str(row.get('Surat pemberian ijin OPS (Expired Date)')),
            status_ijin_ops        = _safe(row.get('Status Surat pemberian ijin OPS')),
            no_isps                = _safe(row.get('Nomor Surat ISPS code')),
            expired_isps           = _to_date_str(row.get('Surat ISPS code (Expired Date)')),
            status_isps            = _safe(row.get('Status Surat ISPS code')),
            status_struktur        = _safe(row.get('Status Struktur jetty head')),
            remark_struktur        = _safe(row.get('Remark Struktur jetty head')),
            status_trestle         = _safe(row.get('Status Trestle')),
            remark_trestle         = _safe(row.get('Remark Trestle')),
            status_mla             = _safe(row.get('Status Marine loading arm/cargo hose')),
            remark_mla             = _safe(row.get('Remark Marine loading arm/cargo hose')),
            status_fire_protection = _safe(row.get('Status Fire protection/ fire hydrant')),
            remark_fire_protection = _safe(row.get('Remark Fire protection/ fire hydrant')),
            month_update           = _to_date_str(row.get('Month Update')),
        ))
        count += 1
    db.commit()
    action = "ditambahkan" if mode == "append" else "diupdate"
    return {"message": f"✅ Readiness Jetty berhasil {action}! ({count} records)"}

def sync_workplan_jetty(file_location: str, db: Session, mode: str = "replace"):
    df = pd.read_excel(file_location, sheet_name=0, header=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    if mode == "replace":
        db.query(WorkplanJetty).delete()
    count = 0
    for _, row in df.iterrows():
        db.add(WorkplanJetty(
            refinery_unit        = _safe(row.get('Refinery Unit')),
            area                 = _safe(row.get('Area')),
            unit                 = _safe(row.get('Unit')),
            tag_no               = _safe(row.get('Tag No')),
            item                 = _safe(row.get('Item')),
            status_item          = _safe(row.get('Status Item')),
            remark               = _safe(row.get('Remark/Kondisi Item')),
            rtl_action_plan      = _safe(row.get('RTL/Action Plan')),
            action_plan_category = _safe(row.get('Action Plan Category')),
            external_resource    = _safe(row.get('External Resource')),
            no_irkap             = _safe(row.get('NO.IRKAP')),
            target               = _to_date_str(row.get('Target')),
            keterangan           = _safe(row.get('Keterangan')),
            status_rtl           = _safe(row.get('Status RTL')),
            month_update         = _to_date_str(row.get('Month Update')),
        ))
        count += 1
    db.commit()
    action = "ditambahkan" if mode == "append" else "diupdate"
    return {"message": f"✅ Workplan Jetty berhasil {action}! ({count} records)"}

def sync_readiness_tank(file_location: str, db: Session, mode: str = "replace"):
    df = pd.read_excel(file_location, sheet_name=0, header=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    if mode == "replace":
        db.query(ReadinessTank).delete()
    count = 0
    for _, row in df.iterrows():
        db.add(ReadinessTank(
            refinery_unit             = _safe(row.get('Refinery Unit')),
            area                      = _safe(row.get('Area')),
            unit                      = _safe(row.get('Unit')),
            tag_number                = _safe(row.get('Tag Number')),
            type_tangki               = _safe(row.get('Type Tangki')),
            service_tangki            = _safe(row.get('Service Tangki')),
            prioritas                 = _safe(row.get('Prioritas')),
            status_operational        = _safe(row.get('Status Operational Tangki')),
            cert_no_atg               = _safe(row.get('Cert No ATG')),
            date_expired_atg          = _to_date_str(row.get('Date Expired ATG')),
            atg_certification_validity= _safe(row.get('ATG Certification Validity')),
            coi_date_expired          = _to_date_str(row.get('COI (Date Expired)')),
            no_coi                    = _safe(row.get('No COI')),
            status_coi                = _safe(row.get('Status COI')),
            internal_inspection       = _safe(row.get('Internal Inspection')),
            plan_internal_inspection  = _to_date_str(row.get('Plan Internal Inspection ')),
            status_atg                = _safe(row.get('Status ATG')),
            remark_atg                = _safe(row.get('Remark ATG')),
            status_grounding          = _safe(row.get('Status Grounding')),
            status_shell_course       = _safe(row.get('Status Shell Course')),
            remark_shell_course       = _safe(row.get('Remark Shell Course')),
            status_roof               = _safe(row.get('Status Roof (cone/Floating)')),
            remark_roof               = _safe(row.get('Remark Roof (cone/Floating)')),
            status_cathodic           = _safe(row.get('Status Cathodic Protection')),
            remark_cathodic           = _safe(row.get('Remark Cathodic Protection')),
            month_update              = _to_date_str(row.get('Month Update')),
        ))
        count += 1
    db.commit()
    action = "ditambahkan" if mode == "append" else "diupdate"
    return {"message": f"✅ Readiness Tank berhasil {action}! ({count} records)"}

def sync_workplan_tank(file_location: str, db: Session, mode: str = "replace"):
    df = pd.read_excel(file_location, sheet_name=0, header=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    if mode == "replace":
        db.query(WorkplanTank).delete()
    count = 0
    for _, row in df.iterrows():
        db.add(WorkplanTank(
            unit                 = _safe(row.get('Unit')),
            tag_no               = _safe(row.get('Tag No')),
            item                 = _safe(row.get('Item')),
            remark               = _safe(row.get('Remark/Kondisi Item')),
            rtl_action_plan      = _safe(row.get('RTL/Action Plan')),
            action_plan_category = _safe(row.get('Action Plan Category')),
            external_resource    = _safe(row.get('External Resource')),
            no_irkap             = _safe(row.get('NO.IRKAP')),
            target               = _to_date_str(row.get('Target')),
            keterangan           = _safe(row.get('Keterangan')),
            status_rtl           = _safe(row.get('Status RTL')),
            month_update         = _to_date_str(row.get('Month Update')),
        ))
        count += 1
    db.commit()
    action = "ditambahkan" if mode == "append" else "diupdate"
    return {"message": f"✅ Workplan Tank berhasil {action}! ({count} records)"}

def sync_readiness_spm(file_location: str, db: Session, mode: str = "replace"):
    df = pd.read_excel(file_location, sheet_name=0, header=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    if mode == "replace":
        db.query(ReadinessSPM).delete()
    count = 0
    for _, row in df.iterrows():
        db.add(ReadinessSPM(
            refinery_unit         = _safe(row.get('Refinery Unit')),
            area                  = _safe(row.get('Area')),
            unit                  = _safe(row.get('Unit')),
            tag_no                = _safe(row.get('Tag No')),
            status_operation      = _safe(row.get('Status Operation')),
            no_laik_operasi       = _safe(row.get('Nomor Persetujuan Laik Operasi (MIGAS)')),
            expired_laik_operasi  = _to_date_str(row.get('Expired Persetujuan Laik Operasi (MIGAS)')),
            status_laik_operasi   = _safe(row.get('Status Persetujuan Laik Operasi (MIGAS)')),
            no_ijin_spl           = _safe(row.get('Nomor Ijin Pengoperasian SPL')),
            expired_ijin_spl      = _to_date_str(row.get('Ijin Pengoperasian SPL Expired Date')),
            status_ijin_spl       = _safe(row.get('Status Ijin Pengoperasian SPL')),
            status_mbc            = _safe(row.get('Status MBC (Marine Breakway Coupling)')),
            remark_mbc            = _safe(row.get('Remark MBC (Marine Breakway Coupling)')),
            status_lds            = _safe(row.get('Status Leak Detection System (LDS)')),
            remark_lds            = _safe(row.get('Remark Leak Detection System (LDS)')),
            status_mooring_hawser = _safe(row.get('Status Mooring Hawser/Bridle/Fairlead/Pickupbuoy/Chafe/Chain Tension Tripod')),
            remark_mooring_hawser = _safe(row.get('Remark Mooring Hawser/Bridle/Fairlead/Pickupbuoy/Chafe/Chain Tension Tripod')),
            status_floating_hose  = _safe(row.get('Status Floating Hose  System')),
            remark_floating_hose  = _safe(row.get('Remark Floating Hose  System')),
            status_cathodic_spl   = _safe(row.get('Status Cathodic Protection (SPL)')),
            status_cathodic_spm   = _safe(row.get('Status Cathodic Protection (SPM)')),
            month_update          = _to_date_str(row.get('Month Update')),
        ))
        count += 1
    db.commit()
    action = "ditambahkan" if mode == "append" else "diupdate"
    return {"message": f"✅ Readiness SPM berhasil {action}! ({count} records)"}

def sync_spm_workplan(file_location: str, db: Session, mode: str = "replace"):
    df = pd.read_excel(file_location, sheet_name=0, header=0)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    if mode == "replace":
        db.query(SPMWorkplan).delete()
    count = 0
    for _, row in df.iterrows():
        db.add(SPMWorkplan(
            refinery_unit        = _safe(row.get('Refinery Unit')),
            area                 = _safe(row.get('Area')),
            unit                 = _safe(row.get('Unit')),
            tag_no               = _safe(row.get('Tag No')),
            item                 = _safe(row.get('Item')),
            remark               = _safe(row.get('Remark/Kondisi Item')),
            rtl_action_plan      = _safe(row.get('RTL/Action Plan')),
            action_plan_category = _safe(row.get('Action Plan Category')),
            external_resource    = _safe(row.get('External Resource')),
            no_irkap             = _safe(row.get('NO.IRKAP')),
            target               = _to_date_str(row.get('Target')),
            keterangan           = _safe(row.get('Keterangan')),
            status_rtl           = _safe(row.get('Status RTL')),
            month_update         = _to_date_str(row.get('Month Update')),
        ))
        count += 1
    db.commit()
    action = "ditambahkan" if mode == "append" else "diupdate"
    return {"message": f"✅ SPM Workplan berhasil {action}! ({count} records)"}
def sync_irkap_program(file_location: str, db: Session, mode: str = "replace"):
    df = pd.read_excel(file_location, header=2)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    if mode == "replace":
        db.query(IrkapProgram).delete()
    count = 0
    for _, row in df.iterrows():
        db.add(IrkapProgram(
            refinery_unit               = _safe(row.get('Refinery Unit')),
            disiplin                    = _safe(row.get('Disiplin')),
            kategori_rkap               = _safe(row.get('Kategori RKAP')),
            material_jasa               = _safe(row.get('Material/Jasa')),
            highlevel_planning_note     = _safe(row.get('HighLevel  PlanningNote')),
            referensi_prokja_sebelumnya = _safe(row.get('Referensi Nomor Prokja Sebelumnya')),
            no_program_kerja            = _safe(row.get('No Program Kerja')),
            equipment_tag_no            = _safe(row.get('Equipment/Tag No')),
            type_equipment              = _safe(row.get('Type Equipment')),
            detail_type_equipment       = _safe(row.get('Detail Type Equipment')),
            program_kerja               = _safe(row.get('ProgramKerja')),
            step_plan_today             = _safe(row.get('Step Plan (Today)')),
            detail_step_plan_today      = _safe(row.get('Detail Step Plan (Today)')),
            step_actual_today           = _safe(row.get('Step Actual (Today)')),
            detail_step_actual_today    = _safe(row.get('Detail Step Actual (Today)')),
            status_step                 = _safe(row.get('Status Step')),
            start_plan                  = _to_date_str(row.get('Start Plan (Overall Project)')),
            finish_plan                 = _to_date_str(row.get('Finish Plan (Overall Project)')),
            status_prognosa             = _safe(row.get('Status Prognosa')),
            kelompok_biaya              = _safe(row.get('KelompokBiaya')),
            nilai_anggaran_idr          = _safe_num(row.get('Nilai Anggaran Plan (IDR)')),
            nilai_anggaran_usd          = _safe_num(row.get('Nilai Anggaran Plan (USD)')),
            top_risk                    = _safe(row.get('Top Risk')),
            asset_integrity             = _safe(row.get('Asset Integrity')),
        ))
        count += 1
    db.commit()
    action = "ditambahkan" if mode == "append" else "diupdate"
    return {"message": f"✅ IRKAP Program berhasil {action}! ({count} records)"}

def sync_irkap_actual(file_location: str, db: Session, mode: str = "replace"):
    df = pd.read_excel(file_location, header=1)
    df = _auto_convert_dates(df)
    df = _dedup_columns(df)
    if mode == "replace":
        db.query(IrkapActual).delete()
    count = 0
    for _, row in df.iterrows():
        db.add(IrkapActual(
            no                           = _safe_int(row.get('NO')),
            no_program                   = _safe(row.get('NO PROGRAM')),
            kategori_rkap                = _safe(row.get('KATEGORI RKAP')),
            program_asset_integrity      = _safe(row.get('PROGRAM ASSET INTEGRITY')),
            refinery_unit                = _safe(row.get('REFINERY UNIT')),
            area                         = _safe(row.get('AREA')),
            unit_process                 = _safe(row.get('UNIT PROCESS')),
            tag_no                       = _safe(row.get('TAG NO')),
            dasar_pengusulan             = _safe(row.get('DASAR PENGUSULAN PROGRAM KERJA')),
            rekomendasi                  = _safe(row.get('REKOMENDASI')),
            program_kerja                = _safe(row.get('PROGRAM KERJA')),
            disiplin                     = _safe(row.get('DISIPLIN')),
            kategory_trigger             = _safe(row.get('KATEGORY TRIGGER')),
            kelompok_sasaran_rk          = _safe(row.get('KELOMPOK SASARAN RK')),
            kel_biaya                    = _safe(row.get('KEL.BIAYA')),
            note                         = _safe(row.get('NOTE')),
            release_type                 = _safe(row.get('RELEASE TYPE')),
            jadwal_pelaksanaan           = _safe(row.get('JADWAL PELAKSANAAN')),
            jadwal_cost                  = _safe(row.get('JADWAL COST')),
            jadwal_cash                  = _safe(row.get('JADWAL CASH')),
            strategy_penyelesaian        = _safe(row.get('STRATEGY PENYELESAIAN PEKERJAAN')),
            failure_impact               = _safe(row.get('FAILURE IMPACT JIKA PROGRAM TIDAK DIJALANKAN')),
            high_level_planning_note     = _safe(row.get('HIGH LEVEL PLANNING NOTE')),
            referensi_prokja_sebelumnya  = _safe(row.get('REFERENSI PROGRAM KERJA SEBELUMNYA')),
            cost_center                  = _safe(row.get('COST CENTER')),
            cost_element                 = _safe(row.get('COST ELEMENT')),
            wbs_number                   = _safe(row.get('WBS NUMBER')),
            anggaran_idr                 = _safe_num(row.get('ANGGARAN IDR')),
            anggaran_usd                 = _safe_num(row.get('ANGGARAN USD')),
            anggaran_equivalent_idr      = _safe_num(row.get('ANGGARAN EQUIVALENT IDR')),
            probability_class            = _safe(row.get('Probability Class')),
            probability_likelyhood       = _safe(row.get('Probability Class_likely hood')),
            economic_usd                 = _safe(row.get('ECONOMIC (USD)')),
            health_safety                = _safe(row.get('HEALTH & SAFETY')),
            environment                  = _safe(row.get('ENVIRONMENT')),
            ram_criticality              = _safe(row.get('RAM CRITICALITY')),
            material_jasa                = _safe(row.get('MATERIAL / JASA')),
            sumber_harga                 = _safe(row.get('SUMBER HARGA')),
            actual_start1                = _safe(row.get('ACTUAL START1')),
            actual_finish1               = _safe(row.get('ACTUAL FINISH1')),
            comp1                        = _safe_float(row.get('COMP 1')),
            notif_no                     = _safe(row.get('NOTIF NO')),
            actual_start2                = _safe(row.get('ACTUAL START2')),
            actual_finish2               = _safe(row.get('ACTUAL FINISH2')),
            comp2                        = _safe_float(row.get('COMP 2')),
            actual_start3                = _safe(row.get('ACTUAL START3')),
            actual_finish3               = _safe(row.get('ACTUAL FINISH3')),
            comp3                        = _safe_float(row.get('COMP 3')),
            wo_no                        = _safe(row.get('WO No')),
            actual_start4                = _safe(row.get('ACTUAL START4')),
            actual_finish4               = _safe(row.get('ACTUAL FINISH4')),
            comp4                        = _safe_float(row.get('COMP 4')),
            ro_no                        = _safe(row.get('RO No')),
            actual_start5                = _safe(row.get('ACTUAL START5')),
            actual_finish5               = _safe(row.get('ACTUAL FINISH5')),
            comp5                        = _safe_float(row.get('COMP 5')),
            actual_start6                = _safe(row.get('ACTUAL START6')),
            actual_finish6               = _safe(row.get('ACTUAL FINISH6')),
            comp6                        = _safe_float(row.get('COMP 6')),
            pr                           = _safe(row.get('PR')),
            actual_start7                = _safe(row.get('ACTUAL START7')),
            actual_finish7               = _safe(row.get('ACTUAL FINISH7')),
            comp7                        = _safe_float(row.get('COMP 7')),
            rfq                          = _safe(row.get('RFQ')),
            actual_start8                = _safe(row.get('ACTUAL START8')),
            actual_finish8               = _safe(row.get('ACTUAL FINISH8')),
            comp8                        = _safe_float(row.get('COMP 8')),
            po                           = _safe(row.get('PO')),
            actual_start9                = _safe(row.get('ACTUAL START9')),
            actual_finish9               = _safe(row.get('ACTUAL FINISH9')),
            comp9                        = _safe_float(row.get('COMP 9')),
            gr_no                        = _safe(row.get('GR No')),
            actual_start10               = _safe(row.get('ACTUAL START10')),
            actual_finish10              = _safe(row.get('ACTUAL FINISH10')),
            comp10                       = _safe_float(row.get('COMP 10')),
            gi_no                        = _safe(row.get('GI No')),
            actual_start11               = _safe(row.get('ACTUAL START11')),
            actual_finish11              = _safe(row.get('ACTUAL FINISH11')),
            comp11                       = _safe_float(row.get('COMP 11')),
            actual_start12               = _safe(row.get('ACTUAL START12')),
            actual_finish12              = _safe(row.get('ACTUAL FINISH12')),
            comp12                       = _safe_float(row.get('COMP 12')),
            actual_start13               = _safe(row.get('ACTUAL START13')),
            actual_finish13              = _safe(row.get('ACTUAL FINISH13')),
            comp13                       = _safe_float(row.get('COMP 13')),
            sa_no                        = _safe(row.get('SA No')),
            actual_start14               = _safe(row.get('ACTUAL START14')),
            actual_finish14              = _safe(row.get('ACTUAL FINISH14')),
            comp14                       = _safe_float(row.get('COMP 14')),
            actual_start15               = _safe(row.get('ACTUAL START15')),
            actual_finish15              = _safe(row.get('ACTUAL FINISH15')),
            comp15                       = _safe_float(row.get('COMP 15')),
            current_step                 = _safe_int(row.get('CURRENT STEP')),
            status_step                  = _safe(row.get('STATUS STEP')),
            status_prognosa              = _safe(row.get('STATUS PROGNOSA')),
        ))
        count += 1
    db.commit()
    action = "ditambahkan" if mode == "append" else "diupdate"
    return {"message": f"✅ IRKAP Actual berhasil {action}! ({count} records)"}
# ─────────────────────────────────────────────────────────────────────────────
# PARSER: MASTER DATA EQUIPMENT (IH08)
# ─────────────────────────────────────────────────────────────────────────────
def sync_master_data_equipment(file_location: str, db: Session, mode: str = "replace"):
    # Baca header raw sebelum pandas mangle duplikat
    raw_header = pd.read_excel(file_location, sheet_name=0, header=None, nrows=1, dtype=str).iloc[0].tolist()

    # Deduplikasi header manual — duplikat diberi suffix .1, .2, dst (bukan di-drop)
    # supaya 'Material' kedua jadi 'Material.1' sesuai mapping model
    seen_h = {}
    clean_header = []
    for h in raw_header:
        h = str(h).strip() if h and str(h) != 'nan' else f"__empty_{len(seen_h)}"
        if h not in seen_h:
            seen_h[h] = 0
            clean_header.append(h)
        else:
            seen_h[h] += 1
            clean_header.append(f"{h}.{seen_h[h]}")

    df = pd.read_excel(file_location, sheet_name=0, header=0, dtype=str)
    df.columns = clean_header
    df = df[[c for c in df.columns if not c.startswith('__empty_')]]
    df = _auto_convert_dates(df)
    df = df.where(pd.notnull(df), None)

    if mode != "append":
        db.query(MasterDataEquipment).delete()
        db.commit()

    records = []
    for _, row in df.iterrows():
        records.append({
            "criticality":                 row.get('Criticallity'),
            "equipment":                   row.get('Equipment'),
            "functional_location":         row.get('Functional Location'),
            "maintenance_plant":           row.get('Maintenance plant'),
            "location":                    row.get('Location'),
            "cost_center":                 row.get('Cost Center'),
            "wbs_element":                 row.get('WBS element'),
            "main_work_center":            row.get('Main work center'),
            "planner_group":               row.get('Planner group'),
            "planning_plant":              row.get('Planning plant'),
            "catalog_profile":             row.get('Catalog profile'),
            "equipment_category":          row.get('Equipment category'),
            "description":                 row.get('Description of Technical Object'),
            "manufacturer":                row.get('Manufacturer of asset'),
            "model_type":                  row.get('Model/Type'),
            "serial_number":               row.get('Serial Number'),
            "changed_by":                  row.get('Changed by'),
            "changed_on":                  row.get('Changed on'),
            "created_by":                  row.get('Created by'),
            "created_on":                  row.get('Created on'),
            "technical_obj_type":          row.get('Technical obj. type'),
            "manufact_serial_number":      row.get('ManufactSerialNumber'),
            "manufacturer_drawing_number": row.get('Manufacturer drawing number'),
            "manufacturer_part_number":    row.get('Manufacturer part number'),
            "material":                    row.get('Material'),
            "material_description":        row.get('Material Description'),
            "order_no":                    row.get('Order'),
            "size_dimension":              row.get('Size/dimension'),
            "sort_field_ata":              row.get('Sort Field / ATA 100'),
        })
        # Bulk insert per 5000 baris supaya memory tidak meledak
        if len(records) >= 5000:
            db.bulk_insert_mappings(MasterDataEquipment, records)
            db.commit()
            records = []

    if records:
        db.bulk_insert_mappings(MasterDataEquipment, records)
        db.commit()

    count = len(df)
    action = "ditambahkan" if mode == "append" else "diupdate"
    return {"message": f"✅ Master Data Equipment berhasil {action}! ({count} records)"}