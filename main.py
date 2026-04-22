import os
import json
import asyncio
import pandas as pd
from datetime import datetime
from fastapi import FastAPI, Depends, Request, UploadFile, File, Form
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, StreamingResponse
from sqlalchemy.orm import Session
from database import engine, get_db, DATABASE_URL
from models import Base, AnggaranMaintenance, PipelineInspection, RotorMonitoring, ATGMonitoring, MeteringMonitoring, BadActorMonitoring, ICUMonitoring, ProgramKerjaATG, PAF, ZeroClamp, IssuePAF, PowerStream, JumlahEqpUTL, CriticalEqpUTL, CriticalEqpPrimSec, MonitoringOperasi
from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from langchain_experimental.sql import SQLDatabaseChain
from langchain_core.prompts import PromptTemplate
from dotenv import load_dotenv

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

app = FastAPI()
templates = Jinja2Templates(directory="templates")

@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)

llm = ChatOpenAI(
    model="gpt-4o",
    openai_api_key=os.getenv("DINOIKI_API_KEY"),
    base_url="https://ai.dinoiki.com/v1",
    temperature=0.7
)

db_engine = SQLDatabase.from_uri(DATABASE_URL)

CUSTOM_PROMPT = """You are a PostgreSQL expert and a helpful AI Assistant for a refinery company.
Given an input question, create a syntactically correct PostgreSQL query to run.
HANYA BERIKAN QUERY SQL MURNI, TANPA MARKDOWN ATAU BACKTICK.

Setelah mendapatkan hasil dari database, berikan jawaban akhir dalam Bahasa Indonesia yang profesional.

STRUKTUR TABEL TERSEDIA:
{table_info}

ATURAN QUERY SQL:
- Pilih tabel yang paling relevan berdasarkan nama tabel dan kolom yang tersedia.
- Jika tabel relevan kosong, jawab: "Data belum tersedia, silakan upload datanya terlebih dahulu."
- Jangan query tabel yang tidak relevan dengan pertanyaan.
- Kolom RU antar tabel mungkin berbeda format, gunakan ILIKE '%RU II%' saat JOIN.
- Selalu gunakan NULLIF(kolom_penyebut, 0) untuk menghindari division by zero.
- Gunakan ROUND(nilai::numeric, 2) untuk pembulatan.
- Jika pertanyaan melibatkan lebih dari satu tabel, gunakan JOIN yang sesuai.
- Untuk bad_actor_monitoring: kolom utama adalah ru, tag_number, status, problem, action_plan, progress, target_date.
- Untuk icu_monitoring: kolom utama adalah ru, icu_status (Medium/High/Critical/Low), tag_no, issue, mitigation, permanent_solution, progress, target_closed, report_date.
- Untuk program_kerja_atg: kolom utama adalah refinery_unit, type, atg_eksisting, program_2024, prokja (progress), action_plan_category, target, month_update.
- Untuk paf: Plant Availability Factor — kolom type, ru, target_realisasi, value (angka PAF), plan_unplan, month.
- Untuk zero_clamp: monitoring temporary repair zero clamp — kolom ru, area, unit, tag_no_ln, type_damage, type_perbaikan, status, tanggal_dipasang, tanggal_rencana_perbaikan.
- Untuk issue_paf: daftar issue yang mempengaruhi PAF — kolom type (Primary/Secondary Unit), ru, date, issue.
- Untuk power_stream: status operasi equipment power & steam — kolom refinery_unit, type_equipment, equipment, status_operation, desain, kapasitas_max, average_actual.
- Untuk jumlah_eqp_utl: jumlah equipment utility per status — kolom refinery_unit, type_equipment, status_equipment, jumlah.
- Untuk critical_eqp_utl: critical equipment utility — kolom refinery_unit, type_equipment, highlight_issue, corrective_action, mitigasi_action, target_corrective.
- Untuk critical_eqp_prim_sec: critical equipment primary & secondary — kolom refinery_unit, unit_proses, equipment, highlight_issue, corrective_action, mitigasi_action.
- Untuk monitoring_operasi: monitoring kapasitas operasi unit proses — kolom refinery_unit, unit_proses, unit, design, minimal_capacity, plant_readiness, actual, target_sts.

ATURAN FORMAT JAWABAN:
1. Gunakan narasi/list (<ul><li>) untuk data sedikit (1-3 baris).
2. Gunakan HTML <table border='1'> untuk data banyak atau perbandingan.
3. Gunakan <b>...</b> untuk angka penting, <i>...</i> untuk catatan kaki.
4. Tambahkan emoticon relevan (🏭, 💰, 📊, ✅, ⚠️, 📈, 📉, 🔧, 🛢️, 🚨, 🔴).

ATURAN GRAFIK — WAJIB DIIKUTI:
- Jika hasil query berisi data numerik yang dapat dibandingkan (anggaran per RU, jumlah equipment per status, tren per tahun, perbandingan kategori, dsb), SELALU sertakan grafik secara otomatis tanpa perlu diminta user.
- Pilih tipe grafik yang paling tepat berdasarkan jenis data:
  * bar          → perbandingan antar kategori/RU (satu grup)
  * bar cluster  → perbandingan multi-metrik per kategori (misal RKAP vs AKTUAL per RU)
  * line         → tren waktu / data berurutan
  * pie/doughnut → proporsi/distribusi (jumlah kategori ≤ 8)
  * radar        → perbandingan multi-dimensi antar entitas
  * polarArea    → distribusi dengan penekanan visual magnitude
- Format grafik:
  Single dataset:  [CHART] {{"type": "bar", "dataset_label": "Label", "labels": ["A","B"], "data": [10,20]}} [/CHART]
  Multi-dataset:   [CHART] {{"type": "bar", "labels": ["RU II","RU III"], "datasets": [{{"label": "RKAP", "data": [100,200]}}, {{"label": "AKTUAL", "data": [90,210]}}]}} [/CHART]
  Scatter plot:    [CHART] {{"type": "scatter", "datasets": [{{"label": "Label", "data": [{{"x": 10, "y": 20}}, {{"x": 15, "y": 30}}]}}]}} [/CHART]
- Scatter plot cocok untuk: korelasi antar dua variabel numerik (misal rem_life_years vs last_measured_thickness, anggaran vs jumlah bad actor per RU, dsb)

Question: {input}"""

PROMPT = PromptTemplate(
    input_variables=["input", "table_info"],
    template=CUSTOM_PROMPT
)

db_chain = SQLDatabaseChain.from_llm(
    llm, db_engine, prompt=PROMPT, verbose=True, return_direct=False
)

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
        }
        if data_type not in handlers:
            return {"error": f"Jenis data tidak dikenal: {data_type}"}
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
    ru_col_start = {
        'RU II': 1, 'RU III': 9, 'RU IV': 17,
        'RU V': 25, 'RU VI': 33, 'RU VII': 41, 'All RUs': 49,
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
        (21, 'TOTAL',       'RKAP'), (22, 'TOTAL',       'PLAN'), (23, 'TOTAL',       'AKTUAL'),
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
    df = pd.read_excel(file_location, sheet_name="Sheet1")
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
    df = pd.read_excel(file_location, sheet_name="Sheet1")
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
    df = pd.read_excel(file_location, sheet_name="Sheet1")
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
    df = pd.read_excel(file_location, sheet_name="Sheet1")
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
    df = pd.read_excel(file_location, sheet_name="Sheet1")
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

def sync_icu(file_location: str, db: Session):
    # Baca dengan header=0, lalu rename kolom duplikat ke nama aslinya (hapus suffix _0, _1, dst)
    df = pd.read_excel(file_location, sheet_name="Sheet1", header=0)

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
    df = pd.read_excel(file_location, sheet_name="Sheet1", header=0)
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

    keys = ["anggaran", "pipeline", "rotor", "atg", "metering", "badactor", "icu", "prokja_atg", "paf", "zero_clamp", "issue_paf", "power_stream", "jumlah_eqp", "critical_utl", "critical_prim", "mon_operasi"]
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
async def ask_ai(question: str):
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
                # Langchain SQLDatabaseChain: urutan internal:
                # 1. LLM generate SQL  → kita tandai sql_done setelah ~jeda kecil
                # 2. DB execute SQL    → kita tandai db_done
                # 3. LLM generate answer
                def _invoke():
                    return db_chain.invoke({"query": question})

                fut = loop.run_in_executor(None, _invoke)

                # Simulasikan sinyal intermediate yang realistis sambil nunggu hasil
                # (LangChain tidak expose hook per-step secara publik)
                await asyncio.sleep(2.5)          # estimasi LLM generate SQL selesai
                sql_done.set()

                await asyncio.sleep(1.0)          # estimasi DB query selesai
                db_done.set()

                result_holder["result"] = await fut

            task = asyncio.create_task(run_chain())

            # Stream progress saat event terjadi
            await sql_done.wait()
            yield sse("progress", "db")
            await asyncio.sleep(0)

            await db_done.wait()
            yield sse("progress", "answer")
            await asyncio.sleep(0)

            await task  # tunggu chain selesai

            raw = result_holder["result"]
            answer = raw["result"].replace("```sql", "").replace("```", "").strip()
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
    df = pd.read_excel(file_location, sheet_name="Sheet1", header=0)
    db.query(PAF).delete()
    count = 0
    for _, row in df.iterrows():
        g = lambda c: _safe(row.get(c)) if pd.notna(row.get(c)) if row.get(c) is not None else False else ''
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
    df = pd.read_excel(file_location, sheet_name="Sheet1", header=0)
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
    df = pd.read_excel(file_location, sheet_name="Sheet1", header=0)
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
    df = pd.read_excel(file_location, sheet_name="Sheet1", header=0)
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
    df = pd.read_excel(file_location, sheet_name="Sheet1", header=0)
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
    df = pd.read_excel(file_location, sheet_name="Sheet1", header=0)
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
    df = pd.read_excel(file_location, sheet_name="Sheet1", header=0)
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
    df = pd.read_excel(file_location, sheet_name="Sheet1", header=0)
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