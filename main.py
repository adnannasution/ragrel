import os
import io
import requests
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
from models import Base, AnggaranMaintenance, PipelineInspection, RotorMonitoring, ATGMonitoring, MeteringMonitoring, BadActorMonitoring, ICUMonitoring, ProgramKerjaATG, PAF, ZeroClamp, IssuePAF, PowerStream, JumlahEqpUTL, CriticalEqpUTL, CriticalEqpPrimSec, MonitoringOperasi, InspectionPlan, TKDN, RCPSRekomendasi, RCPS, BOC, ReadinessJetty, WorkplanJetty, ReadinessTank, WorkplanTank, ReadinessSPM, SPMWorkplan
from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from langchain_experimental.sql import SQLDatabaseChain
from langchain_core.prompts import PromptTemplate
from langchain_core.messages import HumanMessage, AIMessage
from dotenv import load_dotenv

load_dotenv()

# ─── PRISMA TA-ex Integration ─────────────────────────────────
PRISMA_URL      = os.getenv("PRISMA_URL", "")
CHATBOT_API_KEY = os.getenv("CHATBOT_API_KEY", "")
PRISMA_HEADERS  = {"x-chatbot-key": CHATBOT_API_KEY}

def query_prisma(sql: str) -> dict:
    """Kirim SQL ke PRISMA TA-ex, return hasil JSON."""
    if not PRISMA_URL:
        return {"ok": False, "error": "PRISMA_URL belum dikonfigurasi"}
    try:
        r = requests.post(
            f"{PRISMA_URL}/chatbot/query",
            headers=PRISMA_HEADERS,
            json={"sql": sql},
            timeout=30
        )
        return r.json()
    except Exception as e:
        return {"ok": False, "error": str(e)}
# ─────────────────────────────────────────────────────────────

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

db_engine = SQLDatabase.from_uri(DATABASE_URL, sample_rows_in_table_info=0)

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
- PENTING: Jangan pernah query SELECT * tanpa LIMIT. Selalu gunakan agregasi, filter, atau LIMIT 20.
- Jika pertanyaan meminta "tampilkan semua" data ribuan baris, buat RINGKASAN/AGREGASI saja lalu tawarkan download dengan format: [DOWNLOAD:key_tabel] — contoh: [DOWNLOAD:pipeline] atau [DOWNLOAD:atg]. Key yang tersedia: anggaran, pipeline, rotor, atg, metering, badactor, icu, prokja_atg, paf, zero_clamp, issue_paf, power_stream, jumlah_eqp, critical_utl, critical_prim, mon_operasi, inspection_plan.
- ATURAN DOWNLOAD OTOMATIS: Jika hasil query mengandung lebih dari 10 baris data, WAJIB sisipkan tag [DOWNLOAD:key_tabel] yang relevan di akhir jawaban — meskipun user tidak memintanya. Ini membantu user mengunduh data lengkap jika ingin melihat detail lebih lanjut.
- DETEKSI PERTANYAAN TIDAK PRODUKTIF: Jika user meminta salah satu dari berikut, JANGAN query — langsung tolak dengan sopan dan arahkan ke pertanyaan analisis yang lebih tepat:
  * "tampilkan semua", "list semua", "show all", "lihat semua", "ceritakan semua"
  * "tampilkan seluruh isi tabel", "dump data", "export semua"
  * Pertanyaan yang jelas akan menghasilkan ribuan baris teks panjang (action plan, progress, prokja, issue, mitigasi)
  * Pertanyaan di luar konteks maintenance kilang (cuaca, berita, pengetahuan umum, coding, dll)
  Untuk pertanyaan view massal → tawarkan [DOWNLOAD:key] dan berikan ringkasan agregasi saja.
  Untuk pertanyaan di luar konteks → jawab: "Maaf, saya hanya dapat membantu analisis data maintenance kilang."
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
- Untuk inspection_plan: rencana & realisasi inspeksi equipment — kolom refinery_unit, area, tag_no_ln, type_equipment, type_inspection, due_date, plan_date, actual_date, result_remaining_life, grand_result.
- Untuk tkdn: Tingkat Kandungan Dalam Negeri — kolom refinery_unit, bulan, nominal (IDR), kdn (IDR), persentase (%), tahun. Selalu tampilkan nominal dan kdn dengan format Rp dan pemisah ribuan.
- Untuk anggaran_maintenance: kolom ru, tahun, kategori, tipe, nilai_usd (USD). Selalu tampilkan nilai_usd dengan format USD dan pemisah ribuan, contoh: 1,234,567.89 USD.
- Untuk rcps_rekomendasi: rekomendasi dari RCPS — kolom kilang, rcps_no, judul_rcps, rekomendasi, traffic, pic, target, remark.
- Untuk rcps: daftar RCPS — kolom kilang, traffic, sum_of_progress, disiplin, judul_rcps, rcps_no, criticallity.
- Untuk boc: Basis of Comparison equipment — kolom ru, area, unit, equipment, status, frequency, running_hours, mttr, mtbf, hasil.
- Untuk readiness_jetty: kesiapan operasional jetty — kolom refinery_unit, tag_no, status_operation, status_tuks, expired_tuks, status_ijin_ops, status_isps, status_struktur, status_trestle, status_mla, status_fire_protection, month_update.
- Untuk workplan_jetty: workplan perbaikan item jetty — kolom refinery_unit, tag_no, item, status_item, remark, rtl_action_plan, target, status_rtl, month_update.
- Untuk readiness_tank: kesiapan operasional tangki — kolom refinery_unit, tag_number, type_tangki, service_tangki, prioritas, status_operational, atg_certification_validity, status_coi, status_atg, status_grounding, status_shell_course, status_roof, status_cathodic, month_update.
- Untuk workplan_tank: workplan perbaikan tangki — kolom unit, tag_no, item, remark, rtl_action_plan, target, status_rtl, month_update.
- Untuk readiness_spm: kesiapan operasional SPM — kolom refinery_unit, tag_no, status_operation, status_laik_operasi, expired_laik_operasi, status_ijin_spl, status_mbc, status_lds, status_mooring_hawser, status_floating_hose, status_cathodic_spl, month_update.
- Untuk spm_workplan: workplan perbaikan SPM — kolom refinery_unit, tag_no, item, remark, rtl_action_plan, target, status_rtl, month_update.

TABEL EKSTERNAL PRISMA TA-ex (data procurement material Turnaround):
Untuk pertanyaan tentang material TA, reservasi, PR, PO, work order turnaround — gunakan query_prisma(sql).
Tabel yang tersedia di sistem PRISMA (BUKAN di database lokal ini):
- taex_reservasi: reservasi material utama TA-ex
  kolom: plant, equipment, "order" (pakai tanda kutip!), reservno, material, material_description,
         qty_reqmts, qty_stock, pr, item, qty_pr, del, fis, ict, pg, reqmts_date, uom, res_price, res_curr
- prisma_reservasi: subset taex aktif (ict=L)
  kolom: plant, equipment, "order", material, qty_reqmts, qty_stock_onhand,
         pr_prisma, qty_pr_prisma, code_kertas_kerja
- kumpulan_summary: ringkasan kebutuhan material per kertas kerja
  kolom: material, material_description, qty_req, qty_stock, qty_pr, qty_to_pr, code_tracking
- sap_pr: Purchase Request dari SAP
  kolom: plant, pr, material, material_description, qty_pr, req_date, release_date, tracking_no
- sap_po: Purchase Order dari SAP
  kolom: plnt, purchreq (=nomor PR), material, po, po_quantity, qty_delivered, deliv_date, net_price, crcy
- work_order: Work Order dari SAP
  kolom: plant, "order", equipment, description, system_status, planner_group,
         basic_start_date, basic_finish_date, total_plan_cost, total_act_cost

STATUS PROCUREMENT (join taex + sap_po ON sap_po.purchreq = taex_reservasi.pr):
- no-pr:      pr IS NULL atau pr = ''
- pr-created: pr ada, belum ada PO
- po-created: PO ada, qty_delivered = 0
- partial:    qty_delivered > 0 tapi < po_quantity
- complete:   qty_delivered >= po_quantity

ATURAN QUERY PRISMA:
- Kolom "order" WAJIB ditulis dengan tanda kutip ganda: "order"
- Selalu gunakan LIMIT maksimal 50
- Untuk query PRISMA, generate SQL lalu panggil query_prisma(sql)
- Keyword PRISMA: turnaround, TA, material, reservasi, PR, PO, kertas kerja, work order TA
- JANGAN query tabel PRISMA ke database lokal — gunakan query_prisma()

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

# ─── SESSION MEMORY ───────────────────────────────────────────────────────────
MAX_HISTORY = 10  # max pesan per sesi (5 pasang tanya-jawab)
session_histories: dict[str, list] = {}

def get_session_history(session_id: str) -> list:
    return session_histories.get(session_id, [])

def add_session_history(session_id: str, question: str, answer: str):
    history = session_histories.get(session_id, [])
    history.append(HumanMessage(content=question))
    history.append(AIMessage(content=answer))
    if len(history) > MAX_HISTORY:
        history = history[-MAX_HISTORY:]
    session_histories[session_id] = history

def clear_session_history(session_id: str):
    session_histories.pop(session_id, None)

async def run_with_memory(question: str, session_id: str, loop) -> str:
    """Jalankan query AI dengan konteks history sesi."""
    history = get_session_history(session_id)
    table_info = db_engine.get_table_info()

    # Build messages dengan history
    messages = [{"role": "system", "content": CUSTOM_PROMPT.replace("{table_info}", table_info).replace("{input}", "")}]
    for msg in history:
        if isinstance(msg, HumanMessage):
            messages.append({"role": "user", "content": msg.content})
        elif isinstance(msg, AIMessage):
            messages.append({"role": "assistant", "content": msg.content})

    # Deteksi apakah pertanyaan tentang data PRISMA TA-ex
    PRISMA_KEYWORDS = [
        "turnaround", "ta-ex", "taex", "reservasi", "material ta",
        "purchase request", " pr ", "purchase order", " po ",
        "kertas kerja", "kumpulan summary", "work order ta",
        "belum pr", "sudah pr", "delivery material", "stock onhand",
        "sap pr", "sap po", "procurement",
    ]
    is_prisma = any(kw in question.lower() for kw in PRISMA_KEYWORDS)

    if is_prisma and PRISMA_URL:
        # ── PRISMA PATH: generate SQL → query_prisma ──
        sql_messages = messages + [{"role": "user", "content": (
            f"Berikan HANYA query SQL PostgreSQL yang valid untuk pertanyaan berikut "
            f"menggunakan tabel PRISMA TA-ex (taex_reservasi, prisma_reservasi, "
            f"kumpulan_summary, sap_pr, sap_po, work_order). "
            f"Ingat: kolom 'order' harus ditulis dengan tanda kutip ganda. "
            f"Selalu tambahkan LIMIT 50. Tanpa penjelasan, tanpa markdown.\n\nPertanyaan: {question}"
        )}]
        sql_response = await loop.run_in_executor(None, lambda: llm.invoke(sql_messages))
        sql_query = sql_response.content.replace("```sql", "").replace("```", "").strip()

        prisma_result = await loop.run_in_executor(None, lambda: query_prisma(sql_query))
        if prisma_result.get("ok"):
            db_result = f"Hasil dari PRISMA TA-ex ({prisma_result.get('rows', 0)} baris):\n{prisma_result.get('data', [])}"
        else:
            db_result = f"Gagal query PRISMA: {prisma_result.get('error', 'Unknown error')}"
    else:
        # ── LOCAL PATH: query database lokal seperti biasa ──
        # Step 1: Generate SQL dengan konteks history
        sql_messages = messages + [{"role": "user", "content": f"Berikan HANYA query SQL PostgreSQL yang valid untuk: {question}. Tanpa penjelasan, tanpa markdown."}]
        sql_response = await loop.run_in_executor(None, lambda: llm.invoke(sql_messages))
        sql_query = sql_response.content.replace("```sql", "").replace("```", "").strip()

        # Step 2: Execute SQL
        try:
            db_result = await loop.run_in_executor(None, lambda: db_engine.run(sql_query))
        except Exception as e:
            db_result = f"Query error: {str(e)}"

    # Step 3: Generate jawaban final dengan hasil query + history
    answer_messages = messages + [
        {"role": "user", "content": question},
        {"role": "user", "content": f"Hasil query SQL:\n{db_result}\n\nBerikan jawaban final dalam Bahasa Indonesia sesuai aturan format."}
    ]
    final_response = await loop.run_in_executor(None, lambda: llm.invoke(answer_messages))
    return final_response.content

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
        }
        APPEND_SUPPORTED = {
            "readiness_jetty", "workplan_jetty",
            "readiness_tank", "workplan_tank",
            "readiness_spm", "spm_workplan",
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
    df = pd.read_excel(file_location, sheet_name=0, header=0)

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

    keys = ["anggaran", "pipeline", "rotor", "atg", "metering", "badactor", "icu", "prokja_atg", "paf", "zero_clamp", "issue_paf", "power_stream", "jumlah_eqp", "critical_utl", "critical_prim", "mon_operasi", "inspection_plan", "tkdn", "rcps_rek", "rcps", "boc", "readiness_jetty", "workplan_jetty", "readiness_tank", "workplan_tank", "readiness_spm", "spm_workplan"]
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

    OUT_OF_SCOPE = [
        "cuaca", "berita", "news", "coding", "resep", "masak", "film", "musik",
        "olahraga", "politik", "saham", "crypto", "bitcoin", "translate", "terjemahkan",
        "siapa presiden", "capital of", "ibukota",
    ]
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