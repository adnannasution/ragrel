"""
chatbot.py
Chatbot engine untuk ragrel — diekstrak dari main.py.
Berisi: integrasi PRISMA, generator SQL (dengan retry-on-error),
auto-deteksi kolom kategorikal & tabel yang belum terdokumentasi,
session memory, query rewriter ringan, dan orkestrator utama (run_with_memory).
"""
import os
import json
import asyncio
import requests
from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from langchain_experimental.sql import SQLDatabaseChain
from langchain_core.prompts import PromptTemplate
from langchain_core.messages import HumanMessage, AIMessage
from dotenv import load_dotenv

from database import engine, DATABASE_URL

load_dotenv()

# ─── PRISMA TA-ex Integration ─────────────────────────────────
PRISMA_URL      = os.getenv("PRISMA_URL", "")
CHATBOT_API_KEY = os.getenv("CHATBOT_API_KEY", "")
PRISMA_HEADERS  = {"x-chatbot-key": CHATBOT_API_KEY}


def fetch_prisma_schema() -> dict:
    """Fetch schema langsung dari PRISMA saat startup — sekali saja."""
    if not PRISMA_URL:
        return {}
    try:
        r = requests.get(
            f"{PRISMA_URL}/chatbot/schema",
            headers=PRISMA_HEADERS,
            timeout=15
        )
        return r.json()
    except Exception as e:
        print(f"[PRISMA] Gagal fetch schema: {e}")
        return {}


def build_prisma_schema_prompt(schema: dict) -> str:
    """
    Bangun deskripsi schema PRISMA dari response /chatbot/schema
    untuk dimasukkan ke CUSTOM_PROMPT secara otomatis.
    """
    if not schema or "tables" not in schema:
        return ""

    lines = [
        "TABEL EKSTERNAL PRISMA TA-ex (data procurement material Turnaround):",
        "Untuk pertanyaan tentang material TA, reservasi, PR, PO, work order turnaround — gunakan query_prisma(sql).",
        "Tabel tersedia di PRISMA (BUKAN di database lokal):",
    ]

    for tbl_name, tbl in schema.get("tables", {}).items():
        col_names = tbl.get("column_names", [])
        desc      = tbl.get("description", "")
        # Kolom order perlu tanda kutip — tandai
        cols_display = []
        for c in col_names:
            if c == "order":
                cols_display.append('"order"')
            else:
                cols_display.append(c)
        lines.append(f'- {tbl_name}: {desc}')
        lines.append(f'  kolom: {", ".join(cols_display)}')

    # Tambah join hints dan status logic dari schema
    if "join_hints" in schema:
        lines.append("")
        lines.append("JOIN HINTS:")
        for k, v in schema["join_hints"].items():
            lines.append(f"  {k}: {v}")

    if "status_logic" in schema:
        lines.append("")
        lines.append("STATUS PROCUREMENT:")
        for k, v in schema["status_logic"].items():
            lines.append(f"  {k}: {v}")

    if "important_notes" in schema:
        lines.append("")
        lines.append("CATATAN PENTING:")
        for note in schema["important_notes"]:
            lines.append(f"  - {note}")

    lines += [
        "",
        "ATURAN QUERY PRISMA:",
        '- Kolom "order" WAJIB ditulis dengan tanda kutip ganda: "order"',
        "- Selalu gunakan LIMIT maksimal 50",
        "- JANGAN query tabel PRISMA ke database lokal — gunakan query_prisma(sql)",
        '- Jika hasil data PRISMA lebih dari 10 baris, arahkan ke: <a href="https://monitoring-material-production.up.railway.app/" target="_blank">🔗 Buka Aplikasi PRISMA TA-ex</a>',
    ]

    return "\n".join(lines)


# Fetch schema PRISMA saat module load
PRISMA_SCHEMA = fetch_prisma_schema()
PRISMA_SCHEMA_PROMPT = build_prisma_schema_prompt(PRISMA_SCHEMA)

# Tabel yang ada di PRISMA (dari schema, untuk deteksi routing)
PRISMA_TABLES = set(PRISMA_SCHEMA.get("allowed_tables", [
    "taex_reservasi", "prisma_reservasi", "kumpulan_summary",
    "sap_pr", "sap_po", "work_order"
]))


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
- ATURAN DOWNLOAD OTOMATIS: Jika hasil query mengandung lebih dari 10 baris data, WAJIB sisipkan tag [DOWNLOAD:key_tabel] yang relevan di akhir jawaban — meskipun user tidak memintanya. Ini membantu user mengunduh data lengkap jika ingin melihat detail lebih lanjut. Key yang tersedia: anggaran, pipeline, rotor, atg, metering, badactor, icu, prokja_atg, paf, zero_clamp, issue_paf, power_stream, jumlah_eqp, critical_utl, critical_prim, mon_operasi, inspection_plan, irkap_program, irkap_actual, master_equipment.
- DETEKSI PERTANYAAN TIDAK PRODUKTIF: Jika user meminta salah satu dari berikut, JANGAN query — langsung tolak dengan sopan dan arahkan ke pertanyaan analisis yang lebih tepat:
  * "tampilkan semua", "list semua", "show all", "lihat semua", "ceritakan semua"
  * "tampilkan seluruh isi tabel", "dump data", "export semua"
  * Pertanyaan yang jelas akan menghasilkan ribuan baris teks panjang (action plan, progress, prokja, issue, mitigasi)
  * Pertanyaan di luar konteks maintenance kilang (cuaca, berita, pengetahuan umum, coding, dll)
  Untuk pertanyaan view massal → tawarkan [DOWNLOAD:key] dan berikan ringkasan agregasi saja.
Untuk pertanyaan di luar konteks → jawab: "Maaf, saya hanya dapat membantu analisis data maintenance kilang."
PENGECUALIAN — tetap jawab dengan ramah untuk:
  * Sapaan umum (halo, hi, selamat pagi, dsb) → balas dengan ramah
  * Pertanyaan tentang kemampuan AI ini (apa yang bisa kamu lakukan, fitur apa saja, dsb) → jelaskan semua data yang tersedia
  * Ucapan terima kasih → balas dengan sopan
- Untuk icu_monitoring: kolom utama adalah ru, icu_status (Medium/High/Critical/Low), tag_no, issue, mitigation, permanent_solution, progress, target_closed, report_date. keterangan Medium = kuning, High = merah, Low = hijau.
- Untuk program_kerja_atg: kolom utama adalah refinery_unit, type, atg_eksisting, program_2024, prokja (progress), action_plan_category, target, month_update.
- Untuk paf: Plant Availability Factor — kolom type, ru, target_realisasi, value (angka PAF), plan_unplan, month.
- Untuk zero_clamp: monitoring temporary repair zero clamp — kolom ru, area, unit, tag_no_ln, type_damage, type_perbaikan, status, tanggal_dipasang, tanggal_rencana_perbaikan.
- Untuk issue_paf: daftar issue yang mempengaruhi PAF — kolom type (Primary/Secondary Unit), ru, date, issue, month_update. Kolom date bertipe DATE, filter gunakan: WHERE EXTRACT(MONTH FROM date) = 12 AND EXTRACT(YEAR FROM date) = 2024. Format month_update: W-I/W-II/W-III/W-IV + Bulan + Tahun, contoh: W-III Juli 2025. Filter bulan gunakan: WHERE month_update ILIKE '%Juli 2025%'. Filter minggu gunakan: WHERE month_update ILIKE 'W-III%Juli 2025%'.
- Untuk power_stream: status operasi equipment power & steam — kolom refinery_unit, type_equipment, equipment, status_operation, desain, kapasitas_max, average_actual.
- Untuk jumlah_eqp_utl: jumlah equipment utility per status — kolom refinery_unit, type_equipment, status_equipment, jumlah.
- Untuk critical_eqp_utl: critical equipment utility — kolom refinery_unit, type_equipment, highlight_issue, corrective_action, mitigasi_action, target_corrective, month_update. Format month_update: W-I/W-II/W-III/W-IV + Bulan + Tahun, contoh: W-I Desember 2024. Filter bulan gunakan: WHERE month_update ILIKE '%Desember 2024%'. Filter minggu gunakan: WHERE month_update ILIKE 'W-I%Desember 2024%'
- Untuk critical_eqp_prim_sec: critical equipment primary & secondary — kolom refinery_unit, unit_proses, equipment, highlight_issue, corrective_action, mitigasi_action, month_update. Format month_update: W-I/W-II/W-III/W-IV + Bulan + Tahun, contoh: W-I Desember 2024. Filter bulan gunakan: WHERE month_update ILIKE '%Desember 2024%'. Filter minggu gunakan: WHERE month_update ILIKE 'W-I%Desember 2024%'
- Untuk monitoring_operasi: monitoring kapasitas operasi unit proses — kolom refinery_unit, unit_proses, unit, design, minimal_capacity, plant_readiness, actual, target_sts, month_update. Format month_update: W-I/W-II/W-III/W-IV + Bulan + Tahun, contoh: W-I Desember 2024. Filter bulan gunakan: WHERE month_update ILIKE '%Desember 2024%'. Filter minggu gunakan: WHERE month_update ILIKE 'W-I%Desember 2024%'
- Untuk inspection_plan: rencana & realisasi inspeksi equipment — kolom refinery_unit, area, tag_no_ln, type_equipment, type_inspection, due_date, plan_date, actual_date, result_remaining_life, grand_result, month_update. Format month_update: W-I/W-II/W-III/W-IV + Bulan + Tahun, contoh: W-I Desember 2024. Filter bulan gunakan: WHERE month_update ILIKE '%Desember 2024%'. Filter minggu gunakan: WHERE month_update ILIKE 'W-I%Desember 2024%'
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
- Untuk irkap_program: daftar program kerja IRKAP 2024. KOLOM YANG TERSEDIA (gunakan HANYA nama kolom ini, jangan tambah kolom lain): refinery_unit, disiplin, kategori_rkap, material_jasa, highlevel_planning_note, referensi_prokja_sebelumnya, no_program_kerja, equipment_tag_no, type_equipment, detail_type_equipment, program_kerja, step_plan_today, detail_step_plan_today, step_actual_today, detail_step_actual_today, status_step, start_plan, finish_plan, status_prognosa, kelompok_biaya, nilai_anggaran_idr, nilai_anggaran_usd, top_risk, asset_integrity. TIDAK ADA kolom month_update, bulan, tahun, atau year di tabel ini — jangan generate kolom tersebut. Untuk filter tahun gunakan YEAR(start_plan) atau STRFTIME(\'%Y\', start_plan). Tampilkan nilai_anggaran_idr dengan format Rp.
- Untuk irkap_actual: realisasi step pelaksanaan IRKAP. KOLOM YANG TERSEDIA (gunakan HANYA nama kolom ini): no, no_program, kategori_rkap, program_asset_integrity, refinery_unit, area, unit_process, tag_no, dasar_pengusulan, rekomendasi, program_kerja, disiplin, kategory_trigger, kelompok_sasaran_rk, kel_biaya, note, release_type, jadwal_pelaksanaan, jadwal_cost, jadwal_cash, strategy_penyelesaian, failure_impact, high_level_planning_note, referensi_prokja_sebelumnya, cost_center, cost_element, wbs_number, anggaran_idr, anggaran_usd, anggaran_equivalent_idr, probability_class, probability_likelyhood, economic_usd, health_safety, environment, ram_criticality, material_jasa, sumber_harga, actual_start1, actual_finish1, comp1, notif_no, actual_start2, actual_finish2, comp2, actual_start3, actual_finish3, comp3, wo_no, actual_start4, actual_finish4, comp4, ro_no, actual_start5, actual_finish5, comp5, actual_start6, actual_finish6, comp6, pr, actual_start7, actual_finish7, comp7, rfq, actual_start8, actual_finish8, comp8, po, actual_start9, actual_finish9, comp9, gr_no, actual_start10, actual_finish10, comp10, gi_no, actual_start11, actual_finish11, comp11, actual_start12, actual_finish12, comp12, actual_start13, actual_finish13, comp13, sa_no, actual_start14, actual_finish14, comp14, actual_start15, actual_finish15, comp15, current_step, status_step, status_prognosa. TIDAK ADA kolom month_update, bulan, tahun di tabel ini. Gunakan status_prognosa ('On Fiscal Year', 'Next Year', 'Closed') dan current_step untuk analisis progres.
- Untuk master_data_equipment: master data equipment dari SAP IH08 — berisi semua equipment yang terdaftar di sistem. KOLOM YANG TERSEDIA: criticality (A/B/C/Z — tingkat kritikal equipment), equipment (nomor equipment SAP), functional_location, maintenance_plant, location (kode RU/lokasi), cost_center, wbs_element, main_work_center, planner_group, planning_plant, catalog_profile, equipment_category, description (deskripsi teknis equipment), manufacturer, model_type, serial_number, changed_by, changed_on, created_by, created_on, technical_obj_type, manufact_serial_number, manufacturer_drawing_number, manufacturer_part_number, material, material_description, order_no, size_dimension, sort_field_ata. Contoh query: jumlah equipment per criticality, list equipment berdasarkan functional_location, cari equipment by description atau manufacturer. Untuk filter criticality gunakan: WHERE criticality = 'A'. Untuk download massal gunakan [DOWNLOAD:master_equipment].

{prisma_schema}

ATURAN KLARIFIKASI — WAJIB DIIKUTI:
- WAJIB tanya klarifikasi jika pertanyaan tidak menyebut nama tabel/data spesifik secara eksplisit.
- Kata-kata berikut SAJA tanpa nama tabel spesifik = AMBIGU = WAJIB tanya dulu:
  "laporan", "data", "status", "berapa", "tampilkan", "jumlah", "lihat", "info"
- CATATAN: Kata "ru", "refinery unit", "kilang" BUKAN nama tabel spesifik — itu hanya
  filter/parameter. Jika pertanyaan hanya menyebut "ru" atau "refinery unit" tanpa nama
  tabel → tetap AMBIGU → WAJIB tanya klarifikasi.
- Nama tabel spesifik yang diakui: Pipeline, ATG, Metering, Rotor, ICU, Bad Actor, PAF,
  Zero Clamp, Power Stream, Anggaran, TKDN, RCPS, BOC, Readiness Jetty, Readiness Tank,
  Readiness SPM, Workplan Jetty, Workplan Tank, SPM Workplan, Inspection Plan,
  Monitoring Operasi, IRKAP, IRKAP Program, IRKAP Actual, Master Data Equipment, Equipment Master,
  master data, reservasi, PR, PO, material TA (PRISMA).
- DISAMBIGUASI kata "equipment":
  Jika user menyebut "equipment" BERSAMAAN dengan nama tabel lain (ICU, Pipeline, Bad Actor, dll)
  → gunakan tabel tersebut, bukan master_data_equipment.
  Contoh: "equipment di ICU" → query icu_monitoring.
  Jika user menyebut "equipment" atau "master data" TANPA nama tabel lain
  → gunakan master_data_equipment.
  Contoh: "berapa total equipment", "equipment criticality A" → query master_data_equipment.
- Jika tidak ada satupun nama tabel di atas disebut → STOP TOTAL,
  JANGAN BUAT SQL QUERY APAPUN, langsung balas dengan 1 kalimat santai saja.
  Contoh balasan: "Laporan apa yang kamu maksud? 😊 Pipeline, ATG, Metering, Rotor, ICU, Master Data Equipment, atau yang lain?"
- Jika terjadi error saat query → JANGAN ceritakan error teknis ke user.
  Cukup balas: "Hmm, sepertinya pertanyaannya kurang spesifik 😊 Laporan apa yang kamu maksud?
  Pipeline, ATG, Metering, Rotor, ICU, Master Data Equipment, atau yang lain?"
- DILARANG mencoba query lalu cerita error ke user.
- DILARANG menulis paragraf panjang untuk klarifikasi.
- Cukup 1 kalimat tanya + contoh pilihan, selesai.

{auto_schema_docs}

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

# ─── DYNAMIC CATEGORICAL VALUES (Opsi B — LLM deteksi per pertanyaan) ────────

# Schema tabel → kolom TEXT yang layak di-DISTINCT (exclude ID, tanggal, teks bebas)
# Di-generate otomatis saat startup dari DB, disimpan di sini
_DB_SCHEMA_COLS: dict = {}  # {"table": ["col1", "col2", ...]}

_SKIP_COL_KEYWORDS = [
    "id", "no", "number", "tanggal", "date", "time", "url",
    "note", "keterangan", "alamat", "deskripsi", "description",
    "ket", "remark", "comment", "kode", "code", "path", "file",
    "nama", "name", "tag", "wbs", "pr", "po", "gr", "gi", "sa",
    "notif", "wo", "ro", "rfq", "serial"
]

def _build_db_schema_cols():
    """Scan semua tabel & kolom TEXT di DB, simpan yang layak jadi contekan."""
    global _DB_SCHEMA_COLS
    from sqlalchemy import inspect as sa_inspect, Text, String
    from sqlalchemy import text

    try:
        insp = sa_inspect(engine)
        tables = insp.get_table_names()
        result = {}
        with engine.connect() as conn:
            for table in tables:
                cols = insp.get_columns(table)
                text_cols = []
                for col in cols:
                    col_name = col["name"].lower()
                    # Skip kalau nama kolom mengandung keyword terlarang
                    if any(kw in col_name for kw in _SKIP_COL_KEYWORDS):
                        continue
                    # Hanya ambil kolom TEXT/VARCHAR
                    if not isinstance(col["type"], (Text, String)):
                        continue
                    # Cek jumlah distinct — skip kalau > 100 (terlalu banyak / teks bebas)
                    try:
                        cnt = conn.execute(
                            text(f"SELECT COUNT(DISTINCT {col['name']}) FROM {table}")
                        ).scalar() or 0
                        if 1 < cnt <= 100:
                            text_cols.append(col["name"])
                    except Exception:
                        pass
                if text_cols:
                    result[table] = text_cols
        _DB_SCHEMA_COLS = result
    except Exception as e:
        print(f"[schema scan error] {e}")

async def _detect_relevant_cols(question: str) -> dict:
    """
    Panggil LLM kecil untuk deteksi tabel & kolom kategorikal
    yang relevan dengan pertanyaan. Return: {"table": ["col1", ...]}
    """
    import json, asyncio

    if not _DB_SCHEMA_COLS:
        return {}

    schema_str = "\n".join(
        f"  {tbl}: {', '.join(cols)}"
        for tbl, cols in _DB_SCHEMA_COLS.items()
    )

    detect_prompt = f"""Kamu adalah asisten yang menentukan kolom kategorikal mana yang relevan untuk sebuah pertanyaan.

Berikut daftar tabel dan kolom TEXT kategorikal yang tersedia:
{schema_str}

Pertanyaan user: "{question}"

Tentukan tabel dan kolom mana yang PALING RELEVAN dengan pertanyaan di atas.
Balas HANYA dengan JSON valid, format:
{{"tabel_nama": ["kolom1", "kolom2"], "tabel_lain": ["kolom3"]}}

Jika tidak ada yang relevan, balas: {{}}
Jangan tambahkan penjelasan apapun, hanya JSON."""

    try:
        import httpx
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{os.getenv('OPENAI_BASE_URL', 'https://ai.dinoiki.com/v1')}/chat/completions",
                headers={
                    "Authorization": f"Bearer {os.getenv('DINOIKI_API_KEY')}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content": detect_prompt}],
                    "max_tokens": 300,
                    "temperature": 0
                }
            )
            raw = resp.json()["choices"][0]["message"]["content"].strip()
            # Bersihkan kalau ada markdown fence
            raw = raw.replace("```json", "").replace("```", "").strip()
            return json.loads(raw)
    except Exception as e:
        print(f"[detect_relevant_cols error] {e}")
        return {}

async def _fetch_dynamic_categorical(question: str) -> str:
    """
    Per pertanyaan: deteksi tabel/kolom relevan via LLM kecil,
    lalu DISTINCT hanya kolom itu, return sebagai string contekan.
    """
    from sqlalchemy import text, inspect as sa_inspect

    relevant = await _detect_relevant_cols(question)
    if not relevant:
        return ""

    try:
        insp = sa_inspect(engine)
        existing = set(insp.get_table_names())
        lines = ["\n\n=== NILAI KATEGORIKAL AKTUAL DI DATABASE ===",
                 "Gunakan nilai-nilai berikut secara EXACT (case-sensitive) dalam SQL query:\n"]

        with engine.connect() as conn:
            for table, cols in relevant.items():
                if table not in existing:
                    continue
                lines.append(f"[{table}]")
                for col in cols:
                    try:
                        result = conn.execute(
                            text(f"SELECT DISTINCT {col} FROM {table} "
                                 f"WHERE {col} IS NOT NULL ORDER BY {col} LIMIT 50")
                        )
                        vals = [str(r[0]).strip() for r in result if r[0]]
                        if vals:
                            lines.append(f"  {col}: {' | '.join(vals)}")
                    except Exception:
                        pass
                lines.append("")

        return "\n".join(lines)
    except Exception as e:
        return f"\n(Gagal load categorical values: {e})"

# ─── AUTO SCHEMA DOCS — Phase A3 ──────────────────────────────────────────────
# Tabel yang SUDAH punya business-rule manual di CUSTOM_PROMPT di atas.
# Tabel real di DB yang TIDAK ada di set ini akan didokumentasikan otomatis
# (nama kolom + contoh nilai kategorikal) supaya tetap bisa di-query tanpa
# perlu menulis rule manual setiap kali ada tabel baru.
_DOCUMENTED_TABLES = {
    "anggaran_maintenance", "pipeline_inspection", "rotor_monitoring",
    "atg_monitoring", "bad_actor_monitoring", "icu_monitoring",
    "metering_monitoring", "program_kerja_atg", "paf", "zero_clamp",
    "issue_paf", "power_stream", "jumlah_eqp_utl", "critical_eqp_utl",
    "critical_eqp_prim_sec", "monitoring_operasi", "inspection_plan",
    "tkdn", "rcps_rekomendasi", "rcps", "boc", "readiness_jetty",
    "workplan_jetty", "readiness_tank", "workplan_tank", "readiness_spm",
    "spm_workplan", "irkap_program", "irkap_actual", "master_data_equipment",
}

_AUTO_SCHEMA_DOCS: str = ""

def _build_auto_schema_docs():
    """Scan tabel real di DB yang belum ada business-rule manual,
    lalu generate deskripsi kolom + contoh nilai kategorikal otomatis."""
    global _AUTO_SCHEMA_DOCS
    from sqlalchemy import inspect as sa_inspect

    try:
        insp = sa_inspect(engine)
        tables = insp.get_table_names()
        undocumented = [t for t in tables if t not in _DOCUMENTED_TABLES]
        if not undocumented:
            _AUTO_SCHEMA_DOCS = ""
            return

        lines = ["=== TABEL TAMBAHAN (auto-detected, belum ada business rule manual) ===",
                  "Tabel berikut ada di database tapi belum punya deskripsi manual — "
                  "gunakan nama kolom & contoh nilai di bawah sebagai acuan:"]
        for table in undocumented:
            cols = insp.get_columns(table)
            col_names = [c["name"] for c in cols]
            lines.append(f"\n- {table}: kolom: {', '.join(col_names)}")
            cat_cols = _DB_SCHEMA_COLS.get(table)
            if cat_cols:
                lines.append(f"  kolom kategorikal: {', '.join(cat_cols)} (lihat nilai aktual di bagian kategorikal)")
        _AUTO_SCHEMA_DOCS = "\n".join(lines)
    except Exception as e:
        print(f"[auto schema docs error] {e}")
        _AUTO_SCHEMA_DOCS = ""

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

# ─── QUERY REWRITER (ringan, opsional) — Phase A4 ─────────────────────────────
ENABLE_QUERY_REWRITE = os.getenv("ENABLE_QUERY_REWRITE", "false").lower() == "true"

_REWRITE_REFERENCE_WORDS = [
    "itu", "tadi", "tersebut", "nya", "mereka", "dia",
    "lanjut", "vs", "dibanding", "bandingkan",
]
_REWRITE_INFORMAL_WORDS = [
    "yg", "utk", "dg", "dgn", "blm", "udh", "gak", "ga ",
    "bln ini", "tahun ini", "tahun lalu", "terbaru", "terbesar",
]

def _needs_rewrite(question: str, history: list) -> bool:
    """Heuristik murah: skip LLM rewrite kalau pertanyaan sudah jelas."""
    if len(question.strip()) < 8:
        return True
    q_lower = question.lower()
    if history and any(w in q_lower.split() or w in q_lower for w in _REWRITE_REFERENCE_WORDS):
        return True
    if any(w in q_lower for w in _REWRITE_INFORMAL_WORDS):
        return True
    return False

async def _rewrite_query(question: str, history: list, loop) -> str:
    """Normalisasi pertanyaan user (typo, singkatan, referensi ke giliran sebelumnya)
    sebelum diproses sistem. Di-skip kalau ENABLE_QUERY_REWRITE=false atau pertanyaan sudah jelas."""
    if not ENABLE_QUERY_REWRITE or not _needs_rewrite(question, history):
        return question

    history_text = "\n".join([
        f"{'User' if isinstance(m, HumanMessage) else 'Bot'}: {m.content[:150]}"
        for m in history[-4:]
    ]) if history else ""

    try:
        resp = await loop.run_in_executor(None, lambda: llm.invoke([{
            "role": "user",
            "content": (
                f"Susun ulang pertanyaan berikut menjadi jelas & lengkap, perbaiki typo/singkatan, "
                f"dan selesaikan kata rujukan ('itu', 'tersebut', dst) berdasarkan riwayat percakapan. "
                f"Kembalikan HANYA pertanyaan yang sudah dinormalisasi, tanpa penjelasan apapun.\n\n"
                f"Riwayat:\n{history_text}\n\nPertanyaan: {question}"
            )
        }]))
        rewritten = resp.content.strip()
        return rewritten or question
    except Exception:
        return question

async def run_with_memory(question: str, session_id: str, loop) -> str:
    """Jalankan query AI dengan konteks history sesi."""
    history = get_session_history(session_id)

    # Layer 0: rewrite — normalisasi pertanyaan (opsional, gated by env var)
    question = await _rewrite_query(question, history, loop)

    table_info = db_engine.get_table_info()

    # Build messages dengan history
    prisma_prompt = PRISMA_SCHEMA_PROMPT or "(PRISMA schema belum tersedia — pastikan PRISMA_URL sudah dikonfigurasi)"
    categorical_ctx = await _fetch_dynamic_categorical(question)
    _prompt = (CUSTOM_PROMPT
        .replace("{table_info}", table_info)
        .replace("{prisma_schema}", prisma_prompt)
        .replace("{auto_schema_docs}", _AUTO_SCHEMA_DOCS)
        .replace("{input}", "")
        .replace("{{", "{")
        .replace("}}", "}")
    ) + categorical_ctx
    messages = [{"role": "system", "content": _prompt}]
    for msg in history:
        if isinstance(msg, HumanMessage):
            messages.append({"role": "user", "content": msg.content})
        elif isinstance(msg, AIMessage):
            messages.append({"role": "assistant", "content": msg.content})

    # ── Cek awal: Python keyword shortcut — bypass LLM untuk keyword yang pasti SPESIFIK ──
    _q_lower = question.lower()
    _SPESIFIK_KEYWORDS = [
        "pipeline", "atg", "metering", "rotor", "icu", "bad actor", "paf",
        "zero clamp", "power stream", "anggaran", "tkdn", "rcps", "boc",
        "readiness jetty", "readiness tank", "readiness spm",
        "workplan jetty", "workplan tank", "spm workplan",
        "inspection plan", "monitoring operasi",
        "irkap", "inspection", "prokja",
        "reservasi", "turnaround", "master data", "master data equipment", "equipment master",
        # ✅ Fix 1: Tambahan kata kunci bahasa Indonesia & kata follow-up
        "inspeksi", "realisasi", "bandingkan", "dibanding", "dibandingkan",
        "program kerja", "rencana inspeksi", "anggaran maintenance",
    ]
    _SAPAAN_KEYWORDS = [
        "halo", "hai", "hello", "hi ", "selamat pagi", "selamat siang",
        "selamat sore", "selamat malam", "terima kasih", "makasih", "thanks",
        "apa yang bisa", "kamu bisa apa", "kemampuan", "siapa kamu",
    ]
    if any(kw in _q_lower for kw in _SAPAAN_KEYWORDS) and not any(kw in _q_lower for kw in _SPESIFIK_KEYWORDS):
        intent = "SAPAAN"
    elif any(kw in _q_lower for kw in _SPESIFIK_KEYWORDS):
        intent = "SPESIFIK"
    else:
        # ✅ Fix 2: Sertakan history ke intent classifier agar bisa baca konteks follow-up
        history_context = ""
        if history:
            last_msgs = history[-4:]
            history_context = "\n".join([
                f"{'User' if isinstance(m, HumanMessage) else 'Bot'}: {m.content[:200]}"
                for m in last_msgs
            ])

        intent_check = await loop.run_in_executor(None, lambda: llm.invoke([{
            "role": "user",
            "content": (
                f"Konteks percakapan sebelumnya:\n{history_context}\n\n"
                f"Klasifikasikan pertanyaan berikut ke salah satu kategori:\n"
                f"1. SAPAAN — jika sapaan, terima kasih, tanya kemampuan AI, atau obrolan umum yang tidak butuh data\n"
                f"2. SPESIFIK — jika menyebut nama tabel/data berikut secara eksplisit: "
                f"Pipeline, ATG, Metering, Rotor, ICU, Bad Actor, PAF, Zero Clamp, Power Stream, "
                f"Anggaran, TKDN, RCPS, BOC, Readiness Jetty, Readiness Tank, Readiness SPM, "
                f"Workplan Jetty, Workplan Tank, SPM Workplan, Inspection Plan, Monitoring Operasi, "
                f"IRKAP, IRKAP Program, IRKAP Actual, Master Data Equipment, master data, reservasi, PR, PO, material TA, turnaround\n"
                f"3. AMBIGU — jika tidak menyebut nama tabel spesifik apapun\n"
                f"CATATAN: Kata 'ru', 'refinery unit', 'kilang', 'equipment', 'laporan', 'data', "
                f"'status', 'berapa', 'jumlah', 'tampilkan' BUKAN nama tabel — jika hanya menyebut "
                f"kata-kata itu tanpa nama tabel spesifik maka AMBIGU.\n"
                f"PENTING: Jika pertanyaan adalah follow-up (pakai kata seperti 'bandingkan', "
                f"'realisasi', 'tersebut', 'itu', 'lanjut', 'vs') dan konteks sebelumnya sudah "
                f"menyebut topik spesifik → klasifikasi SPESIFIK.\n"
                f"Jawab hanya satu kata: SAPAAN, SPESIFIK, atau AMBIGU\n\nPertanyaan: {question}"
            )
        }]))
        intent = intent_check.content.strip().upper()

    if "SAPAAN" in intent:
        greeting_response = await loop.run_in_executor(None, lambda: llm.invoke(
            messages + [{"role": "user", "content": question}]
        ))
        return greeting_response.content

    if "AMBIGU" in intent:
        # ✅ Fix 3: Konfirmasi dinamis — LLM analisis apa yang kurang lalu tanya yang relevan
        history_context = ""
        if history:
            last_msgs = history[-4:]
            history_context = "\n".join([
                f"{'User' if isinstance(m, HumanMessage) else 'Bot'}: {m.content[:200]}"
                for m in last_msgs
            ])
        clarify = await loop.run_in_executor(None, lambda: llm.invoke([{
            "role": "user",
            "content": (
                f"Riwayat percakapan:\n{history_context}\n\n"
                f"Pertanyaan user: {question}\n\n"
                f"Pertanyaan ini kurang lengkap untuk query database kilang. "
                f"Identifikasi informasi apa yang kurang (nama laporan, RU, tahun, filter, dll) "
                f"lalu buat satu kalimat tanya yang natural dan relevan dalam Bahasa Indonesia. "
                f"Jangan listing semua laporan yang ada, cukup tanyakan yang kurang saja. "
                f"Format singkat dan ramah."
            )
        }]))
        return clarify.content.strip()

    # ── Cek PRISMA via LLM ──
    prisma_table_list = ", ".join(PRISMA_TABLES) if PRISMA_TABLES else "taex_reservasi, prisma_reservasi, kumpulan_summary, sap_pr, sap_po, work_order"
    prisma_check = await loop.run_in_executor(None, lambda: llm.invoke([{
        "role": "user",
        "content": (
            f"Berdasarkan schema PRISMA TA-ex berikut:\n{PRISMA_SCHEMA_PROMPT}\n\n"
            f"PENTING: Jawab YA hanya jika pertanyaan EKSPLISIT menyebut salah satu dari: "
            f"reservasi, material TA, Purchase Request, PR, Purchase Order, PO, "
            f"work order turnaround, kertas kerja, delivery material, stock material TA. "
            f"Jika pertanyaan hanya menyebut 'laporan', 'data', 'status', 'berapa', "
            f"'tampilkan' tanpa konteks procurement/pengadaan TA → jawab TIDAK. "
            f"Apakah pertanyaan berikut berkaitan dengan data di PRISMA tersebut? "
            f"Jawab hanya YA atau TIDAK.\n\nPertanyaan: {question}"
        )
    }]))
    is_prisma = "YA" in prisma_check.content.strip().upper()

    if is_prisma and PRISMA_URL:

        # ── Deteksi jalur: SEDERHANA atau KOMPLEKS ──
        SIMPLE_PATTERNS = [
            "berapa", "total", "jumlah", "rangkuman", "ringkasan",
            "summary", "status", "berapa yang", "sudah pr", "belum pr",
            "sudah po", "belum po", "complete", "partial",
        ]
        COMPLEX_PATTERNS = [
            "per equipment", "per order", "per material", "per plant",
            "nilai po", "net price", "harga", "breakdown", "detail",
            "join", "gabungkan", "bandingkan", "lebih dari", "kurang dari",
            "terbesar", "terkecil", "tertinggi", "terendah",
        ]

        q_low = question.lower()
        is_simple = any(p in q_low for p in SIMPLE_PATTERNS)
        is_complex = any(p in q_low for p in COMPLEX_PATTERNS)

        # Kalau ada indikasi kompleks → jalur SQL
        # Kalau hanya sederhana → jalur filter langsung
        use_simple = is_simple and not is_complex

        if use_simple:
            # ── JALUR SEDERHANA: GET /chatbot/tracking ──
            params = {}
            if "belum pr" in q_low or "no-pr" in q_low or "no pr" in q_low:
                params["status"] = "no-pr"
            elif "pr created" in q_low or "sudah pr" in q_low:
                params["status"] = "pr-created"
            elif "po created" in q_low or "sudah po" in q_low:
                params["status"] = "po-created"
            elif "partial" in q_low or "sebagian" in q_low:
                params["status"] = "partial"
            elif "complete" in q_low or "selesai" in q_low or "lengkap" in q_low:
                params["status"] = "complete"

            if "rangkuman" in q_low or "ringkasan" in q_low or "summary" in q_low or "total" in q_low or "berapa" in q_low:
                params["summary_only"] = "true"

            params["chatbot_key"] = CHATBOT_API_KEY

            try:
                r = requests.get(f"{PRISMA_URL}/chatbot/tracking",
                                 params=params, timeout=30)
                prisma_result = r.json()
                print(f"[PRISMA SIMPLE] params: {params}")
                db_result = f"Hasil dari PRISMA TA-ex (jalur sederhana):\n{prisma_result}"
            except Exception as e:
                db_result = f"Gagal fetch PRISMA tracking: {str(e)}"

        else:
            # ── JALUR KOMPLEKS: POST /chatbot/query (LLM generate SQL, retry 1x jika error) ──
            def _gen_prisma_sql(previous_sql=None, previous_error=None):
                retry_ctx = ""
                if previous_error:
                    retry_ctx = (f"\nQuery sebelumnya GAGAL:\nSQL: {previous_sql}\nError: {previous_error}\n"
                                 f"Perbaiki query SQL di atas, jangan ulangi kesalahan yang sama.")
                sql_messages = messages + [{"role": "user", "content": (
                    f"Berikan HANYA query SQL PostgreSQL yang valid untuk pertanyaan berikut "
                    f"menggunakan tabel PRISMA TA-ex. "
                    f"Tabel tersedia: taex_reservasi, prisma_reservasi, kumpulan_summary, sap_pr, sap_po, work_order. "
                    f"ATURAN WAJIB:\n"
                    f"1. Kolom 'order' SELALU ditulis dengan tanda kutip ganda: \"order\"\n"
                    f"2. Selalu tambahkan LIMIT 50 di akhir query\n"
                    f"3. Untuk hitung yang sudah PR: WHERE pr IS NOT NULL AND pr != ''\n"
                    f"4. Untuk hitung yang belum PR: WHERE pr IS NULL OR pr = ''\n"
                    f"5. Untuk status PO: JOIN sap_po ON sap_po.purchreq = taex_reservasi.pr\n"
                    f"6. Gunakan COUNT(*) atau COUNT(DISTINCT ...) untuk agregasi\n"
                    f"7. HANYA output SQL murni, tanpa penjelasan, tanpa markdown, tanpa backtick\n"
                    f"{retry_ctx}\n\nPertanyaan: {question}"
                )}]
                resp = llm.invoke(sql_messages)
                return resp.content.replace("```sql", "").replace("```", "").strip()

            sql_query = await loop.run_in_executor(None, _gen_prisma_sql)
            prisma_result = await loop.run_in_executor(None, lambda: query_prisma(sql_query))

            if not prisma_result.get("ok"):
                # Retry sekali — feed error balik ke LLM
                first_error = prisma_result.get("error", "Unknown error")
                print(f"[PRISMA RETRY] attempt 1 failed. SQL: {sql_query} | Error: {first_error}")
                sql_query = await loop.run_in_executor(None, lambda: _gen_prisma_sql(sql_query, first_error))
                prisma_result = await loop.run_in_executor(None, lambda: query_prisma(sql_query))

            if prisma_result.get("ok"):
                rows = prisma_result.get('rows', 0)
                data = prisma_result.get('data', [])
                db_result = f"Hasil dari PRISMA TA-ex ({rows} baris):\n{data}"
            else:
                err = prisma_result.get('error', 'Unknown error')
                print(f"[PRISMA ERROR] SQL: {sql_query}")
                print(f"[PRISMA ERROR] Error: {err}")
                db_result = (
                    f"Query PRISMA gagal setelah retry. SQL yang dicoba: {sql_query}. "
                    f"Error: {err}. "
                    f"Coba perbaiki query atau arahkan user ke aplikasi PRISMA langsung."
                )
    else:
        # ── LOCAL PATH: query database lokal — generate SQL + retry 1x jika error ──
        def _gen_local_sql(previous_sql=None, previous_error=None):
            retry_ctx = ""
            if previous_error:
                retry_ctx = (f"\nQuery sebelumnya GAGAL:\nSQL: {previous_sql}\nError: {previous_error}\n"
                             f"Perbaiki query SQL di atas, jangan ulangi kesalahan yang sama.")
            sql_messages = messages + [{"role": "user", "content": (
                f"Berikan HANYA query SQL PostgreSQL yang valid untuk: {question}.{retry_ctx} "
                f"Tanpa penjelasan, tanpa markdown."
            )}]
            resp = llm.invoke(sql_messages)
            return resp.content.replace("```sql", "").replace("```", "").strip()

        sql_query = await loop.run_in_executor(None, _gen_local_sql)
        try:
            db_result = await loop.run_in_executor(None, lambda: db_engine.run(sql_query))
        except Exception as e:
            first_error = str(e)
            print(f"[SQL RETRY] attempt 1 failed. SQL: {sql_query} | Error: {first_error}")
            sql_query = await loop.run_in_executor(None, lambda: _gen_local_sql(sql_query, first_error))
            try:
                db_result = await loop.run_in_executor(None, lambda: db_engine.run(sql_query))
            except Exception as e2:
                db_result = f"Query error setelah retry: {str(e2)}"

    # Step 3: Generate jawaban final dengan hasil query + history
    answer_messages = messages + [
        {"role": "user", "content": question},
        {"role": "user", "content": f"Hasil query SQL:\n{db_result}\n\nBerikan jawaban final dalam Bahasa Indonesia sesuai aturan format."}
    ]
    final_response = await loop.run_in_executor(None, lambda: llm.invoke(answer_messages))
    return final_response.content
