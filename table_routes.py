# table_routes.py — modul terpisah untuk Table Viewer
#
# Daftarkan ke main.py hanya dengan DUA baris, setelah `app = FastAPI()`:
#
#   from table_routes import router as table_router
#   app.include_router(table_router)
#
# Tidak perlu ubah apapun lagi di main.py.

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import inspect as sa_inspect, text
from database import engine, get_db

router    = APIRouter()
templates = Jinja2Templates(directory="templates")

# ── Meta tabel (key → (db_table_name, label, icon)) ──────────────────────────
TABLE_VIEWER_META = {
    "anggaran":        ("anggaran_maintenance",   "Anggaran Maintenance",   "💰"),
    "pipeline":        ("pipeline_inspection",     "Pipeline Inspection",    "🔧"),
    "rotor":           ("rotor_monitoring",        "Rotor Monitoring",       "⚙️"),
    "atg":             ("atg_monitoring",          "ATG Monitoring",         "🛢️"),
    "metering":        ("metering_monitoring",     "Metering Monitoring",    "📏"),
    "badactor":        ("bad_actor_monitoring",    "Bad Actor Monitoring",   "🚨"),
    "icu":             ("icu_monitoring",          "ICU Monitoring",         "🔴"),
    "prokja_atg":      ("program_kerja_atg",       "Program Kerja ATG",      "📋"),
    "paf":             ("paf",                     "PAF",                    "📈"),
    "zero_clamp":      ("zero_clamp",              "Zero Clamp",             "🔩"),
    "issue_paf":       ("issue_paf",               "Issue PAF",              "⚠️"),
    "power_stream":    ("power_stream",            "Power & Steam",          "⚡"),
    "jumlah_eqp":      ("jumlah_eqp_utl",          "Jumlah Equipment UTL",   "🏭"),
    "critical_utl":    ("critical_eqp_utl",        "Critical Eqp UTL",       "🚧"),
    "critical_prim":   ("critical_eqp_prim_sec",   "Critical Prim/Sec",      "🔴"),
    "mon_operasi":     ("monitoring_operasi",       "Monitoring Operasi",     "📊"),
    "inspection_plan": ("inspection_plan",          "Inspection Plan",        "🗂️"),
    "tkdn":            ("tkdn",                    "TKDN",                   "🇮🇩"),
    "rcps_rek":        ("rcps_rekomendasi",         "RCPS Rekomendasi",       "📝"),
    "rcps":            ("rcps",                    "RCPS",                   "🔍"),
    "boc":             ("boc",                     "BOC",                    "⚖️"),
    "readiness_jetty": ("readiness_jetty",          "Readiness Jetty",        "⚓"),
    "workplan_jetty":  ("workplan_jetty",           "Workplan Jetty",         "🚢"),
    "readiness_tank":  ("readiness_tank",           "Readiness Tank",         "🛢️"),
    "workplan_tank":   ("workplan_tank",            "Workplan Tank",          "🗒️"),
    "readiness_spm":   ("readiness_spm",            "Readiness SPM",          "🌊"),
    "spm_workplan":    ("spm_workplan",             "SPM Workplan",           "🔧"),
    "irkap_program":   ("irkap_program",            "IRKAP Program",          "📋"),
    "irkap_actual":    ("irkap_actual",             "IRKAP Actual",           "📊"),
    "master_equipment":("master_data_equipment",   "Master Data Equipment",  "🗂️"),
    "oa":              ("oa_monitoring",            "OA Monitoring",          "📈"),
    "plo":             ("plo_monitoring",           "PLO Monitoring",         "📜"),
}


# ── Halaman daftar semua tabel ────────────────────────────────────────────────
@router.get("/tables", response_class=HTMLResponse)
async def tables_list_page(request: Request):
    return templates.TemplateResponse(request=request, name="tables_list.html")


# ── Halaman viewer satu tabel ─────────────────────────────────────────────────
@router.get("/view/{table_key}", response_class=HTMLResponse)
async def table_viewer_page(request: Request, table_key: str):
    if table_key not in TABLE_VIEWER_META:
        raise HTTPException(status_code=404, detail=f"Tabel tidak ditemukan: {table_key}")
    db_table, label, icon = TABLE_VIEWER_META[table_key]
    return templates.TemplateResponse(
        request=request,
        name="table_viewer.html",
        context={"table_key": table_key, "table_label": label, "table_icon": icon},
    )


# ── API data server-side (pagination, search, sort) ───────────────────────────
@router.get("/api/table-data/{table_key}")
def api_table_data(
    table_key: str,
    page:      int = 1,
    per_page:  int = 25,
    search:    str = "",
    sort_col:  str = "",
    sort_dir:  str = "asc",
    db: Session = Depends(get_db),
):
    if table_key not in TABLE_VIEWER_META:
        return {"error": f"Tabel tidak ditemukan: {table_key}"}

    db_table_name = TABLE_VIEWER_META[table_key][0]

    # Periksa tabel ada di database
    insp = sa_inspect(engine)
    if db_table_name not in insp.get_table_names():
        return {"error": f"Tabel {db_table_name} belum ada di database"}

    # Ambil kolom (kecuali 'id')
    col_info  = insp.get_columns(db_table_name)
    all_cols  = [c["name"] for c in col_info if c["name"] != "id"]
    text_cols = [
        c["name"] for c in col_info
        if c["name"] != "id"
        and str(c["type"]).upper() in ("TEXT", "VARCHAR", "CHAR", "STRING")
    ]

    # Validasi sort_col
    valid_sort = sort_col if sort_col in all_cols else (all_cols[0] if all_cols else "id")
    direction  = "ASC" if sort_dir.lower() != "desc" else "DESC"

    # Build WHERE clause
    params       = {}
    where_clause = ""
    if search and text_cols:
        dialect = engine.dialect.name  # "postgresql" | "sqlite"
        op      = "ILIKE" if dialect == "postgresql" else "LIKE"
        conds   = " OR ".join([f'CAST("{c}" AS TEXT) {op} :search' for c in text_cols[:20]])
        where_clause = f"WHERE ({conds})"
        params["search"] = f"%{search}%"

    # Count
    total_count    = db.execute(text(f'SELECT COUNT(*) FROM "{db_table_name}"')).scalar() or 0
    filtered_count = db.execute(
        text(f'SELECT COUNT(*) FROM "{db_table_name}" {where_clause}'), params
    ).scalar() or 0

    # Fetch page
    per_page    = max(1, min(per_page, 1000))
    page        = max(1, page)
    offset      = (page - 1) * per_page
    total_pages = max(1, -(-filtered_count // per_page))  # ceiling division

    cols_sql = ", ".join([f'"{c}"' for c in all_cols])
    data_sql = text(
        f'SELECT {cols_sql} FROM "{db_table_name}" {where_clause} '
        f'ORDER BY "{valid_sort}" {direction} '
        f'LIMIT :limit OFFSET :offset'
    )
    rows = db.execute(data_sql, {**params, "limit": per_page, "offset": offset}).mappings().all()

    return {
        "total":       total_count,
        "filtered":    filtered_count,
        "page":        page,
        "per_page":    per_page,
        "total_pages": total_pages,
        "columns":     all_cols,
        "data":        [dict(r) for r in rows],
    }
