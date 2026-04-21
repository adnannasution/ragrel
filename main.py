import os
import pandas as pd
from fastapi import FastAPI, Depends, Request, UploadFile, File
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session
from database import engine, get_db, DATABASE_URL
from models import Base, AnggaranMaintenance
from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from langchain_experimental.sql import SQLDatabaseChain
from langchain_core.prompts import PromptTemplate
from dotenv import load_dotenv

load_dotenv()

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

CUSTOM_PROMPT = """You are a PostgreSQL expert and a helpful AI Assistant specializing in maintenance budget analysis for a refinery company.
Given an input question, create a syntactically correct PostgreSQL query to run.
HANYA BERIKAN QUERY SQL MURNI, TANPA MARKDOWN ATAU BACKTICK.

Setelah mendapatkan hasil dari database, berikan jawaban akhir dalam Bahasa Indonesia yang profesional.

DATABASE TABLE: anggaran_maintenance
Kolom:
- ru          : Refinery Unit (RU II, RU III, RU IV, RU V, RU VI, RU VII, All RUs)
- tahun       : Tahun anggaran (2018 - 2025). Catatan: RU II tidak memiliki data tahun 2024.
- kategori    : Jenis maintenance (RUTIN, NON RUTIN, TURN AROUND, OVERHAUL, TOTAL)
- tipe        : Jenis angka (RKAP = anggaran resmi, PLAN = rencana, AKTUAL = realisasi)
- nilai_usd   : Nilai anggaran dalam satuan USD

CATATAN DATA:
- Gunakan tipe='TOTAL' untuk pertanyaan tentang total anggaran keseluruhan.
- Gunakan tipe='AKTUAL' untuk realisasi/serapan anggaran.
- Gunakan tipe='RKAP' untuk target/anggaran yang ditetapkan.
- 'All RUs' adalah agregat seluruh Refinery Unit — jangan dobel-count dengan RU individual.
- RU II tidak memiliki data untuk tahun 2024.

ATURAN QUERY SQL:
- Selalu gunakan NULLIF(nilai_usd, 0) pada posisi penyebut untuk menghindari division by zero.
- Untuk perhitungan persentase serapan: ROUND((SUM(aktual)/NULLIF(SUM(rkap),0)*100)::numeric, 2)
- Gunakan ROUND(nilai::numeric, 2) untuk pembulatan desimal.
- Pastikan query kompatibel dengan PostgreSQL.

ATURAN FORMAT BIJAK & RAPI:
1. PENYAJIAN DATA:
   - Gunakan format narasi/list (<ul><li>) jika data hanya sedikit (1-3 baris).
   - Gunakan format HTML <table> yang rapi dengan border='1' jika data banyak, perbandingan, atau agregasi.

2. GAYA PENULISAN:
   - Gunakan <b>...</b> untuk menebalkan angka penting atau poin utama.
   - Gunakan <i>...</i> untuk catatan kaki atau keterangan tambahan.
   - Gunakan <br> untuk memberi jarak antar paragraf.
   - Format angka USD dengan pemisah ribuan bila memungkinkan.

3. ESTETIKA:
   - Tambahkan emoticon relevan (🏭, 💰, 📊, ✅, ⚠️, 📈, 📉) di setiap poin jawaban.
   - Pastikan tabel memiliki header (<th>) yang jelas.

4. FITUR GRAFIK (CHART):
   - JIKA user meminta "chart", "grafik", atau "visualisasi", tambahkan format berikut di akhir jawaban:
     [CHART] {{ "type": "bar", "dataset_label": "Nama Label", "labels": ["A", "B"], "data": [10, 20] }} [/CHART]
   - Sesuaikan "type" (bar/line/pie), "labels", dan "data" dengan hasil query.

Table structure: {table_info}
Question: {input}"""

PROMPT = PromptTemplate(
    input_variables=["input", "table_info"],
    template=CUSTOM_PROMPT
)

db_chain = SQLDatabaseChain.from_llm(
    llm,
    db_engine,
    prompt=PROMPT,
    verbose=True,
    return_direct=False
)

@app.get("/", response_class=HTMLResponse)
async def chat_ui(request: Request):
    return templates.TemplateResponse(request=request, name="chatbot.html")

@app.post("/upload-sync")
async def upload_sync(file: UploadFile = File(...), db: Session = Depends(get_db)):
    file_location = f"temp_{file.filename}"
    with open(file_location, "wb") as f:
        f.write(await file.read())

    try:
        df = pd.read_excel(file_location, sheet_name="RU's", header=None)

        # Posisi kolom awal tiap RU (tetap, sesuai format template)
        ru_col_start = {
            'RU II': 1, 'RU III': 9, 'RU IV': 17,
            'RU V': 25, 'RU VI': 33, 'RU VII': 41, 'All RUs': 49,
        }

        # Baca tahun LANGSUNG dari baris header Excel (row index 2)
        # agar otomatis menyesuaikan jika ada perubahan tahun di file
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

        # Mapping baris Excel -> (kategori, tipe)
        row_mapping = [
            (4,  'RUTIN',       'RKAP'),  (5,  'RUTIN',       'PLAN'),  (6,  'RUTIN',       'AKTUAL'),
            (8,  'NON RUTIN',   'RKAP'),  (9,  'NON RUTIN',   'PLAN'),  (10, 'NON RUTIN',   'AKTUAL'),
            (12, 'TURN AROUND', 'RKAP'),  (13, 'TURN AROUND', 'PLAN'),  (14, 'TURN AROUND', 'AKTUAL'),
            (16, 'OVERHAUL',    'RKAP'),  (17, 'OVERHAUL',    'PLAN'),  (18, 'OVERHAUL',    'AKTUAL'),
            (21, 'TOTAL',       'RKAP'),  (22, 'TOTAL',       'PLAN'),  (23, 'TOTAL',       'AKTUAL'),
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
                        if val != val:  # NaN
                            val = None
                    except:
                        val = None
                    db.add(AnggaranMaintenance(
                        ru=ru_name, tahun=year,
                        kategori=kategori, tipe=tipe, nilai_usd=val
                    ))
                    count += 1

        db.commit()
        if os.path.exists(file_location):
            os.remove(file_location)
        return {"message": f"Data Rekap Anggaran Maintenance berhasil diupdate ke PostgreSQL! ({count} records)"}

    except Exception as e:
        db.rollback()
        if os.path.exists(file_location):
            os.remove(file_location)
        return {"error": f"Gagal proses Excel: {str(e)}"}

@app.get("/ask")
async def ask_ai(question: str):
    try:
        response = db_chain.invoke({"query": question})
        answer = response["result"].replace("```sql", "").replace("```", "").strip()
        return {"answer": answer}
    except Exception as e:
        return {"error": f"AI Error: {str(e)}"}