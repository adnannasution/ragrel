import os
import pandas as pd
from fastapi import FastAPI, Depends, Request, UploadFile, File
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session
from database import engine, get_db, DATABASE_URL
from models import Base, KPIData
from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from langchain_experimental.sql import SQLDatabaseChain
from langchain.prompts import PromptTemplate
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
templates = Jinja2Templates(directory="templates")

@app.on_event("startup")
def startup():
    # Auto-migrate tabel saat aplikasi dijalankan di Railway
    Base.metadata.create_all(bind=engine)

# Konfigurasi LLM via Dinoiki
llm = ChatOpenAI(
    model="gpt-4o",
    openai_api_key=os.getenv("DINOIKI_API_KEY"),
    base_url="https://ai.dinoiki.com/v1",
    temperature=0.7
)

# Menghubungkan LangChain ke database PostgreSQL Railway
db_engine = SQLDatabase.from_uri(DATABASE_URL)

# CUSTOM PROMPT: Untuk memaksa AI hanya memberikan SQL murni tanpa backtick markdown
QUERY_PROMPT = """Berikan query SQL PostgreSQL yang valid untuk menjawab pertanyaan user. 
HANYA berikan kode SQL saja, tanpa tanda kutip backtick (```), tanpa kata 'sql', dan tanpa penjelasan apapun.

Struktur Tabel: {table_info}
Pertanyaan: {input}"""

prompt_template = PromptTemplate(
    input_variables=["input", "table_info"],
    template=QUERY_PROMPT
)

# Inisialisasi Chain dengan Prompt Khusus
db_chain = SQLDatabaseChain.from_llm(
    llm, 
    db_engine, 
    prompt=prompt_template,
    verbose=True
)

@app.get("/", response_class=HTMLResponse)
async def chat_ui(request: Request):
    # Perbaikan parameter request untuk Jinja2 (Starlette standar)
    return templates.TemplateResponse(
        request=request, 
        name="chatbot.html"
    )

@app.post("/upload-sync")
async def upload_sync(file: UploadFile = File(...), db: Session = Depends(get_db)):
    file_location = f"temp_{file.filename}"
    with open(file_location, "wb") as f:
        f.write(await file.read())
    
    try:
        df = pd.read_excel(file_location)
        # Hapus data lama agar sinkronisasi bersih
        db.query(KPIData).delete()
        
        for _, row in df.iterrows():
            item = KPIData(
                id_kpi=str(row['ID_KPI']),
                ru=str(row['RU (Refinery Unit)']),
                area=str(row['Area']),
                kategori=str(row['Kategori_Maintenance']),
                nama_kpi=str(row['Nama_KPI']),
                deskripsi=str(row['Deskripsi_Indikator']),
                satuan=str(row['Satuan']),
                target=float(row['Target_Tahunan']),
                realisasi=float(row['Realisasi_YTD']),
                pencapaian=str(row['Pencapaian_Persen']),
                status=str(row['Status']),
                analisis=str(row['Analisis_Penyebab']),
                rekomendasi=str(row['Rekomendasi_Tindakan'])
            )
            db.add(item)
        
        db.commit()
        if os.path.exists(file_location):
            os.remove(file_location)
        return {"message": "Data Berhasil Diupdate ke PostgreSQL!"}
    
    except Exception as e:
        db.rollback()
        if os.path.exists(file_location):
            os.remove(file_location)
        return {"error": f"Gagal proses Excel: {str(e)}"}

@app.get("/ask")
async def ask_ai(question: str):
    try:
        # Menjalankan RAG Text-to-SQL
        response = db_chain.invoke({"query": question})
        
        # Pembersihan tambahan (Antisipasi jika LLM masih bandel pakai backtick)
        answer = response["result"].replace("```sql", "").replace("```", "").strip()
        
        return {"answer": answer}
    except Exception as e:
        return {"error": f"AI Error: {str(e)}"}