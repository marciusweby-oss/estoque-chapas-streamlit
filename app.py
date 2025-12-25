# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import io
import time
import plotly.express as px
import os
from fpdf import FPDF

# --- 1. CONFIGURAÇÃO DE SEGURANÇA E CONEXÃO ---

@st.cache_resource
def inicializar_firebase():
    """Inicializa a ligação ao Firebase de forma persistente."""
    try:
        if "firebase" not in st.secrets:
            return None, "ERRO: Secrets não configuradas."
        
        config = dict(st.secrets["firebase"])
        
        if "private_key" in config:
            pk = config["private_key"].replace("\\n", "\n").strip().strip('"').strip("'")
            if "-----BEGIN PRIVATE KEY-----" not in pk:
                pk = "-----BEGIN PRIVATE KEY-----\n" + pk
            if "-----END PRIVATE KEY-----" not in pk:
                pk = pk + "\n-----END PRIVATE KEY-----\n"
            config["private_key"] = pk
            
        app_name = "marcius-estoque-v30"
        
        if not firebase_admin._apps:
            cred = credentials.Certificate(config)
            firebase_admin.initialize_app(cred, name=app_name)
        
        app_inst = firebase_admin.get_app(app_name)
        return firestore.client(app=app_inst), None
    except Exception as e:
        return None, f"Erro: {str(e)}"

# Inicialização do DB
db, erro_conexao = inicializar_firebase()
PROJECT_ID = "marcius-estoque-pro-v30"

# --- 2. GESTÃO DE DADOS ---

def get_coll(nome):
    if db is None: return None
    return db.collection("artifacts").document(PROJECT_ID).collection("public").document("data").collection(nome)

@st.cache_data(ttl=300) # Cache de 5 minutos para performance
def carregar_base_mestra():
    coll = get_coll("master_csv_store")
    if coll is None: return pd.DataFrame()
    try:
        docs = coll.stream()
        lista = [d.to_dict() for d in sorted(docs, key=lambda x: x.to_dict().get("part", 0))]
        csv_raw = "".join([d.get("csv_data", "") for d in lista])
        if not csv_raw: return pd.DataFrame()
        df = pd.read_csv(io.StringIO(csv_raw), dtype=str)
        for col in ["Peso", "Larg", "Comp", "Esp"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", "."), errors="coerce").fillna(0)
        return df
    except: return pd.DataFrame()

def carregar_movimentos():
    coll = get_coll("movements")
    if coll is None: return pd.DataFrame()
    try:
        docs = coll.stream()
        return pd.DataFrame([d.to_dict() for d in docs])
    except: return pd.DataFrame()

def carregar_users():
    coll = get_coll("users")
    if coll is None: return {}
    try:
        docs = coll.stream()
        return {d.to_dict()["username"]: d.to_dict() for d in docs}
    except: return {}

# --- 3. CÁLCULOS ---

def calcular_saldos():
    base = carregar_base_mestra()
    if base.empty: return pd.DataFrame()
    
    chaves = ["LVM", "Material", "Obra", "ElementoPEP"]
    especs = ["Grau", "Esp", "Larg", "Comp"]
    
    for c in chaves + especs:
        if c in base.columns: base[c] = base[c].astype(str).str.strip().str.upper()

    inv = base.groupby(chaves + especs).agg({
        "DescritivoMaterial": "first", "Peso": "first", "Material": "count"
    }).rename(columns={"Material": "Qtd_Inicial"}).reset_index()
    
    movs = carregar_movimentos()
    if not movs.empty and "Tipo" in movs.columns:
        for c in chaves:
            if c in movs.columns: movs[c] = movs[c].astype(str).str.strip().str.upper()
        movs["Qtd_N"] = pd.to_numeric(movs["Qtde"], errors="coerce").fillna(0)
        movs["Impacto"] = movs.apply(lambda x: x["Qtd_N"] if str(x["Tipo"]).upper() in ["ENTRADA", "TDMA"] else -x["Qtd_N"], axis=1)
        resumo = movs.groupby(chaves)["Impacto"].sum().reset_index()
        inv = pd.merge(inv, resumo, on=chaves, how="left").fillna(0)
    else:
        inv["Impacto"] = 0
        
    inv["Saldo_Pecas"] = inv["Qtd_Inicial"] + inv["Impacto"]
    inv["Saldo_KG"] = inv["Saldo_Pecas"] * inv["Peso"]
    return inv[inv["Saldo_Pecas"] > 0].sort_values(by=["Obra", "LVM"])

# --- 4. RELATÓRIOS ---

def gerar_pdf(df):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("helvetica", "B", 12)
    pdf.cell(0, 10, "Mapa de Estoque - Marcius Estoque", ln=True, align="C")
    pdf.set_font("helvetica", "", 8)
    pdf.ln(5)
    # Cabeçalho da tabela
    headers = ["LVM", "Material", "Obra", "Qtd", "Kg"]
    for h in headers: pdf.cell(38, 7, h, 1)
    pdf.ln()
    for _, r in df.head(100).iterrows(): # Limite para o PDF não ficar gigante no mobile
        pdf.cell(38, 6, str(r['LVM'])[:15], 1)
        pdf.cell(38, 6, str(r['Material']), 1)
        pdf.cell(38, 6, str(r['Obra'])[:15], 1)
        pdf.cell(38, 6, str(int(r['Saldo_Pecas'])), 1)
        pdf.cell(38, 6, f"{r['Saldo_KG']:.1f}", 1)
        pdf.ln()
    return pdf.output()

# --- 5. INTERFACE ---

def main():
    st.set_page_config(page_title="Marcius Estoque Pro", layout="wide")

    # CSS para Mobile
    st.markdown("""
        <style>
            .stButton>button { width: 100%; border-radius: 10px; height: 3.5em; font-weight: bold; }
            .login-card { background: white; padding: 2rem; border-radius: 1rem; border: 1px solid #eee; }
        </style>
    """, unsafe_allow_html=True)

    if db is None:
        st.error(f"🔴 Erro de Ligação: {erro_conexao}")
        return

    users = carregar_users()
    if "logado" not in st.session_state: st.session_state.logado = False

    if not st.session_state.logado:
        st.markdown("<h1 style='text-align: center;'>🏗️ Gestão de Estoque</h1>", unsafe_allow_html=True)
        _, col, _ = st.columns([1, 4, 1])
        with col:
            with st.container():
                u = st.text_input("Utilizador").lower().strip()
                p = st.text_input("Senha", type="password").strip()
                if st.button("ENTRAR"):
                    if u in users and users[u]["password"] == p:
                        st.session_state.logado = True
                        st.session_state.user = users[u]
                        st.rerun()
                    else: st.error("Utilizador ou Senha incorretos.")
            
            st.divider()
            with st.expander("⚠️ Problemas ao aceder pelo Telemóvel?"):
                st.write("""
                Se estiver a ver o erro **"Too many redirects"**:
                1. Use o **Google Chrome** em vez do Safari.
                2. Abra o link em **Modo Incógnito/Anónimo**.
                3. Limpe os 'Cookies' e 'Dados de Navegação' nas definições do telemóvel.
                4. Verifique se o telemóvel não está a 'bloquear cookies de terceiros'.
                """)
        return

    # Menu
    nav = ["📊 Dashboard", "🔄 Movimentações", "👤 Conta"]
    if st.session_state.user['nivel'] == "admin": nav += ["📂 Base Mestra", "👥 Equipa"]
    
    menu = st.sidebar.radio("Navegação", nav)
    st.sidebar.button("Terminar Sessão", on_click=lambda: st.session_state.update({"logado": False}))

    if menu == "📊 Dashboard":
        st.title("📊 Painel de Controle")
        df = calcular_saldos()
        if df.empty:
            st.info("💡 Carregue a Base Mestra para ver dados.")
        else:
            # KPIs
            c1, c2, c3 = st.columns(3)
            c1.metric("Peças", f"{int(df['Saldo_Pecas'].sum()):,}")
            c2.metric("Total KG", f"{df['Saldo_KG'].sum():,.1f}")
            c3.metric("LVMs", len(df["LVM"].unique()))
            
            # Gráficos simples para mobile
            col1, col2 = st.columns(2)
            with col1:
                st.plotly_chart(px.pie(df.groupby("Obra")["Saldo_Pecas"].sum().reset_index().nlargest(5, "Saldo_Pecas"), values="Saldo_Pecas", names="Obra", title="Top 5 Obras"), use_container_width=True)
            with col2:
                st.plotly_chart(px.bar(df.groupby("Grau")["Saldo_KG"].sum().reset_index(), x="Grau", y="Saldo_KG", title="Peso por Grau"), use_container_width=True)
            
            st.dataframe(df, use_container_width=True, hide_index=True)

    elif menu == "🔄 Movimentações":
        st.title("🔄 Registos")
        base = carregar_base_mestra()
        if base.empty: st.warning("Carregue a base primeiro."); return
        
        with st.form("f_mov"):
            tipo = st.selectbox("Operação", ["SAIDA", "ENTRADA", "TMA", "TDMA"])
            mat = st.selectbox("Material", sorted(base["Material"].unique()))
            lvm = st.text_input("LVM").upper().strip()
            qtd = st.number_input("Qtde", min_value=1, step=1)
            obr = st.text_input("Obra").upper().strip()
            pep = st.text_input("PEP").upper().strip()
            if st.form_submit_button("GRAVAR"):
                get_coll("movements").add({"Tipo": tipo, "Material": mat, "LVM": lvm, "Qtde": qtd, "Obra": obr, "ElementoPEP": pep, "Data": datetime.now().strftime('%Y-%m-%d'), "timestamp": firestore.SERVER_TIMESTAMP})
                st.success("Gravado!"); time.sleep(1); st.rerun()

    elif menu == "📂 Base Mestra":
        st.title("📂 Sincronização")
        f = st.file_uploader("Subir Excel", type="xlsx")
        if f and st.button("🚀 ATUALIZAR NUVEM"):
            df_m = pd.read_excel(f, dtype=str)
            coll = get_coll("master_csv_store")
            for d in coll.stream(): d.reference.delete()
            csv_t = df_m.to_csv(index=False)
            size = 800000
            for i, p in enumerate([csv_t[i:i+size] for i in range(0, len(csv_t), size)]):
                coll.document(f"p_{i}").set({"part": i, "csv_data": p})
            st.cache_data.clear()
            st.success("Sincronizado!"); st.balloons()

if __name__ == "__main__":
    main()