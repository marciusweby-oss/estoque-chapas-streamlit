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

def inicializar_firebase():
    """Inicializa a ligação ao Firebase usando as Secrets do Streamlit Cloud."""
    try:
        if "firebase" not in st.secrets:
            return None, "ERRO: Secrets não configuradas."
        
        config = dict(st.secrets["firebase"])
        app_name = "marcius-stock-v18-pro"
        
        if not firebase_admin._apps:
            cred = credentials.Certificate(config)
            firebase_admin.initialize_app(cred, name=app_name)
        else:
            try:
                firebase_admin.get_app(app_name)
            except ValueError:
                cred = credentials.Certificate(config)
                firebase_admin.initialize_app(cred, name=app_name)
        
        app_inst = firebase_admin.get_app(app_name)
        return firestore.client(app=app_inst), None
    except Exception as e:
        return None, f"Erro de ligação: {str(e)}"

# Inicialização global
db, erro_db = inicializar_firebase()
PROJECT_ID = "marcius-stock-pro-v18"

# --- 2. GESTÃO DE DADOS (FIRESTORE) ---

def get_coll(nome):
    if db is None: return None
    return db.collection("artifacts").document(PROJECT_ID).collection("public").document("data").collection(nome)

@st.cache_data(ttl=60)
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
        dados = [d.to_dict() for d in docs]
        return pd.DataFrame(dados) if dados else pd.DataFrame()
    except: return pd.DataFrame()

def carregar_users():
    coll = get_coll("users")
    if coll is None: return {}
    try:
        docs = coll.stream()
        users = {d.to_dict()["username"]: d.to_dict() for d in docs}
        if not users:
            mestre = {"username": "marcius.arruda", "password": "MwsArruda", "nivel": "Admin"}
            coll.add(mestre)
            return {"marcius.arruda": mestre}
        return users
    except: return {}

# --- 3. LÓGICA DE INVENTÁRIO ---

def calcular_saldos():
    base = carregar_base_mestra()
    if base.empty: return pd.DataFrame()
    chaves = ["LVM", "Material", "Obra", "ElementoPEP"]
    especs = ["Grau", "Esp", "Larg", "Comp"]
    todas = chaves + especs
    for c in todas:
        if c in base.columns: base[c] = base[c].astype(str).str.strip().str.upper()
    inv = base.groupby(todas).agg({
        "DescritivoMaterial": "first", "Peso": "first", "Material": "count"
    }).rename(columns={"Material": "Qtd_Inicial"}).reset_index()
    movs = carregar_movimentos()
    if not movs.empty and "Tipo" in movs.columns and "Qtde" in movs.columns:
        for c in chaves:
            if c in movs.columns: movs[c] = movs[c].astype(str).str.strip().str.upper()
        movs["Qtd_Num"] = pd.to_numeric(movs["Qtde"], errors="coerce").fillna(0)
        movs["Impacto"] = movs.apply(
            lambda x: x["Qtd_Num"] if str(x.get("Tipo")).upper() in ["ENTRADA", "TDMA"] else -x["Qtd_Num"], 
            axis=1
        )
        resumo = movs.groupby(chaves)["Impacto"].sum().reset_index()
        inv = pd.merge(inv, resumo, on=chaves, how="left").fillna(0)
    else: inv["Impacto"] = 0
    inv["Saldo_Pecas"] = inv["Qtd_Inicial"] + inv["Impacto"]
    inv["Saldo_KG"] = inv["Saldo_Pecas"] * inv["Peso"]
    return inv[inv["Saldo_Pecas"] > 0].sort_values(by=["Obra", "LVM"])

# --- 4. EXPORTAÇÃO PDF ---

class StockPDF(FPDF):
    def header(self):
        if os.path.exists("logo_empresa.png"):
            self.image("logo_empresa.png", 10, 8, 25)
        self.set_font("helvetica", "B", 14)
        self.cell(0, 10, "MAPA DE STOCK - MARCIUS ARRUDA", ln=True, align="R")
        self.set_font("helvetica", "I", 8)
        self.cell(0, 5, f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M')}", ln=True, align="R")
        self.ln(10)

def gerar_pdf(df):
    pdf = StockPDF()
    pdf.alias_nb_pages()
    pdf.add_page()
    pdf.set_font("helvetica", "B", 7)
    pdf.set_fill_color(240, 240, 240)
    cols = ["LVM", "Material", "Obra", "Grau", "Esp.", "Larg.", "Comp.", "Qtd"]
    ws = [25, 35, 30, 20, 15, 20, 20, 25]
    for i in range(len(cols)):
        pdf.cell(ws[i], 8, cols[i], 1, 0, "C", 1)
    pdf.ln()
    pdf.set_font("helvetica", "", 6)
    for _, r in df.iterrows():
        pdf.cell(25, 7, str(r['LVM']), 1)
        pdf.cell(35, 7, str(r['Material']), 1)
        pdf.cell(30, 7, str(r['Obra']), 1)
        pdf.cell(20, 7, str(r['Grau']), 1)
        pdf.cell(15, 7, str(r['Esp']), 1, 0, "C")
        pdf.cell(20, 7, str(r['Larg']), 1, 0, "C")
        pdf.cell(20, 7, str(r['Comp']), 1, 0, "C")
        pdf.cell(25, 7, f"{int(r['Saldo_Pecas'])} PC", 1, 1, "R")
    pdf.ln(5)
    pdf.set_font("helvetica", "B", 10)
    pdf.cell(0, 10, f"TOTAL: {int(df['Saldo_Pecas'].sum())} Peças | {df['Saldo_KG'].sum():,.2f} KG", align="R")
    return pdf.output()

# --- 5. INTERFACE ---

def main():
    st.set_page_config(page_title="Gestão de Stock", layout="wide", page_icon="🏗️")

    st.markdown("""
        <style>
            .stButton>button { width: 100%; border-radius: 10px; height: 3.5em; font-weight: bold; background-color: #f0f2f6; border: 1px solid #ccc; }
            .stMetric { background-color: #ffffff; padding: 15px; border-radius: 10px; border: 1px solid #ddd; }
            .status-indicator { font-size: 0.8em; color: gray; margin-bottom: 10px; }
        </style>
    """, unsafe_allow_html=True)

    with st.sidebar:
        if os.path.exists("logo_empresa.png"):
            st.image("logo_empresa.png", use_container_width=True)
        else:
            st.markdown("<h2 style='text-align: center; color: #FF4B4B;'>🏗️ STOCK PRO</h2>", unsafe_allow_html=True)
        
        # Indicador de Status de Ligação
        if db is not None:
            st.markdown("<div class='status-indicator'>🟢 Sistema Online</div>", unsafe_allow_html=True)
        else:
            st.markdown("<div class='status-indicator'>🔴 Sistema Offline</div>", unsafe_allow_html=True)
        
        st.divider()

    if db is None:
        st.error("🔴 LIGAÇÃO AO BANCO DE DADOS FALHOU")
        st.info("O Administrador deve verificar as 'Secrets' no painel do Streamlit Cloud.")
        return

    # LOGIN
    users = carregar_users()
    if "logado" not in st.session_state: st.session_state.logado = False
    
    if not st.session_state.logado:
        st.markdown("<h1 style='text-align: center;'>🔐 Login de Segurança</h1>", unsafe_allow_html=True)
        st.markdown("<p style='text-align: center;'>Introduza o seu utilizador exatamente como criado.</p>", unsafe_allow_html=True)
        
        _, col_l, _ = st.columns([1, 2, 1])
        with col_l:
            u_input = st.text_input("Utilizador").lower().strip()
            p_input = st.text_input("Senha", type="password")
            
            if st.button("ACEDER AO SISTEMA"):
                # Validação robusta: remove espaços e converte para minúsculas
                if u_input in users and users[u_input]["password"] == p_input:
                    st.session_state.logado = True
                    st.session_state.user = users[u_input]
                    st.rerun()
                else:
                    st.error("Falha no Login. Verifique o nome e a senha. Nota: O teclado do telemóvel pode ter colocado uma letra maiúscula por engano.")
            
            st.divider()
            if st.button("🔄 Limpar Cache / Tentar Novamente"):
                st.cache_data.clear()
                st.rerun()
        return

    # NAVEGAÇÃO
    menu_options = ["📊 Dashboard", "🔄 Movimentações", "👤 Minha Conta"]
    if st.session_state.user['nivel'] == "Admin":
        menu_options += ["📂 Base Mestra", "👥 Gestão de Acessos"]
    
    menu = st.sidebar.radio("Navegação", menu_options)
    st.sidebar.markdown(f"**👤 Sessão:** {st.session_state.user['username']}")
    
    if st.sidebar.button("Terminar Sessão"):
        st.session_state.logado = False
        st.rerun()

    # --- PÁGINAS ---
    if menu == "📊 Dashboard":
        st.title("📊 Painel de Controle")
        df_full = calcular_saldos()
        if df_full.empty:
            st.info("💡 Catálogo vazio. Administrador: carregue a Base Mestra.")
        else:
            st.sidebar.markdown("### 🔍 Filtros")
            f_mat = st.sidebar.multiselect("Material", sorted(df_full["Material"].unique()))
            f_obra = st.sidebar.multiselect("Obra", sorted(df_full["Obra"].unique()))
            f_pep = st.sidebar.multiselect("Elemento PEP", sorted(df_full["ElementoPEP"].unique()))
            f_grau = st.sidebar.multiselect("Grau", sorted(df_full["Grau"].unique()))
            f_esp = st.sidebar.multiselect("Espessura", sorted(df_full["Esp"].unique()))
            f_lvm = st.sidebar.text_input("Pesquisar LVM").upper().strip()

            df_v = df_full.copy()
            if f_mat: df_v = df_v[df_v["Material"].isin(f_mat)]
            if f_obra: df_v = df_v[df_v["Obra"].isin(f_obra)]
            if f_pep: df_v = df_v[df_v["ElementoPEP"].isin(f_pep)]
            if f_grau: df_v = df_v[df_v["Grau"].isin(f_grau)]
            if f_esp: df_v = df_v[df_v["Esp"].isin(f_esp)]
            if f_lvm: df_v = df_v[df_v["LVM"].str.contains(f_lvm)]

            c1, c2, c3 = st.columns(3)
            c1.metric("Peças", f"{int(df_v['Saldo_Pecas'].sum()):,}")
            c2.metric("Total KG", f"{df_v['Saldo_KG'].sum():,.2f}")
            c3.metric("LVMs", len(df_v["LVM"].unique()))

            if st.button("📥 Gerar Relatório PDF"):
                pdf_data = gerar_pdf(df_v)
                st.download_button("💾 Baixar PDF", pdf_data, f"stock_{datetime.now().strftime('%d%m%Y')}.pdf", "application/pdf")
            st.divider()
            g1, g2 = st.columns(2)
            with g1:
                fig1 = px.pie(df_v.groupby("Obra")["Saldo_Pecas"].sum().reset_index().nlargest(10, "Saldo_Pecas"), values="Saldo_Pecas", names="Obra", title="Top Obras", hole=0.3)
                st.plotly_chart(fig1, use_container_width=True)
            with g2:
                fig2 = px.bar(df_v.groupby("Grau")["Saldo_KG"].sum().reset_index(), x="Grau", y="Saldo_KG", title="Peso por Grau", color="Grau")
                st.plotly_chart(fig2, use_container_width=True)
            st.dataframe(df_v, use_container_width=True, hide_index=True)

    elif menu == "🔄 Movimentações":
        st.title("🔄 Movimentações")
        base = carregar_base_mestra()
        if base.empty: st.error("Falta carregar a Base Mestra."); return
        t1, t2 = st.tabs(["📝 Individual", "📁 Lote (Excel)"])
        with t1:
            with st.form("form_ind"):
                tipo = st.selectbox("Operação", ["SAIDA", "ENTRADA", "TMA", "TDMA"])
                mat = st.selectbox("Material", sorted(base["Material"].unique()))
                lvm = st.text_input("LVM").upper().strip()
                qtd = st.number_input("Quantidade", min_value=1, step=1)
                obr = st.text_input("Obra").upper().strip()
                pep = st.text_input("Elemento PEP").upper().strip()
                if st.form_submit_button("GRAVAR"):
                    coll = get_coll("movements")
                    dt = datetime.now().strftime("%d/%m/%Y")
                    coll.add({"Tipo": tipo, "Material": mat, "LVM": lvm, "Qtde": qtd, "Obra": obr, "ElementoPEP": pep, "Data": dt, "timestamp": firestore.SERVER_TIMESTAMP})
                    st.success("Registado!"); time.sleep(1); st.rerun()
        with t2:
            st.info("Colunas: Material, LVM, Qtde, Obra, ElementoPEP, Data")
            tipo_up = st.selectbox("Tipo de Movimento", ["SAIDA", "ENTRADA", "TMA", "TDMA"])
            up = st.file_uploader(f"Subir Excel", type="xlsx")
            if up and st.button("🚀 IMPORTAR"):
                df_up = pd.read_excel(up, dtype=str)
                coll = get_coll("movements")
                for _, r in df_up.iterrows():
                    d = r.to_dict(); d["Tipo"] = tipo_up; d["timestamp"] = firestore.SERVER_TIMESTAMP
                    coll.add(d)
                st.success("Importação concluída!"); st.rerun()

    elif menu == "👤 Minha Conta":
        st.title("👤 Configurações")
        with st.form("f_pass"):
            s_atual = st.text_input("Senha Atual", type="password")
            s_nova = st.text_input("Nova Senha", type="password")
            if st.form_submit_button("ATUALIZAR"):
                if s_atual == st.session_state.user['password']:
                    ref = get_coll("users").where("username", "==", st.session_state.user['username']).stream()
                    for d in ref: d.reference.update({"password": s_nova})
                    st.session_state.user['password'] = s_nova
                    st.success("Senha alterada!"); st.rerun()
                else: st.error("Senha incorreta.")

    elif menu == "👥 Gestão de Acessos":
        st.title("👥 Gerir Equipa")
        with st.form("f_u"):
            nu = st.text_input("Novo User").lower().strip()
            np = st.text_input("Senha", type="password")
            nv = st.selectbox("Nível", ["Operador", "Admin"])
            if st.form_submit_button("CRIAR"):
                get_coll("users").add({"username": nu, "password": np, "nivel": nv})
                st.success("Criado!"); st.rerun()
        st.divider()
        for n, d in users.items():
            c1, c2 = st.columns([4, 1])
            c1.write(f"🏷️ **{n}** | {d['nivel']}")
            if n != "marcius.arruda" and c2.button("Eliminar", key=f"d_{n}"):
                docs = get_coll("users").where("username", "==", n).stream()
                for doc in docs: doc.reference.delete()
                st.rerun()

    elif menu == "📂 Base Mestra":
        st.title("📂 Catálogo")
        f = st.file_uploader("Ficheiro Excel", type="xlsx")
        if f and st.button("🚀 SINCRONIZAR"):
            df_m = pd.read_excel(f, dtype=str)
            coll = get_coll("master_csv_store")
            for d in coll.stream(): d.reference.delete()
            csv_text = df_m.to_csv(index=False)
            size = 800000
            parts = [csv_text[i:i+size] for i in range(0, len(csv_text), size)]
            for i, p in enumerate(parts):
                coll.document(f"p_{i}").set({"part": i, "csv_data": p})
            st.cache_data.clear()
            st.success("Sincronizado!"); st.balloons()

if __name__ == "__main__":
    main()