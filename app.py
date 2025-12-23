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
    try:
        if "firebase" not in st.secrets:
            return None, "Aba 'Secrets' não configurada."
        config = dict(st.secrets["firebase"])
        app_id = "marcius-stock-v10"
        if not firebase_admin._apps:
            cred = credentials.Certificate(config)
            firebase_admin.initialize_app(cred, name=app_id)
        app_inst = firebase_admin.get_app(app_id)
        return firestore.client(app=app_inst), None
    except Exception as e:
        return None, str(e)

db, erro_conexao = inicializar_firebase()
APP_ID = "marcius-stock-pro-v10"

# --- 2. GESTÃO DE DADOS ---

def get_coll(nome_colecao):
    if db is None: return None
    return db.collection("artifacts").document(APP_ID).collection("public").document("data").collection(nome_colecao)

@st.cache_data(ttl=30)
def carregar_catalogo_nuvem():
    coll = get_coll("master_csv_store")
    if coll is None: return pd.DataFrame()
    try:
        docs = coll.stream()
        partes = "".join([d.to_dict()["csv_data"] for d in sorted(docs, key=lambda x: x.to_dict().get("part", 0))])
        df = pd.read_csv(io.StringIO(partes), dtype=str)
        for c in ["Peso", "Larg", "Comp", "Esp"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c].astype(str).str.replace(",", "."), errors="coerce").fillna(0)
        return df
    except: return pd.DataFrame()

def carregar_movimentacoes_nuvem():
    coll = get_coll("movements")
    if coll is None: return pd.DataFrame()
    docs = coll.stream()
    return pd.DataFrame([d.to_dict() for d in docs])

def carregar_utilizadores():
    coll = get_coll("users")
    if coll is None: return {}
    docs = coll.stream()
    users = {d.to_dict()["username"]: d.to_dict() for d in docs}
    # Cria utilizador mestre se a coleção estiver vazia
    if not users:
        admin_data = {"username": "marcius.arruda", "password": "MwsArruda", "nivel": "Admin"}
        coll.add(admin_data)
        return {"marcius.arruda": admin_data}
    return users

# --- 3. LÓGICA DE INVENTÁRIO ---

def calcular_estoque_atual():
    base = carregar_catalogo_nuvem()
    if base.empty: return pd.DataFrame()
    cols_tec = ["LVM", "Material", "Obra", "ElementoPEP", "Grau", "Esp", "Larg", "Comp"]
    for c in cols_tec:
        if c in base.columns: base[c] = base[c].astype(str).str.strip().str.upper()
    
    inv = base.groupby(cols_tec).agg({"DescritivoMaterial": "first", "Peso": "first", "Material": "count"}).rename(columns={"Material": "Pecas_Iniciais"}).reset_index()
    movs = carregar_movimentacoes_nuvem()
    if not movs.empty:
        for c in ["LVM", "Material", "Obra", "ElementoPEP"]:
            if c in movs.columns: movs[c] = movs[c].astype(str).str.strip().str.upper()
        movs["Qtd_N"] = pd.to_numeric(movs["Qtde"], errors="coerce").fillna(0)
        movs["Impacto"] = movs.apply(lambda x: x["Qtd_N"] if x["Tipo"] in ["ENTRADA", "TDMA"] else -x["Qtd_N"], axis=1)
        resumo = movs.groupby(["LVM", "Material", "Obra", "ElementoPEP"])["Impacto"].sum().reset_index()
        inv = pd.merge(inv, resumo, on=["LVM", "Material", "Obra", "ElementoPEP"], how="left").fillna(0)
    else: inv["Impacto"] = 0
    
    inv["Saldo_Pecas"] = inv["Pecas_Iniciais"] + inv["Impacto"]
    inv["Saldo_KG"] = inv["Saldo_Pecas"] * inv["Peso"]
    return inv[inv["Saldo_Pecas"] > 0]

# --- 4. RELATÓRIOS PDF ---

class PDF_Stock(FPDF):
    def header(self):
        if os.path.exists("logo_empresa.png"):
            self.image("logo_empresa.png", 10, 8, 30)
        self.set_font("helvetica", "B", 14)
        self.cell(0, 10, "RELATÓRIO DE ESTOQUE - MARCIUS STOCK", ln=True, align="C")
        self.set_font("helvetica", "", 10)
        self.cell(0, 5, f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M')}", ln=True, align="C")
        self.ln(15)

def gerar_relatorio_pdf(df, usuario):
    pdf = PDF_Stock()
    pdf.add_page()
    pdf.set_font("helvetica", "B", 8)
    # Cabeçalho da Tabela
    pdf.set_fill_color(240, 240, 240)
    pdf.cell(30, 8, "LVM", 1, 0, "C", 1)
    pdf.cell(50, 8, "Material", 1, 0, "C", 1)
    pdf.cell(30, 8, "Obra", 1, 0, "C", 1)
    pdf.cell(15, 8, "Esp.", 1, 0, "C", 1)
    pdf.cell(30, 8, "Saldo (PC)", 1, 0, "C", 1)
    pdf.cell(35, 8, "Peso (KG)", 1, 1, "C", 1)
    
    pdf.set_font("helvetica", "", 7)
    for _, r in df.iterrows():
        pdf.cell(30, 7, str(r['LVM']), 1)
        pdf.cell(50, 7, str(r['Material'])[:25], 1)
        pdf.cell(30, 7, str(r['Obra']), 1)
        pdf.cell(15, 7, str(r['Esp']), 1, 0, "C")
        pdf.cell(30, 7, f"{int(r['Saldo_Pecas'])}", 1, 0, "R")
        pdf.cell(35, 7, f"{r['Saldo_KG']:,.2f}", 1, 1, "R")
    
    return pdf.output()

# --- 5. INTERFACE ---

def main():
    if db is None:
        st.error(f"🔴 Erro de Ligação: {erro_conexao}")
        return

    # Login
    utilizadores = carregar_utilizadores()
    if "logado" not in st.session_state: st.session_state.logado = False
    
    if not st.session_state.logado:
        st.sidebar.markdown("### 🔐 Acesso")
        u = st.sidebar.text_input("Utilizador").lower().strip()
        p = st.sidebar.text_input("Senha", type="password")
        if st.sidebar.button("Entrar", use_container_width=True):
            if u in utilizadores and utilizadores[u]["password"] == p:
                st.session_state.logado = True
                st.session_state.user = utilizadores[u]
                st.rerun()
            else: st.sidebar.error("Dados Inválidos")
        st.info("👋 Bem-vindo. Por favor, faça login para aceder ao estoque.")
        return

    # Sidebar
    if os.path.exists("logo_empresa.png"): st.sidebar.image("logo_empresa.png")
    st.sidebar.title("Marcius Stock")
    st.sidebar.caption(f"👤 {st.session_state.user['username']} ({st.session_state.user['nivel']})")
    
    opcoes = ["📊 Dashboard", "🔄 Movimentações"]
    if st.session_state.user['nivel'] == "Admin":
        opcoes += ["📂 Base Mestra", "👥 Gestão de Acessos"]
    
    menu = st.sidebar.radio("Navegação", opcoes)
    if st.sidebar.button("Terminar Sessão"):
        st.session_state.logado = False
        st.rerun()

    # --- PÁGINA: DASHBOARD ---
    if menu == "📊 Dashboard":
        st.title("📊 Controle de Saldos")
        df = calcular_estoque_atual()
        if df.empty: st.warning("Sem dados."); return
        
        with st.sidebar.expander("🔍 Filtros", expanded=True):
            f_obra = st.multiselect("Obra", sorted(df["Obra"].unique()))
            f_esp = st.multiselect("Espessura (mm)", sorted(df["Esp"].unique()))
            f_lvm = st.text_input("LVM (Busca rápida)").upper()

        df_v = df.copy()
        if f_obra: df_v = df_v[df_v["Obra"].isin(f_obra)]
        if f_esp: df_v = df_v[df_v["Esp"].isin(f_esp)]
        if f_lvm: df_v = df_v[df_v["LVM"].str.contains(f_lvm)]

        c1, c2, c3 = st.columns(3)
        c1.metric("Peças", f"{int(df_v['Saldo_Pecas'].sum()):,}")
        c2.metric("Peso Total", f"{df_v['Saldo_KG'].sum():,.2f} kg")
        
        # Botão PDF
        if st.button("📥 Exportar Saldo para PDF"):
            pdf_bytes = gerar_relatorio_pdf(df_v, st.session_state.user['username'])
            st.download_button("💾 Baixar PDF", pdf_bytes, f"estoque_{datetime.now().strftime('%d%m%Y')}.pdf", "application/pdf")

        st.dataframe(df_v, use_container_width=True, hide_index=True)

    # --- PÁGINA: GESTÃO DE ACESSOS ---
    elif menu == "👥 Gestão de Acessos":
        st.title("👥 Gerir Utilizadores")
        with st.form("novo_user"):
            st.subheader("Criar Novo Acesso")
            nu = st.text_input("Novo Utilizador").lower().strip()
            np = st.text_input("Senha", type="password")
            nv = st.selectbox("Nível", ["Admin", "Operador"])
            if st.form_submit_button("Guardar Utilizador"):
                get_coll("users").add({"username": nu, "password": np, "nivel": nv})
                st.success("Criado!")
                time.sleep(1)
                st.rerun()
        
        st.subheader("Utilizadores Ativos")
        for u_name, u_data in utilizadores.items():
            col_u1, col_u2 = st.columns([3, 1])
            col_u1.write(f"**{u_name}** - {u_data['nivel']}")
            if u_name != "marcius.arruda":
                if col_u2.button("Apagar", key=u_name):
                    # Lógica para apagar doc no firestore
                    docs = get_coll("users").where("username", "==", u_name).stream()
                    for d in docs: d.reference.delete()
                    st.rerun()

    # --- PÁGINA: MOVIMENTAÇÕES ---
    elif menu == "🔄 Movimentações":
        st.title("🔄 Movimentações")
        base = carregar_catalogo_nuvem()
        if base.empty: st.error("Sem Base Mestra"); return
        
        t1, t2 = st.tabs(["📝 Individual", "📁 Lote (Excel)"])
        with t1:
            with st.form("mov_form"):
                tp = st.selectbox("Tipo", ["SAIDA", "ENTRADA", "TMA", "TDMA"])
                mat = st.selectbox("Material", sorted(base["Material"].unique()))
                lvm = st.text_input("LVM").upper()
                qtd = st.number_input("Qtd", min_value=1)
                ob = st.text_input("Obra").upper()
                pe = st.text_input("PEP").upper()
                if st.form_submit_button("Registrar"):
                    get_coll("movements").add({"Tipo": tp, "Material": mat, "LVM": lvm, "Qtde": qtd, "Obra": ob, "ElementoPEP": pe, "timestamp": firestore.SERVER_TIMESTAMP})
                    st.success("Feito!")
        
        with t2:
            st.info("O Excel deve conter: Material, LVM, Qtde, Obra, ElementoPEP, Tipo")
            up = st.file_uploader("Subir Movimentações", type="xlsx")
            if up and st.button("🚀 Processar Excel"):
                df_up = pd.read_excel(up, dtype=str)
                coll = get_coll("movements")
                for _, r in df_up.iterrows():
                    coll.add(r.to_dict())
                st.success("Importado!")

    # --- PÁGINA: BASE MESTRA ---
    elif menu == "📂 Base Mestra":
        st.title("📂 Base Mestra")
        f = st.file_uploader("Carregar Catálogo Principal", type="xlsx")
        if f and st.button("🚀 Sincronizar"):
            df_up = pd.read_excel(f, dtype=str)
            coll = get_coll("master_csv_store")
            for d in coll.stream(): d.reference.delete()
            # Split and Save
            csv_text = df_up.to_csv(index=False)
            size = 800000
            parts = [csv_text[i:i+size] for i in range(0, len(csv_text), size)]
            for i, p in enumerate(parts):
                coll.document(f"p_{i}").set({"part": i, "csv_data": p})
            st.cache_data.clear()
            st.success("Base Mestra Atualizada!")

if __name__ == "__main__":
    main()