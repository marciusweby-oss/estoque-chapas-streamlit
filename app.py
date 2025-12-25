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
    """Inicializa a ligação ao Firebase usando as Secrets do Streamlit."""
    try:
        if "firebase" not in st.secrets:
            return None, "ERRO: Secrets não configuradas no Streamlit Cloud."
        
        config = dict(st.secrets["firebase"])
        
        # Tratamento da Private Key para compatibilidade PEM
        if "private_key" in config:
            pk = config["private_key"]
            pk = pk.replace("\\n", "\n")
            pk = pk.strip().strip('"').strip("'")
            if "-----BEGIN PRIVATE KEY-----" not in pk:
                pk = "-----BEGIN PRIVATE KEY-----\n" + pk
            if "-----END PRIVATE KEY-----" not in pk:
                pk = pk + "\n-----END PRIVATE KEY-----\n"
            config["private_key"] = pk
            
        app_name = "marcius-estoque-v28"
        
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
        return None, f"Erro de conexão: {str(e)}"

db, erro_conexao = inicializar_firebase()
PROJECT_ID = "marcius-estoque-pro-v28"

# --- 2. GESTÃO DE DADOS ---

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
            admin = {"username": "marcius.arruda", "password": "MwsArruda", "nivel": "Admin"}
            coll.add(admin)
            return {"marcius.arruda": admin}
        return users
    except: return {}

# --- 3. LÓGICA DE NEGÓCIO ---

def calcular_saldos():
    base = carregar_base_mestra()
    if base.empty: return pd.DataFrame()
    
    chaves = ["LVM", "Material", "Obra", "ElementoPEP"]
    especs = ["Grau", "Esp", "Larg", "Comp"]
    todas_cols = chaves + especs
    
    for c in todas_cols:
        if c in base.columns: base[c] = base[c].astype(str).str.strip().str.upper()

    inv = base.groupby(todas_cols).agg({
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

# --- 4. EXPORTAÇÃO PDF ---

class EstoquePDF(FPDF):
    def header(self):
        if os.path.exists("logo_empresa.png"):
            self.image("logo_empresa.png", 10, 8, 25)
        self.set_font("helvetica", "B", 14)
        self.cell(0, 10, "MAPA DE ESTOQUE - MARCIUS ESTOQUE", ln=True, align="R")
        self.set_font("helvetica", "I", 8)
        self.cell(0, 5, f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M')}", ln=True, align="R")
        self.ln(10)

def gerar_pdf(df):
    pdf = EstoquePDF()
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
    st.set_page_config(page_title="Marcius Gestão de Estoque", layout="wide", page_icon="🏗️")

    st.markdown("""
        <style>
            .stButton>button { width: 100%; border-radius: 12px; height: 3.5em; font-weight: bold; background-color: #f8f9fa; border: 1px solid #d1d3e2; }
            .stTextInput>div>div>input { height: 3.5em; border-radius: 8px; }
            .login-card { background-color: #ffffff; padding: 30px; border-radius: 20px; box-shadow: 0 10px 25px rgba(0,0,0,0.05); border: 1px solid #eee; }
            .stMetric { background-color: white; padding: 15px; border-radius: 12px; border: 1px solid #eee; }
        </style>
    """, unsafe_allow_html=True)

    with st.sidebar:
        if os.path.exists("logo_empresa.png"):
            st.image("logo_empresa.png", use_container_width=True)
        else:
            st.markdown("<h2 style='text-align: center; color: #FF4B4B;'>🏗️ ESTOQUE PRO</h2>", unsafe_allow_html=True)
        
        if db is not None:
            st.markdown("<div style='color: green; font-size: 0.85em; text-align: center;'>● Ligação Ativa</div>", unsafe_allow_html=True)
        else:
            st.markdown("<div style='color: red; font-size: 0.85em; text-align: center;'>● Sem Ligação</div>", unsafe_allow_html=True)
        st.divider()

    if db is None:
        st.error("🔴 ERRO TÉCNICO: Ligação à Base de Dados falhou.")
        st.info("O Administrador deve rever as 'Secrets' no Streamlit Cloud.")
        return

    users = carregar_users()
    if "logado" not in st.session_state: st.session_state.logado = False
    
    # --- TELA DE LOGIN ---
    if not st.session_state.logado:
        st.markdown("<br><h1 style='text-align: center; color: #1e3a8a;'>🏗️ Sistema de Gestão de Estoque</h1>", unsafe_allow_html=True)
        st.markdown("<p style='text-align: center; color: #64748b;'>Introduza as suas credenciais para aceder ao inventário.</p>", unsafe_allow_html=True)
        
        col_a, col_b, col_c = st.columns([1, 4, 1])
        with col_b:
            st.markdown("<div class='login-card'>", unsafe_allow_html=True)
            u_in = st.text_input("Nome de Utilizador").lower().strip()
            p_in = st.text_input("Senha", type="password").strip()
            
            if st.button("ENTRAR NO SISTEMA"):
                if u_in in users and users[u_in]["password"] == p_in:
                    st.session_state.logado = True
                    st.session_state.user = users[u_in]
                    st.rerun()
                else: 
                    st.error("Credenciais inválidas. Verifique maiúsculas e espaços.")
            st.markdown("</div>", unsafe_allow_html=True)
            
            with st.expander("💡 Dica para Telemóvel"):
                st.write("Para um acesso mais rápido, adicione este site ao seu 'Ecrã Principal' através do menu do navegador (Chrome ou Safari).")
        return

    # --- MENU NAVEGAÇÃO ---
    nav = ["📊 Dashboard", "🔄 Movimentações", "👤 Minha Conta"]
    if st.session_state.user['nivel'] == "Admin": 
        nav += ["📂 Base Mestra", "👥 Gestão de Acessos"]
    
    menu = st.sidebar.radio("Navegação", nav)
    st.sidebar.markdown(f"**👤 Utilizador:** {st.session_state.user['username']}")
    
    if st.sidebar.button("Terminar Sessão"):
        st.session_state.logado = False
        st.rerun()

    # --- TELA: DASHBOARD ---
    if menu == "📊 Dashboard":
        st.title("📊 Painel de Estoque Real")
        df = calcular_saldos()
        if df.empty:
            st.info("💡 Inventário vazio. O Administrador deve carregar a Base Mestra.")
        else:
            # Filtros Expandidos (Todos os campos possíveis da base)
            st.sidebar.markdown("### 🔍 Filtros Detalhados")
            f_mat = st.sidebar.multiselect("Código Material", sorted(df["Material"].unique()))
            f_obra = st.sidebar.multiselect("Obra", sorted(df["Obra"].unique()))
            f_pep = st.sidebar.multiselect("Elemento PEP", sorted(df["ElementoPEP"].unique()))
            f_grau = st.sidebar.multiselect("Grau", sorted(df["Grau"].unique()))
            f_esp = st.sidebar.multiselect("Espessura", sorted(df["Esp"].unique()))
            f_larg = st.sidebar.multiselect("Largura", sorted(df["Larg"].unique()))
            f_comp = st.sidebar.multiselect("Comprimento", sorted(df["Comp"].unique()))
            f_lvm = st.sidebar.text_input("Pesquisar LVM").upper().strip()

            df_v = df.copy()
            if f_mat: df_v = df_v[df_v["Material"].isin(f_mat)]
            if f_obra: df_v = df_v[df_v["Obra"].isin(f_obra)]
            if f_pep: df_v = df_v[df_v["ElementoPEP"].isin(f_pep)]
            if f_grau: df_v = df_v[df_v["Grau"].isin(f_grau)]
            if f_esp: df_v = df_v[df_v["Esp"].isin(f_esp)]
            if f_larg: df_v = df_v[df_v["Larg"].isin(f_larg)]
            if f_comp: df_v = df_v[df_v["Comp"].isin(f_comp)]
            if f_lvm: df_v = df_v[df_v["LVM"].str.contains(f_lvm)]

            c1, c2, c3 = st.columns(3)
            c1.metric("Peças Totais", f"{int(df_v['Saldo_Pecas'].sum()):,}")
            c2.metric("Peso Total (KG)", f"{df_v['Saldo_KG'].sum():,.2f}")
            c3.metric("LVMs Ativas", len(df_v["LVM"].unique()))
            
            st.divider()
            
            col_g1, col_g2 = st.columns(2)
            with col_g1:
                fig1 = px.pie(df_v.groupby("Obra")["Saldo_Pecas"].sum().reset_index().nlargest(10, "Saldo_Pecas"), values="Saldo_Pecas", names="Obra", title="Estoque por Obra (Top 10)", hole=0.4)
                st.plotly_chart(fig1, use_container_width=True)
            with col_g2:
                fig2 = px.bar(df_v.groupby("Grau")["Saldo_KG"].sum().reset_index(), x="Grau", y="Saldo_KG", title="Peso por Grau de Material", color="Grau")
                st.plotly_chart(fig2, use_container_width=True)
            
            st.divider()
            if st.button("📥 Gerar Relatório PDF"):
                pdf_data = gerar_pdf(df_v)
                st.download_button("💾 Clique para Baixar PDF", pdf_data, f"estoque_{datetime.now().strftime('%d%m%Y')}.pdf", "application/pdf")
            
            st.dataframe(df_v, use_container_width=True, hide_index=True)

    # --- TELA: MOVIMENTAÇÕES ---
    elif menu == "🔄 Movimentações":
        st.title("🔄 Registo de Entradas/Saídas")
        base = carregar_base_mestra()
        if base.empty: st.error("Carregue a base primeiro na aba 'Base Mestra'."); return
        
        tab_ind, tab_lote = st.tabs(["📝 Individual", "📁 Em Lote (Excel)"])
        
        with tab_ind:
            with st.form("f_ind"):
                t_op = st.selectbox("Tipo de Movimento", ["SAIDA", "ENTRADA", "TMA", "TDMA"])
                t_mat = st.selectbox("Material", sorted(base["Material"].unique()))
                t_lvm = st.text_input("LVM").upper().strip()
                t_qtd = st.number_input("Qtde", min_value=1, step=1)
                t_obr = st.text_input("Obra").upper().strip()
                t_pep = st.text_input("PEP").upper().strip()
                if st.form_submit_button("GRAVAR REGISTO"):
                    coll = get_coll("movements")
                    dt = datetime.now().strftime("%d/%m/%Y")
                    coll.add({"Tipo": t_op, "Material": t_mat, "LVM": t_lvm, "Qtde": t_qtd, "Obra": t_obr, "ElementoPEP": t_pep, "Data": dt, "timestamp": firestore.SERVER_TIMESTAMP})
                    st.success("Registo efetuado!"); time.sleep(1); st.rerun()

        with tab_lote:
            st.subheader("📁 Upload de Ficheiro")
            t_batch = st.selectbox("Operação do Ficheiro", ["SAIDA", "ENTRADA", "TMA", "TDMA"])
            up_f = st.file_uploader(f"Excel para {t_batch}", type="xlsx")
            if up_f and st.button(f"🚀 Importar Registos"):
                df_up = pd.read_excel(up_f, dtype=str)
                coll = get_coll("movements")
                for _, r in df_up.iterrows():
                    d = r.to_dict(); d["Tipo"] = t_batch; d["timestamp"] = firestore.SERVER_TIMESTAMP
                    coll.add(d)
                st.success("Registos importados!"); time.sleep(1); st.rerun()

    # --- TELA: MINHA CONTA ---
    elif menu == "👤 Minha Conta":
        st.title("👤 Definições de Conta")
        with st.form("f_p"):
            nova = st.text_input("Nova Palavra-passe", type="password")
            if st.form_submit_button("Guardar"):
                ref = get_coll("users").where("username", "==", st.session_state.user['username']).stream()
                for d in ref: d.reference.update({"password": nova})
                st.success("Palavra-passe atualizada!")

    # --- TELA: GESTÃO DE ACESSOS ---
    elif menu == "👥 Gestão de Acessos":
        st.title("👥 Gestão da Equipa")
        with st.form("f_u"):
            nu = st.text_input("Novo Utilizador").lower().strip()
            np = st.text_input("Senha", type="password")
            nv = st.selectbox("Permissão", ["Operador", "Admin"])
            if st.form_submit_button("Criar"):
                get_coll("users").add({"username": nu, "password": np, "nivel": nv})
                st.success("Conta criada!"); st.rerun()
        st.divider()
        for n, d in users.items():
            c1, c2 = st.columns([4, 1])
            c1.write(f"🏷️ **{n}** | {d['nivel']}")
            if n != "marcius.arruda" and c2.button("Remover", key=f"d_{n}"):
                docs = get_coll("users").where("username", "==", n).stream()
                for doc in docs: doc.reference.delete()
                st.rerun()

    # --- TELA: BASE MESTRA ---
    elif menu == "📂 Base Mestra":
        st.title("📂 Sincronização de Inventário")
        st.warning("Importante: Este passo substitui o catálogo base do sistema.")
        f = st.file_uploader("Ficheiro Excel (Base Mestra)", type="xlsx")
        if f and st.button("🚀 SINCRONIZAR AGORA"):
            df_m = pd.read_excel(f, dtype=str)
            coll = get_coll("master_csv_store")
            for d in coll.stream(): d.reference.delete()
            csv_t = df_m.to_csv(index=False)
            size = 800000
            for i, p in enumerate([csv_t[i:i+size] for i in range(0, len(csv_t), size)]):
                coll.document(f"p_{i}").set({"part": i, "csv_data": p})
            st.cache_data.clear()
            st.success("Inventário Sincronizado!"); st.balloons()

if __name__ == "__main__":
    main()