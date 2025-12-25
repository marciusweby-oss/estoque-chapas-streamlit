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
            return None, "ERRO: Secrets não configuradas no Cloud."
        
        config = dict(st.secrets["firebase"])
        
        if "private_key" in config:
            pk = config["private_key"].replace("\\n", "\n").strip().strip('"').strip("'")
            if "-----BEGIN PRIVATE KEY-----" not in pk:
                pk = "-----BEGIN PRIVATE KEY-----\n" + pk
            if "-----END PRIVATE KEY-----" not in pk:
                pk = pk + "\n-----END PRIVATE KEY-----\n"
            config["private_key"] = pk
            
        app_name = "marcius-estoque-v34"
        
        if not firebase_admin._apps:
            cred = credentials.Certificate(config)
            firebase_admin.initialize_app(cred, name=app_name)
        
        app_inst = firebase_admin.get_app(app_name)
        return firestore.client(app=app_inst), None
    except Exception as e:
        return None, f"Erro: {str(e)}"

# Inicialização do DB
db, erro_conexao = inicializar_firebase()
PROJECT_ID = "marcius-estoque-pro-v34"

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
        return pd.DataFrame([d.to_dict() for d in docs])
    except: return pd.DataFrame()

def carregar_users():
    coll = get_coll("users")
    if coll is None: return {}
    try:
        docs = coll.stream()
        users_map = {d.to_dict()["username"].lower().strip(): d.to_dict() for d in docs}
        if not users_map:
            admin = {"username": "marcius.arruda", "password": "MwsArruda", "nivel": "admin"}
            coll.add(admin)
            return {"marcius.arruda": admin}
        return users_map
    except: return {}

# --- 3. LÓGICA DE NEGÓCIO ---

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
    pdf.set_font("helvetica", "B", 14)
    pdf.cell(0, 10, "Mapa de Estoque - Gestão de Chapas", ln=True, align="C")
    pdf.set_font("helvetica", "", 8)
    pdf.ln(5)
    headers = ["LVM", "Material", "Obra", "Qtd", "Kg"]
    col_width = 38
    for h in headers: pdf.cell(col_width, 7, h, 1, 0, "C")
    pdf.ln()
    for _, r in df.iterrows():
        pdf.cell(col_width, 6, str(r['LVM'])[:15], 1)
        pdf.cell(col_width, 6, str(r['Material']), 1)
        pdf.cell(col_width, 6, str(r['Obra'])[:15], 1)
        pdf.cell(col_width, 6, str(int(r['Saldo_Pecas'])), 1, 0, "R")
        pdf.cell(col_width, 6, f"{r['Saldo_KG']:,.1f}", 1, 1, "R")
    return pdf.output()

# --- 5. INTERFACE ---

def main():
    st.set_page_config(page_title="Gestão de Estoque", layout="wide", page_icon="🏗️")

    st.markdown("""
        <style>
            .stButton>button { width: 100%; border-radius: 10px; height: 3.5em; font-weight: bold; background-color: #1e3a8a; color: white; }
            .login-card { background: white; padding: 2.5rem; border-radius: 1.5rem; box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1); border: 1px solid #f1f5f9; }
            .stTextInput>div>div>input { height: 3.5em; }
        </style>
    """, unsafe_allow_html=True)

    if db is None:
        st.error(f"🔴 Erro de Ligação ao Banco de Dados: {erro_conexao}")
        return

    users = carregar_users()
    if "logado" not in st.session_state: st.session_state.logado = False

    if not st.session_state.logado:
        st.markdown("<br><h1 style='text-align: center; color: #1e3a8a;'>🏗️ Gestão de Estoque Chapas</h1>", unsafe_allow_html=True)
        _, col, _ = st.columns([1, 3, 1])
        with col:
            st.markdown("<div class='login-card'>", unsafe_allow_html=True)
            u = st.text_input("Utilizador").lower().strip()
            p = st.text_input("Senha", type="password").strip()
            if st.button("ENTRAR NO SISTEMA"):
                if u in users and users[u]["password"] == p:
                    st.session_state.logado = True
                    st.session_state.user = users[u]
                    st.rerun()
                else: st.error("Utilizador ou Senha incorretos.")
            st.markdown("</div>", unsafe_allow_html=True)
        return

    # Navegação
    nav = ["📊 Dashboard", "🔄 Movimentações", "👤 Conta"]
    if st.session_state.user.get('nivel') == "admin": 
        nav += ["📂 Base Mestra", "👥 Gestão de Acessos"]
    
    menu = st.sidebar.radio("Navegação", nav)
    st.sidebar.divider()
    if st.sidebar.button("Sair (Logout)"):
        st.session_state.logado = False
        st.rerun()

    # --- PÁGINA: DASHBOARD ---
    if menu == "📊 Dashboard":
        st.title("📊 Painel de Controle de Estoque")
        df_base = calcular_saldos()
        
        if df_base.empty:
            st.info("💡 Inventário vazio. Administrador: carregue a Base Mestra.")
        else:
            # Filtros na Barra Lateral
            st.sidebar.markdown("### 🔍 Filtros")
            f_obra = st.sidebar.multiselect("Obra", sorted(df_base["Obra"].unique()))
            f_pep = st.sidebar.multiselect("Elemento PEP", sorted(df_base["ElementoPEP"].unique()))
            
            # FILTRO: Grau
            f_grau = st.sidebar.multiselect("Grau", sorted(df_base["Grau"].unique()))
            
            f_esp = st.sidebar.multiselect("Espessura", sorted(df_base["Esp"].unique()))
            f_larg = st.sidebar.multiselect("Largura", sorted(df_base["Larg"].unique()))
            f_comp = st.sidebar.multiselect("Comprimento", sorted(df_base["Comp"].unique()))
            
            f_lvm = st.sidebar.text_input("Pesquisar LVM").upper().strip()

            # Aplicação dos filtros
            df_v = df_base.copy()
            if f_obra: df_v = df_v[df_v["Obra"].isin(f_obra)]
            if f_pep: df_v = df_v[df_v["ElementoPEP"].isin(f_pep)]
            if f_grau: df_v = df_v[df_v["Grau"].isin(f_grau)]
            if f_esp: df_v = df_v[df_v["Esp"].isin(f_esp)]
            if f_larg: df_v = df_v[df_v["Larg"].isin(f_larg)]
            if f_comp: df_v = df_v[df_v["Comp"].isin(f_comp)]
            if f_lvm: df_v = df_v[df_v["LVM"].str.contains(f_lvm)]

            # KPIs
            c1, c2, c3 = st.columns(3)
            c1.metric("Peças Totais", f"{int(df_v['Saldo_Pecas'].sum()):,}")
            c2.metric("Peso Total (KG)", f"{df_v['Saldo_KG'].sum():,.1f}")
            c3.metric("LVMs Ativas", len(df_v["LVM"].unique()))
            
            st.divider()
            
            # Gráficos
            g1, g2 = st.columns(2)
            with g1:
                # CORREÇÃO: Removido erro de aspas na linha abaixo
                pie_data = df_v.groupby("Obra")["Saldo_Pecas"].sum().reset_index().nlargest(10, "Saldo_Pecas")
                fig1 = px.pie(pie_data, values="Saldo_Pecas", names="Obra", title="Top 10 Obras (Peças)", hole=0.4)
                st.plotly_chart(fig1, use_container_width=True)
            with g2:
                bar_data = df_v.groupby("Grau")["Saldo_KG"].sum().reset_index()
                fig2 = px.bar(bar_data, x="Grau", y="Saldo_KG", title="Peso por Grau de Material", color="Grau")
                st.plotly_chart(fig2, use_container_width=True)
            
            st.divider()
            if st.button("📥 Exportar Relatório PDF"):
                pdf_data = gerar_pdf(df_v)
                st.download_button("💾 Baixar PDF", pdf_data, f"estoque_{datetime.now().strftime('%d%m%Y')}.pdf", "application/pdf")
            
            st.dataframe(df_v, use_container_width=True, hide_index=True)

    # --- PÁGINA: MOVIMENTAÇÕES ---
    elif menu == "🔄 Movimentações":
        st.title("🔄 Registar Movimento")
        base = carregar_base_mestra()
        if base.empty: st.warning("Carregue a Base Mestra primeiro."); return
        
        # ABAS: Individual e Lote
        tab_ind, tab_lote = st.tabs(["📝 Lançamento Individual", "📁 Importação em Lote (Excel)"])
        
        with tab_ind:
            with st.form("f_mov"):
                col1, col2 = st.columns(2)
                tipo = col1.selectbox("Operação", ["SAIDA", "ENTRADA", "TMA", "TDMA"])
                mat = col2.selectbox("Material", sorted(base["Material"].unique()))
                lvm = st.text_input("LVM").upper().strip()
                qtd = st.number_input("Quantidade", min_value=1, step=1)
                obr = st.text_input("Obra").upper().strip()
                pep = st.text_input("PEP").upper().strip()
                if st.form_submit_button("GRAVAR NO ESTOQUE"):
                    get_coll("movements").add({
                        "Tipo": tipo, "Material": mat, "LVM": lvm, "Qtde": qtd, 
                        "Obra": obr, "ElementoPEP": pep, 
                        "Data": datetime.now().strftime('%d/%m/%Y'),
                        "timestamp": firestore.SERVER_TIMESTAMP
                    })
                    st.success("Movimentação registada!")
                    time.sleep(1)
                    st.rerun()

        with tab_lote:
            st.subheader("📁 Upload de Arquivos de Movimentação")
            st.info("O Excel deve conter as colunas: Material, LVM, Qtde, Obra, ElementoPEP, Data")
            tipo_up = st.selectbox("Tipo para este ficheiro", ["SAIDA", "ENTRADA", "TMA", "TDMA"])
            up_mov = st.file_uploader(f"Selecione o ficheiro de {tipo_up}", type="xlsx")
            
            if up_mov and st.button(f"🚀 Importar Registos de {tipo_up}"):
                try:
                    df_up = pd.read_excel(up_mov, dtype=str)
                    coll = get_coll("movements")
                    ts = firestore.SERVER_TIMESTAMP
                    for _, r in df_up.iterrows():
                        d = r.to_dict()
                        d["Tipo"] = tipo_up
                        d["timestamp"] = ts
                        coll.add(d)
                    st.success(f"Importação de {len(df_up)} registros concluída!")
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    st.error(f"Erro ao processar o ficheiro: {e}")

    # --- PÁGINA: EQUIPA ---
    elif menu == "👥 Gestão de Acessos":
        st.title("👥 Gestão de Utilizadores")
        with st.form("f_add_user"):
            new_u = st.text_input("Novo Utilizador").lower().strip()
            new_p = st.text_input("Senha")
            new_n = st.selectbox("Nível", ["operador", "admin", "consulta"])
            if st.form_submit_button("CRIAR UTILIZADOR"):
                if new_u and new_p:
                    get_coll("users").add({"username": new_u, "password": new_p, "nivel": new_n})
                    st.success(f"Utilizador {new_u} criado!")
                    st.rerun()
        st.divider()
        st.subheader("Utilizadores Ativos")
        for u_name, u_data in users.items():
            st.write(f"• **{u_name}** ({u_data['nivel']})")

    # --- PÁGINA: BASE MESTRA ---
    elif menu == "📂 Base Mestra":
        st.title("📂 Sincronizar Catálogo")
        f = st.file_uploader("Ficheiro Excel da Base Mestra", type="xlsx")
        if f and st.button("🚀 ENVIAR PARA A NUVEM"):
            df_m = pd.read_excel(f, dtype=str)
            coll = get_coll("master_csv_store")
            for d in coll.stream(): d.reference.delete()
            csv_t = df_m.to_csv(index=False)
            size = 800000
            for i, p in enumerate([csv_t[i:i+size] for i in range(0, len(csv_t), size)]):
                coll.document(f"p_{i}").set({"part": i, "csv_data": p})
            st.cache_data.clear()
            st.success("Base Mestra sincronizada!")
            st.balloons()

if __name__ == "__main__":
    main()