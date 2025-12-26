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
            
        app_name = "marcius-estoque-v38"
        
        if not firebase_admin._apps:
            cred = credentials.Certificate(config)
            firebase_admin.initialize_app(cred, name=app_name)
        
        app_inst = firebase_admin.get_app(app_name)
        return firestore.client(app=app_inst), None
    except Exception as e:
        return None, f"Erro: {str(e)}"

db, erro_conexao = inicializar_firebase()
PROJECT_ID = "marcius-estoque-pro-v38"

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
        
        # Padronização de Colunas: Garante que o código do material seja identificado
        if 'CodigodoMaterial' in df.columns:
            df.rename(columns={'CodigodoMaterial': 'Material'}, inplace=True)
            
        return df
    except: return pd.DataFrame()

def carregar_movimentos():
    coll = get_coll("movements")
    if coll is None: return pd.DataFrame()
    try:
        docs = coll.stream()
        df = pd.DataFrame([d.to_dict() for d in docs])
        if not df.empty:
            if 'CodigodoMaterial' in df.columns:
                df.rename(columns={'CodigodoMaterial': 'Material'}, inplace=True)
        return df
    except: return pd.DataFrame()

def carregar_users():
    coll = get_coll("users")
    if coll is None: return {}
    try:
        docs = coll.stream()
        return {d.to_dict()["username"].lower().strip(): d.to_dict() for d in docs}
    except: return {}

# --- 3. LÓGICA DE CÁLCULO ---

def calcular_saldos():
    base = carregar_base_mestra()
    if base.empty: return pd.DataFrame()
    
    # Colunas Técnicas para Chapas
    cols_limpar = ["LVM", "Material", "Obra", "ElementoPEP", "Grau", "Esp", "Larg", "Comp"]
    
    # Limpeza e Padronização de Dados
    for c in cols_limpar:
        if c in base.columns:
            base[c] = base[c].astype(str).str.strip().str.upper()
            base[c] = base[c].apply(lambda x: x.replace(".0", "") if x.endswith(".0") else x)

    # Agrupamento Inicial
    inv = base.groupby(cols_limpar).agg({
        "DescritivoMaterial": "first", 
        "Peso": "first"
    }).reset_index()
    
    contagem_inicial = base.groupby(["LVM", "Material", "Obra", "ElementoPEP"]).size().reset_index(name='Qtd_Inicial')
    inv = pd.merge(inv, contagem_inicial, on=["LVM", "Material", "Obra", "ElementoPEP"], how="left")
    
    movs = carregar_movimentos()
    if not movs.empty:
        # Padronização nos Movimentos
        for c in ["LVM", "Material", "Obra", "ElementoPEP"]:
            if c in movs.columns:
                movs[c] = movs[c].astype(str).str.strip().str.upper()
                movs[c] = movs[c].apply(lambda x: x.replace(".0", "") if x.endswith(".0") else x)
        
        if "Qtde" in movs.columns:
            movs["Qtd_N"] = pd.to_numeric(movs["Qtde"], errors="coerce").fillna(0)
            # Entradas e TDMA somam ao estoque
            movs["Impacto"] = movs.apply(
                lambda x: x["Qtd_N"] if str(x.get("Tipo", "")).strip().upper() in ["ENTRADA", "TDMA"] 
                else -x["Qtd_N"], axis=1
            )
            
            resumo = movs.groupby(["LVM", "Material", "Obra", "ElementoPEP"])["Impacto"].sum().reset_index()
            inv = pd.merge(inv, resumo, on=["LVM", "Material", "Obra", "ElementoPEP"], how="left").fillna(0)
        else:
            inv["Impacto"] = 0
    else:
        inv["Impacto"] = 0
        
    inv["Saldo_Pecas"] = inv["Qtd_Inicial"] + inv["Impacto"]
    
    # Tratamento Numérico do Peso para cálculos exatos
    inv["Peso_N"] = pd.to_numeric(inv["Peso"].astype(str).str.replace(",", "."), errors="coerce").fillna(0)
    inv["Saldo_KG"] = inv["Saldo_Pecas"] * inv["Peso_N"]
    
    return inv[inv["Saldo_Pecas"] > 0].sort_values(by=["Obra", "LVM"])

# --- 4. RELATÓRIOS ---

def gerar_pdf(df):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("helvetica", "B", 14)
    pdf.cell(0, 10, "Mapa de Estoque - Gestão de Chapas", ln=True, align="C")
    pdf.set_font("helvetica", "", 8)
    pdf.ln(5)
    headers = ["LVM", "Material", "Obra", "Grau", "Qtd", "Kg"]
    col_width = 31.5
    for h in headers: pdf.cell(col_width, 7, h, 1, 0, "C")
    pdf.ln()
    for _, r in df.iterrows():
        pdf.cell(col_width, 6, str(r['LVM'])[:12], 1)
        pdf.cell(col_width, 6, str(r['Material'])[:12], 1)
        pdf.cell(col_width, 6, str(r['Obra'])[:12], 1)
        pdf.cell(col_width, 6, str(r['Grau'])[:12], 1)
        pdf.cell(col_width, 6, str(int(r['Saldo_Pecas'])), 1, 0, "R")
        pdf.cell(col_width, 6, f"{r['Saldo_KG']:,.1f}", 1, 1, "R")
    return pdf.output()

# --- 5. INTERFACE PRINCIPAL ---

def main():
    st.set_page_config(page_title="Gestão de Estoque Chapas", layout="wide", page_icon="🏗️")

    st.markdown("""
        <style>
            .stButton>button { width: 100%; border-radius: 10px; height: 3.5em; font-weight: bold; background-color: #1e3a8a; color: white; }
            .login-card { background: white; padding: 2.5rem; border-radius: 1.5rem; box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1); border: 1px solid #f1f5f9; }
        </style>
    """, unsafe_allow_html=True)

    if db is None:
        st.error("🔴 Sem ligação à base de dados. Verifique os segredos.")
        return

    users = carregar_users()
    if "logado" not in st.session_state: st.session_state.logado = False

    if not st.session_state.logado:
        st.markdown("<br><h1 style='text-align: center; color: #1e3a8a;'>🏗️ Sistema de Gestão de Estoque</h1>", unsafe_allow_html=True)
        _, col, _ = st.columns([1, 2, 1])
        with col:
            st.markdown("<div class='login-card'>", unsafe_allow_html=True)
            u = st.text_input("Utilizador").lower().strip()
            p = st.text_input("Senha", type="password").strip()
            if st.button("ACESSAR ESTOQUE"):
                if u in users and users[u]["password"] == p:
                    st.session_state.logado = True
                    st.session_state.user = users[u]
                    st.rerun()
                else: st.error("Utilizador ou Senha incorretos.")
            st.markdown("</div>", unsafe_allow_html=True)
        return

    # Menu Lateral
    nav = ["📊 Dashboard", "🔄 Movimentações", "👤 Conta"]
    if st.session_state.user.get('nivel') == "admin": 
        nav += ["📂 Base Mestra", "👥 Gestão de Acessos"]
    
    menu = st.sidebar.radio("Navegação", nav)
    st.sidebar.divider()
    if st.sidebar.button("Terminar Sessão"):
        st.session_state.logado = False
        st.rerun()

    # --- TELA: DASHBOARD ---
    if menu == "📊 Dashboard":
        st.title("📊 Controle de Estoque de Chapas")
        df = calcular_saldos()
        
        if df.empty:
            st.info("💡 Nenhum material com saldo positivo encontrado. Carregue a Base Mestra ou registre entradas.")
        else:
            # FILTROS DINÂMICOS NA SIDEBAR
            st.sidebar.header("🔍 Filtros de Busca")
            
            def get_opts(col): return sorted(df[col].unique().tolist())

            f_obra = st.sidebar.multiselect("Obra", get_opts("Obra"))
            f_pep = st.sidebar.multiselect("Elemento PEP", get_opts("ElementoPEP"))
            
            # FILTRO: Grau (Anteriormente Cinza)
            f_grau = st.sidebar.multiselect("Grau", get_opts("Grau"))
            
            f_esp = st.sidebar.multiselect("Espessura", get_opts("Esp"))
            f_larg = st.sidebar.multiselect("Largura", get_opts("Larg"))
            f_comp = st.sidebar.multiselect("Comprimento", get_opts("Comp"))
            f_lvm = st.sidebar.text_input("Pesquisar LVM").upper().strip()

            # Lógica de Filtros Aplicada
            df_v = df.copy()
            if f_obra: df_v = df_v[df_v["Obra"].isin(f_obra)]
            if f_pep: df_v = df_v[df_v["ElementoPEP"].isin(f_pep)]
            if f_grau: df_v = df_v[df_v["Grau"].isin(f_grau)]
            if f_esp: df_v = df_v[df_v["Esp"].isin(f_esp)]
            if f_larg: df_v = df_v[df_v["Larg"].isin(f_larg)]
            if f_comp: df_v = df_v[df_v["Comp"].isin(f_comp)]
            if f_lvm: df_v = df_v[df_v["LVM"].str.contains(f_lvm)]

            # Painel de Indicadores
            c1, c2, c3 = st.columns(3)
            c1.metric("Peças Totais", f"{int(df_v['Saldo_Pecas'].sum()):,}")
            c2.metric("Peso Total (KG)", f"{df_v['Saldo_KG'].sum():,.1f}")
            c3.metric("LVMs Filtradas", len(df_v["LVM"].unique()))
            
            st.divider()
            
            # Análise Visual
            g1, g2 = st.columns(2)
            with g1:
                st.plotly_chart(px.pie(df_v.groupby("Obra")["Saldo_Pecas"].sum().reset_index().nlargest(10, "Saldo_Pecas"), values="Saldo_Pecas", names="Obra", title="Top 10 Obras", hole=0.4), use_container_width=True)
            with g2:
                st.plotly_chart(px.bar(df_v.groupby("Grau")["Saldo_KG"].sum().reset_index(), x="Grau", y="Saldo_KG", title="Peso por Grau", color="Grau"), use_container_width=True)
            
            if st.button("📥 Exportar Estoque Atual (PDF)"):
                pdf_data = gerar_pdf(df_v)
                st.download_button("💾 Baixar Arquivo PDF", pdf_data, f"estoque_marcius_{datetime.now().strftime('%d%m%Y')}.pdf", "application/pdf")
            
            st.dataframe(df_v.drop(columns=["Peso_N", "Impacto"]), use_container_width=True, hide_index=True)

    # --- TELA: MOVIMENTAÇÕES ---
    elif menu == "🔄 Movimentações":
        st.title("🔄 Registro de Entradas e Saídas")
        base_cat = carregar_base_mestra()
        if base_cat.empty: st.error("O catálogo está vazio. Carregue a Base Mestra primeiro."); return
        
        tab1, tab2 = st.tabs(["📝 Registro Individual", "📁 Importação em Lote (Excel)"])
        
        with tab1:
            with st.form("f_ind"):
                tipo = st.selectbox("Operação", ["SAIDA", "ENTRADA", "TMA", "TDMA"])
                mat = st.selectbox("Material", sorted(base_cat["Material"].unique()))
                lvm = st.text_input("LVM").upper().strip()
                qtd = st.number_input("Quantidade", min_value=1, step=1)
                obr = st.text_input("Obra").upper().strip()
                pep = st.text_input("PEP").upper().strip()
                if st.form_submit_button("GRAVAR NO BANCO"):
                    get_coll("movements").add({
                        "Tipo": tipo, "Material": mat, "LVM": lvm, "Qtde": qtd, 
                        "Obra": obr, "ElementoPEP": pep, 
                        "Data": datetime.now().strftime('%d/%m/%Y'), 
                        "timestamp": firestore.SERVER_TIMESTAMP
                    })
                    st.success("Operação gravada com sucesso!"); time.sleep(0.5); st.rerun()
        
        with tab2:
            st.subheader("📁 Processar Ficheiro Excel")
            tp_lote = st.selectbox("Tipo de Movimento no Arquivo", ["ENTRADA", "SAIDA", "TMA", "TDMA"])
            f_lote = st.file_uploader(f"Selecione o Excel de {tp_lote}", type="xlsx")
            if f_lote and st.button("🚀 Iniciar Processamento"):
                df_up = pd.read_excel(f_lote, dtype=str)
                df_up.columns = [str(c).strip() for c in df_up.columns]
                # Normalização de nomes de coluna comuns (CodigodoMaterial -> Material)
                df_up.rename(columns={'CodigodoMaterial': 'Material', 'CodigoMaterial': 'Material'}, inplace=True)
                
                coll = get_coll("movements")
                ts = firestore.SERVER_TIMESTAMP
                p_bar = st.progress(0)
                for i, r in df_up.iterrows():
                    d = {str(k).strip(): str(v).strip() for k, v in r.to_dict().items() if pd.notna(v)}
                    d["Tipo"] = tp_lote
                    d["timestamp"] = ts
                    coll.add(d)
                    p_bar.progress((i + 1) / len(df_up))
                st.success("Importação de lote concluída!"); time.sleep(1); st.rerun()

    # --- TELA: BASE MESTRA ---
    elif menu == "📂 Base Mestra":
        st.title("📂 Gerenciar Catálogo de Materiais")
        st.warning("Atenção: A sincronização irá substituir o catálogo atual na nuvem.")
        f = st.file_uploader("Carregar Ficheiro Excel (Master)", type="xlsx")
        if f and st.button("🚀 SINCRONIZAR CATÁLOGO"):
            df_m = pd.read_excel(f, dtype=str)
            coll = get_coll("master_csv_store")
            # Limpeza do catálogo antigo
            for d in coll.stream(): d.reference.delete()
            # Envio fragmentado
            csv_t = df_m.to_csv(index=False)
            size = 800000
            for i, p in enumerate([csv_t[i:i+size] for i in range(0, len(csv_t), size)]):
                coll.document(f"p_{i}").set({"part": i, "csv_data": p})
            st.cache_data.clear()
            st.success("Catálogo sincronizado com sucesso!"); st.balloons()

if __name__ == "__main__":
    main()