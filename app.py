# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import io
import time
import plotly.express as px

# --- 1. CONFIGURAÇÃO DE SEGURANÇA E CONEXÃO ---

def inicializar_firebase():
    """Inicializa o Firebase utilizando as Secrets do Streamlit."""
    try:
        if "firebase" not in st.secrets:
            return None, "Aba 'Secrets' não configurada no Streamlit Cloud."
        
        config = dict(st.secrets["firebase"])
        app_id = "marcius-stock-pro-v3"
        
        if not firebase_admin._apps:
            cred = credentials.Certificate(config)
            firebase_admin.initialize_app(cred, name=app_id)
        
        app_inst = firebase_admin.get_app(app_id)
        return firestore.client(app=app_inst), None
    except Exception as e:
        return None, str(e)

db, erro_conexao = inicializar_firebase()
APP_ID = "marcius-stock-pro-v3"

# --- 2. GESTÃO DE DADOS (FIRESTORE) ---

def get_coll(nome_colecao):
    if db is None: return None
    return db.collection("artifacts").document(APP_ID).collection("public").document("data").collection(nome_colecao)

@st.cache_data(ttl=30)
def carregar_catalogo_nuvem():
    coll = get_coll("master_csv_store")
    if coll is None: return pd.DataFrame()
    try:
        docs = coll.stream()
        partes = {}
        for d in docs:
            obj = d.to_dict()
            if "part" in obj: partes[obj["part"]] = obj["csv_data"]
        
        if not partes: return pd.DataFrame()
        
        csv_full = "".join([partes[k] for k in sorted(partes.keys())])
        df = pd.read_csv(io.StringIO(csv_full), dtype=str)
        
        if not df.empty:
            df["Peso"] = pd.to_numeric(df["Peso"].astype(str).str.replace(",", "."), errors="coerce").fillna(0)
            for c in ["Larg", "Comp", "Esp"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c].astype(str).str.replace(",", "."), errors="coerce").fillna(0)
        return df
    except: return pd.DataFrame()

def carregar_movimentacoes_nuvem():
    coll = get_coll("movements")
    if coll is None: return pd.DataFrame()
    try:
        docs = coll.stream()
        data = [doc.to_dict() for doc in docs]
        return pd.DataFrame(data)
    except: return pd.DataFrame()

# --- 3. LÓGICA DE INVENTÁRIO ---

def calcular_estoque_atual():
    base = carregar_catalogo_nuvem()
    if base.empty: return pd.DataFrame()
    
    cols_tec = ["Material", "Obra", "ElementoPEP", "Grau", "Esp", "Larg", "Comp"]
    for c in cols_tec:
        if c in base.columns:
            base[c] = base[c].astype(str).str.strip().str.upper()

    inv = base.groupby(cols_tec).agg({
        "DescritivoMaterial": "first", "Peso": "first", "LVM": "first", "Material": "count"
    }).rename(columns={"Material": "Pecas_Iniciais"}).reset_index()
    
    movs = carregar_movimentacoes_nuvem()
    if not movs.empty:
        for c in ["Material", "Obra", "ElementoPEP"]:
            if c in movs.columns: movs[c] = movs[c].astype(str).str.strip().str.upper()
        
        movs["Qtd_N"] = pd.to_numeric(movs["Qtde"], errors="coerce").fillna(0)
        # Impacto: ENTRADA/TDMA soma | SAIDA/TMA subtrai
        movs["Impacto"] = movs.apply(lambda x: x["Qtd_N"] if x["Tipo"] in ["ENTRADA", "TDMA"] else -x["Qtd_N"], axis=1)
        
        resumo_movs = movs.groupby(["Material", "Obra", "ElementoPEP"])["Impacto"].sum().reset_index()
        inv = pd.merge(inv, resumo_movs, on=["Material", "Obra", "ElementoPEP"], how="left").fillna(0)
    else:
        inv["Impacto"] = 0
        
    inv["Saldo_Pecas"] = inv["Pecas_Iniciais"] + inv["Impacto"]
    inv["Saldo_KG"] = inv["Saldo_Pecas"] * inv["Peso"]
    return inv[inv["Saldo_Pecas"] > 0]

# --- 4. INTERFACE ---

def main():
    st.set_page_config(page_title="Marcius Stock Pro", layout="wide")

    # LOGO E ESTILO
    st.sidebar.markdown("<h1 style='text-align: center; color: #FF4B4B;'>🏗️ MARCIUS STOCK</h1>", unsafe_allow_html=True)
    st.sidebar.markdown("<p style='text-align: center;'>Gestão de Chapas e Perfis</p>", unsafe_allow_html=True)
    st.sidebar.divider()

    if db is None:
        st.error("🔴 FIREBASE DESCONECTADO")
        return
    
    if "logado" not in st.session_state: st.session_state.logado = False
    if not st.session_state.logado:
        st.title("Acesso Restrito")
        u = st.text_input("Utilizador").lower()
        p = st.text_input("Senha", type="password")
        if st.button("Entrar"):
            if u == "marcius.arruda" and p == "MwsArruda":
                st.session_state.logado = True
                st.rerun()
        return

    menu = st.sidebar.radio("Navegação", ["📊 Dashboard", "🔄 Movimentações", "📂 Base Mestra"])

    # --- DASHBOARD COM GRÁFICOS E TODOS OS FILTROS ---
    if menu == "📊 Dashboard":
        st.title("📊 Controle de Saldos")
        df = calcular_estoque_atual()
        
        if df.empty:
            st.info("Nuvem vazia. Carregue o catálogo na aba 'Base Mestra'.")
        else:
            with st.sidebar.expander("🔍 Filtros Técnicos", expanded=True):
                f_obra = st.multiselect("Obra", sorted(df["Obra"].unique()))
                f_pep = st.multiselect("PEP", sorted(df["ElementoPEP"].unique()))
                f_grau = st.multiselect("Grau", sorted(df["Grau"].unique()))
                f_esp = st.multiselect("Espessura", sorted(df["Esp"].unique()))
                f_larg = st.multiselect("Largura", sorted(df["Larg"].unique()))
                f_comp = st.multiselect("Comprimento", sorted(df["Comp"].unique()))

            df_v = df.copy()
            if f_obra: df_v = df_v[df_v["Obra"].isin(f_obra)]
            if f_pep: df_v = df_v[df_v["ElementoPEP"].isin(f_pep)]
            if f_grau: df_v = df_v[df_v["Grau"].isin(f_grau)]
            if f_esp: df_v = df_v[df_v["Esp"].isin(f_esp)]
            if f_larg: df_v = df_v[df_v["Larg"].isin(f_larg)]
            if f_comp: df_v = df_v[df_v["Comp"].isin(f_comp)]

            # KPIs
            c1, c2, c3 = st.columns(3)
            c1.metric("Total de Peças", f"{int(df_v['Saldo_Pecas'].sum()):,}")
            c2.metric("Peso Total (KG)", f"{df_v['Saldo_KG'].sum():,.2f}")
            c3.metric("Obras Ativas", len(df_v["Obra"].unique()))

            # GRÁFICOS
            st.divider()
            g1, g2 = st.columns(2)
            with g1:
                st.subheader("Obras com maior volume")
                fig1 = px.pie(df_v.groupby("Obra")["Saldo_Pecas"].sum().reset_index().nlargest(8, "Saldo_Pecas"), values="Saldo_Pecas", names="Obra", hole=0.3)
                st.plotly_chart(fig1, use_container_width=True)
            with g2:
                st.subheader("Peso por Grau de Aço")
                fig2 = px.bar(df_v.groupby("Grau")["Saldo_KG"].sum().reset_index(), x="Grau", y="Saldo_KG", color="Grau")
                st.plotly_chart(fig2, use_container_width=True)

            st.dataframe(df_v, use_container_width=True, hide_index=True)

    # --- MOVIMENTAÇÕES (SAIDA, ENTRADA, TMA, TDMA) ---
    elif menu == "🔄 Movimentações":
        st.title("🔄 Registro de Movimento")
        base = carregar_catalogo_nuvem()
        if base.empty: st.error("Carregue a base primeiro."); return

        with st.form("mov_form"):
            c1, c2 = st.columns(2)
            t = c1.selectbox("Operação", ["SAIDA", "ENTRADA", "TMA", "TDMA"])
            mat = c1.selectbox("Material", sorted(base["Material"].unique()))
            qtd = c2.number_input("Qtd Peças", min_value=1, step=1)
            obr = c2.text_input("Obra").upper()
            pep = st.text_input("Elemento PEP").upper()
            if st.form_submit_button("Confirmar Registro"):
                data = {"Tipo": t, "Material": mat, "Qtde": qtd, "Obra": obr, "ElementoPEP": pep, "Data": datetime.now().strftime("%d/%m/%Y"), "timestamp": firestore.SERVER_TIMESTAMP}
                get_coll("movements").add(data)
                st.success("Registrado com sucesso!")
                time.sleep(1)
                st.rerun()

    elif menu == "📂 Base Mestra":
        st.title("📂 Catálogo Principal")
        f = st.file_uploader("Subir Excel", type=["xlsx"])
        if f:
            df_up = pd.read_excel(f, dtype=str)
            st.success(f"{len(df_up)} itens carregados.")
            if st.button("🚀 Sincronizar Tudo"):
                coll = get_coll("master_csv_store")
                for d in coll.stream(): d.reference.delete()
                buffer = io.StringIO()
                df_up.to_csv(buffer, index=False)
                csv_text = buffer.getvalue()
                tamanho = 800000
                partes = [csv_text[i:i+tamanho] for i in range(0, len(csv_text), tamanho)]
                prog = st.progress(0)
                for idx, p in enumerate(partes):
                    coll.document(f"p_{idx}").set({"part": idx, "csv_data": p})
                    prog.progress((idx+1)/len(partes))
                st.cache_data.clear()
                st.success("Concluído!")
                st.rerun()

if __name__ == "__main__":
    main()