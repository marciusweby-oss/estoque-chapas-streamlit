# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import io
import time

# --- 1. CONFIGURAÇÃO DE SEGURANÇA ---

def inicializar_firebase():
    """Tenta conectar ao Firebase usando as Secrets do Streamlit."""
    try:
        if "firebase" not in st.secrets:
            return None, "Aba 'Secrets' não configurada no Streamlit Cloud."
        
        config = dict(st.secrets["firebase"])
        app_id = "marcius-stock-v1"
        
        if not firebase_admin._apps:
            cred = credentials.Certificate(config)
            firebase_admin.initialize_app(cred, name=app_id)
        
        app_inst = firebase_admin.get_app(app_id)
        return firestore.client(app=app_inst), None
    except Exception as e:
        return None, str(e)

# Inicialização Global
db, erro_conexao = inicializar_firebase()
APP_ID = "marcius-stock-v1"

# --- 2. FUNÇÕES DE DADOS (PROTOCOLO SUPER-CSV) ---

def get_coll(nome_colecao):
    if db is None: return None
    return db.collection("artifacts").document(APP_ID).collection("public").document("data").collection(nome_colecao)

@st.cache_data(ttl=60)
def carregar_dados_nuvem():
    coll = get_coll("estoque_master")
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
        
        # Converte números
        if not df.empty:
            df["Peso"] = pd.to_numeric(df["Peso"].astype(str).str.replace(",", "."), errors="coerce").fillna(0)
        return df
    except: return pd.DataFrame()

def guardar_dados_nuvem(df):
    coll = get_coll("estoque_master")
    if coll is None: return False
    try:
        # Limpa antigo
        for d in coll.stream(): d.reference.delete()
        
        # Compacta em CSV para suportar 10.000+ linhas
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        csv_text = buffer.getvalue()
        
        # Divide em partes de 800KB
        tamanho = 800000
        partes = [csv_text[i:i+tamanho] for i in range(0, len(csv_text), tamanho)]
        
        prog = st.progress(0, text="Sincronizando...")
        for idx, p in enumerate(partes):
            coll.document(f"p_{idx}").set({"part": idx, "csv_data": p})
            prog.progress((idx+1)/len(partes))
        
        st.cache_data.clear()
        return True
    except: return False

# --- 3. INTERFACE ---

def main():
    st.set_page_config(page_title="Marcius Stock", layout="wide")

    # TESTE DE CONEXÃO (O SEMÁFORO)
    if db is None:
        st.error("🔴 FIREBASE DESCONECTADO")
        st.warning(f"Motivo: {erro_conexao}")
        st.info("👉 Vá em Settings > Secrets no Streamlit Cloud e cole suas chaves.")
        return
    else:
        st.sidebar.success("🟢 FIREBASE CONECTADO")

    # Login simples
    if "logado" not in st.session_state: st.session_state.logado = False
    if not st.session_state.logado:
        st.header("🏗️ Acesso ao Sistema")
        u = st.text_input("Utilizador")
        p = st.text_input("Palavra-passe", type="password")
        if st.button("Entrar"):
            if u == "marcius.arruda" and p == "MwsArruda":
                st.session_state.logado = True
                st.rerun()
        return

    # Menu
    menu = st.sidebar.radio("Navegação", ["📊 Dashboard", "📂 Base Mestra"])
    
    if menu == "📊 Dashboard":
        st.title("📊 Dashboard de Chapas")
        df = carregar_dados_nuvem()
        
        if df.empty:
            st.info("Nuvem vazia. Carregue o catálogo em 'Base Mestra'.")
        else:
            # Filtros
            obra = st.sidebar.multiselect("Filtrar Obra", sorted(df["Obra"].unique()))
            df_v = df.copy()
            if obra: df_v = df_v[df_v["Obra"].isin(obra)]
            
            c1, c2 = st.columns(2)
            c1.metric("Total de Peças", f"{len(df_v):,}")
            c2.metric("Peso Total (KG)", f"{df_v['Peso'].sum():,.2f}")
            st.dataframe(df_v, use_container_width=True)

    elif menu == "📂 Base Mestra":
        st.title("📂 Carregar Catálogo")
        f = st.file_uploader("Subir Excel", type=["xlsx"])
        if f:
            df_up = pd.read_excel(f, dtype=str)
            st.success(f"{len(df_up)} linhas prontas.")
            if st.button("🚀 Sincronizar com a Nuvem"):
                if guardar_dados_nuvem(df_up):
                    st.success("Sincronizado!")
                    st.balloons()

if __name__ == "__main__":
    main()