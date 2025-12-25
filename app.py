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
            
        app_name = "marcius-estoque-v31"
        
        if not firebase_admin._apps:
            cred = credentials.Certificate(config)
            firebase_admin.initialize_app(cred, name=app_name)
        
        app_inst = firebase_admin.get_app(app_name)
        return firestore.client(app=app_inst), None
    except Exception as e:
        return None, f"Erro: {str(e)}"

# Inicialização do DB
db, erro_conexao = inicializar_firebase()
PROJECT_ID = "marcius-estoque-pro-v31"

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
        # Se a base estiver vazia, cria o admin padrão na nuvem
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

# --- 4. INTERFACE ---

def main():
    st.set_page_config(page_title="Marcius Estoque Pro", layout="wide", page_icon="🏗️")

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

    # Carrega utilizadores da nuvem
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
                if not u or not p:
                    st.warning("Por favor, preencha todos os campos.")
                elif u in users:
                    if users[u]["password"] == p:
                        st.session_state.logado = True
                        st.session_state.user = users[u]
                        st.success("Acesso confirmado! A carregar...")
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        st.error("Senha incorreta. Verifique maiúsculas/minúsculas.")
                else:
                    st.error(f"Utilizador '{u}' não encontrado na base de dados.")
            st.markdown("</div>", unsafe_allow_html=True)
            
            with st.expander("❓ Ajuda com o Login"):
                st.write(f"**Utilizadores detetados na nuvem:** {', '.join(users.keys())}")
                st.write("1. Garanta que não há espaços antes ou depois da senha.")
                st.write("2. A senha diferencia letras MAIÚSCULAS de minúsculas.")
                if st.button("🔄 Limpar Cache de Sessão"):
                    st.cache_data.clear()
                    st.rerun()
        return

    # Interface após login
    nav = ["📊 Dashboard", "🔄 Movimentações", "👤 Conta"]
    if st.session_state.user.get('nivel') == "admin": 
        nav += ["📂 Base Mestra", "👥 Gestão de Acessos"]
    
    menu = st.sidebar.radio("Navegação", nav)
    st.sidebar.divider()
    st.sidebar.write(f"👤 **{st.session_state.user['username']}**")
    
    if st.sidebar.button("Sair"):
        st.session_state.logado = False
        st.rerun()

    if menu == "📊 Dashboard":
        st.title("📊 Painel de Controle de Estoque")
        df = calcular_saldos()
        if df.empty:
            st.info("💡 Inventário vazio. Administrador: carregue a Base Mestra.")
        else:
            c1, c2, c3 = st.columns(3)
            c1.metric("Peças Totais", f"{int(df['Saldo_Pecas'].sum()):,}")
            c2.metric("Peso Total (KG)", f"{df['Saldo_KG'].sum():,.1f}")
            c3.metric("Itens Únicos", len(df))
            
            st.divider()
            st.dataframe(df, use_container_width=True, hide_index=True)

    elif menu == "🔄 Movimentações":
        st.title("🔄 Registar Movimento")
        base = carregar_base_mestra()
        if base.empty: st.warning("Carregue a Base Mestra primeiro."); return
        
        with st.form("f_mov"):
            tipo = st.selectbox("Operação", ["SAIDA", "ENTRADA", "TMA", "TDMA"])
            mat = st.selectbox("Material", sorted(base["Material"].unique()))
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
                st.success("Movimentação registada com sucesso!")
                time.sleep(1)
                st.rerun()

    elif menu == "👥 Gestão de Acessos":
        st.title("👥 Gestão de Utilizadores")
        with st.form("f_add_user"):
            new_u = st.text_input("Novo Utilizador").lower().strip()
            new_p = st.text_input("Senha")
            new_n = st.selectbox("Nível", ["operador", "admin", "consulta"])
            if st.form_submit_button("CRIAR UTILIZADOR NA NUVEM"):
                if new_u and new_p:
                    get_coll("users").add({"username": new_u, "password": new_p, "nivel": new_n})
                    st.success(f"Utilizador {new_u} criado com sucesso!")
                    st.rerun()
        
        st.divider()
        st.subheader("Utilizadores Ativos")
        for u_name, u_data in users.items():
            st.write(f"• **{u_name}** ({u_data['nivel']})")

    elif menu == "📂 Base Mestra":
        st.title("📂 Sincronizar Base Mestra")
        f = st.file_uploader("Carregar Excel da Base Mestra", type="xlsx")
        if f and st.button("🚀 ENVIAR PARA A NUVEM"):
            df_m = pd.read_excel(f, dtype=str)
            coll = get_coll("master_csv_store")
            # Limpa dados antigos
            for d in coll.stream(): d.reference.delete()
            # Fragmenta CSV para o Firestore
            csv_t = df_m.to_csv(index=False)
            size = 800000
            parts = [csv_t[i:i+size] for i in range(0, len(csv_t), size)]
            for i, p in enumerate(parts):
                coll.document(f"p_{i}").set({"part": i, "csv_data": p})
            st.cache_data.clear()
            st.success("Base Mestra sincronizada com sucesso!")

if __name__ == "__main__":
    main()