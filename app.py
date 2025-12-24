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
            return None, "ERRO: Secrets não configuradas no Streamlit Cloud."
        
        config = dict(st.secrets["firebase"])
        
        # Tratamento da Private Key para evitar erros de Padding/PEM
        if "private_key" in config:
            pk = config["private_key"]
            pk = pk.replace("\\n", "\n")
            pk = pk.strip().strip('"').strip("'")
            if "-----BEGIN PRIVATE KEY-----" not in pk:
                pk = "-----BEGIN PRIVATE KEY-----\n" + pk
            if "-----END PRIVATE KEY-----" not in pk:
                pk = pk + "\n-----END PRIVATE KEY-----\n"
            config["private_key"] = pk
            
        app_name = "marcius-stock-v23-final"
        
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
PROJECT_ID = "marcius-stock-pro-v23"

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
    else: inv["Impacto"] = 0
    inv["Saldo_Pecas"] = inv["Qtd_Inicial"] + inv["Impacto"]
    inv["Saldo_KG"] = inv["Saldo_Pecas"] * inv["Peso"]
    return inv[inv["Saldo_Pecas"] > 0].sort_values(by=["Obra", "LVM"])

# --- 4. INTERFACE ---

def main():
    st.set_page_config(page_title="Marcius Stock Pro", layout="wide", page_icon="🏗️")

    st.markdown("""
        <style>
            .stButton>button { width: 100%; border-radius: 12px; height: 3.8em; font-weight: bold; background-color: #f8f9fa; border: 1px solid #d1d3e2; }
            .stTextInput>div>div>input { height: 3.5em; border-radius: 8px; }
            .login-card { background-color: #ffffff; padding: 25px; border-radius: 15px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
        </style>
    """, unsafe_allow_html=True)

    with st.sidebar:
        if os.path.exists("logo_empresa.png"):
            st.image("logo_empresa.png", use_container_width=True)
        else:
            st.markdown("<h2 style='text-align: center; color: #FF4B4B;'>🏗️ STOCK PRO</h2>", unsafe_allow_html=True)
        
        if db is not None:
            st.markdown("<div style='color: green; font-size: 0.85em; text-align: center;'>● Sistema Conectado</div>", unsafe_allow_html=True)
        else:
            st.markdown("<div style='color: red; font-size: 0.85em; text-align: center;'>● Sistema Desconectado</div>", unsafe_allow_html=True)
        st.divider()

    if db is None:
        st.error("🔴 FALHA CRÍTICA DE CONEXÃO")
        st.code(erro_conexao)
        return

    users = carregar_users()
    if "logado" not in st.session_state: st.session_state.logado = False
    
    # --- TELA DE LOGIN OTIMIZADA ---
    if not st.session_state.logado:
        st.markdown("<h1 style='text-align: center;'>🏗️ Gestão de Stock</h1>", unsafe_allow_html=True)
        
        col_a, col_b, col_c = st.columns([1, 5, 1]) if st.sidebar.checkbox("Mobile", True) else st.columns([1, 2, 1])
        
        with col_b:
            st.markdown("<div class='login-card'>", unsafe_allow_html=True)
            u_in = st.text_input("Nome de Utilizador").lower().strip()
            p_in = st.text_input("Palavra-passe", type="password").strip()
            
            if st.button("ENTRAR NO SISTEMA"):
                if u_in in users and users[u_in]["password"] == p_in:
                    st.session_state.logado = True
                    st.session_state.user = users[u_in]
                    st.rerun()
                else:
                    st.error("Utilizador ou senha incorretos.")
            
            st.markdown("</div>", unsafe_allow_html=True)
            
            # Ajuda para quem não consegue entrar
            with st.expander("❓ Não consegue entrar? Clique aqui"):
                st.write("1. Verifique se o teclado colocou a primeira letra em maiúscula.")
                st.write("2. Certifique-se de que não há espaços após a senha.")
                if st.button("🔄 FORÇAR LIMPEZA DE CACHE"):
                    st.cache_data.clear()
                    st.rerun()
        return

    # --- MENU PRINCIPAL ---
    nav = ["📊 Dashboard", "🔄 Movimentações", "👤 Minha Conta"]
    if st.session_state.user['nivel'] == "Admin": nav += ["📂 Base Mestra", "👥 Gestão de Acessos"]
    
    menu = st.sidebar.radio("Menu Principal", nav)
    st.sidebar.markdown(f"**Logado como:** {st.session_state.user['username']}")
    
    if st.sidebar.button("Terminar Sessão"):
        st.session_state.logado = False
        st.rerun()

    # PÁGINAS (Resumidas para performance mobile)
    if menu == "📊 Dashboard":
        st.title("📊 Saldo Atual")
        df = calcular_saldos()
        if df.empty:
            st.info("💡 Carregue a Base Mestra para ver o saldo.")
        else:
            st.sidebar.markdown("### 🔍 Filtros")
            f_mat = st.sidebar.multiselect("Material", sorted(df["Material"].unique()))
            f_obra = st.sidebar.multiselect("Obra", sorted(df["Obra"].unique()))
            f_lvm = st.sidebar.text_input("Busca LVM").upper().strip()

            df_v = df.copy()
            if f_mat: df_v = df_v[df_v["Material"].isin(f_mat)]
            if f_obra: df_v = df_v[df_v["Obra"].isin(f_obra)]
            if f_lvm: df_v = df_v[df_v["LVM"].str.contains(f_lvm)]

            k1, k2, k3 = st.columns(3)
            k1.metric("Peças", f"{int(df_v['Saldo_Pecas'].sum()):,}")
            k2.metric("Total KG", f"{df_v['Saldo_KG'].sum():,.2f}")
            k3.metric("LVMs", len(df_v["LVM"].unique()))
            
            st.dataframe(df_v, use_container_width=True, hide_index=True)

    elif menu == "🔄 Movimentações":
        st.title("🔄 Registros")
        base = carregar_base_mestra()
        if base.empty: st.error("Catálogo vazio."); return
        
        op = st.selectbox("Operação", ["SAIDA", "ENTRADA", "TMA", "TDMA"])
        mat = st.selectbox("Material", sorted(base["Material"].unique()))
        lvm = st.text_input("LVM").upper().strip()
        qtd = st.number_input("Qtd", min_value=1, step=1)
        obr = st.text_input("Obra").upper().strip()
        pep = st.text_input("PEP").upper().strip()
        
        if st.button("GRAVAR REGISTRO"):
            coll = get_coll("movements")
            dt = datetime.now().strftime("%d/%m/%Y")
            coll.add({"Tipo": op, "Material": mat, "LVM": lvm, "Qtde": qtd, "Obra": obr, "ElementoPEP": pep, "Data": dt, "timestamp": firestore.SERVER_TIMESTAMP})
            st.success("Salvo!"); time.sleep(1); st.rerun()

    elif menu == "👤 Minha Conta":
        st.title("👤 Alterar Senha")
        with st.form("f_p"):
            nova = st.text_input("Nova Senha", type="password")
            if st.form_submit_button("Atualizar"):
                ref = get_coll("users").where("username", "==", st.session_state.user['username']).stream()
                for d in ref: d.reference.update({"password": nova})
                st.success("Senha alterada!")

    elif menu == "👥 Gestão de Acessos":
        st.title("👥 Usuários")
        with st.form("f_u"):
            nu = st.text_input("Novo Usuário").lower().strip()
            np = st.text_input("Senha", type="password")
            nv = st.selectbox("Nível", ["Operador", "Admin"])
            if st.form_submit_button("Cadastrar"):
                get_coll("users").add({"username": nu, "password": np, "nivel": nv})
                st.success("Criado!"); st.rerun()
        st.divider()
        for n, d in users.items():
            c1, c2 = st.columns([4, 1])
            c1.write(f"**{n}** | {d['nivel']}")
            if n != "marcius.arruda" and c2.button("Remover", key=f"d_{n}"):
                docs = get_coll("users").where("username", "==", n).stream()
                for doc in docs: doc.reference.delete()
                st.rerun()

    elif menu == "📂 Base Mestra":
        st.title("📂 Sincronizar Catálogo")
        f = st.file_uploader("Subir Excel", type="xlsx")
        if f and st.button("🚀 SINCRONIZAR"):
            df_m = pd.read_excel(f, dtype=str)
            coll = get_coll("master_csv_store")
            for d in coll.stream(): d.reference.delete()
            csv_t = df_m.to_csv(index=False)
            size = 800000
            for i, p in enumerate([csv_t[i:i+size] for i in range(0, len(csv_t), size)]):
                coll.document(f"p_{i}").set({"part": i, "csv_data": p})
            st.cache_data.clear()
            st.success("Catálogo Sincronizado!")

if __name__ == "__main__":
    main()