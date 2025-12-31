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
            return None, "ERRO: Secrets não configuradas."
        config = dict(st.secrets["firebase"])
        if "private_key" in config:
            pk = config["private_key"].replace("\\n", "\n").strip().strip('"').strip("'")
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
        return firestore.client(app=firebase_admin.get_app(app_name)), None
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
        df = df.rename(columns={'Cinza': 'Grau', 'cinza': 'Grau', 'Espessura': 'Esp', 'Largura': 'Larg', 'Comprimento': 'Comp'})
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
        df = pd.DataFrame(dados) if dados else pd.DataFrame()
        return df.rename(columns={'Cinza': 'Grau', 'cinza': 'Grau', 'Espessura': 'Esp', 'Largura': 'Larg', 'Comprimento': 'Comp'})
    except: return pd.DataFrame()

def carregar_users():
    coll = get_coll("users")
    if coll is None: return {}
    try:
        docs = coll.stream()
        return {d.to_dict()["username"]: d.to_dict() for d in docs}
    except: return {}

# --- 3. LÓGICA DE NEGÓCIO ---

def calcular_saldos():
    base = carregar_base_mestra()
    if base.empty: return pd.DataFrame()
    chaves = ["LVM", "Material", "Obra", "ElementoPEP", "Grau", "Esp", "Larg", "Comp"]
    for c in chaves:
        if c in base.columns: base[c] = base[c].astype(str).str.strip().str.upper()
    inv = base.groupby(chaves).agg({"DescritivoMaterial": "first", "Peso": "first", "Material": "count"}).rename(columns={"Material": "Qtd_Inicial"}).reset_index()
    movs = carregar_movimentos()
    if not movs.empty and "Tipo" in movs.columns:
        for c in chaves:
            if c in movs.columns: movs[c] = movs[c].astype(str).str.strip().str.upper()
        movs["Qtd_N"] = pd.to_numeric(movs["Qtde"], errors="coerce").fillna(0)
        movs["Impacto"] = movs.apply(lambda x: x["Qtd_N"] if str(x.get("Tipo","")).upper() in ["ENTRADA", "TDMA"] else -x["Qtd_N"], axis=1)
        resumo = movs.groupby(chaves)["Impacto"].sum().reset_index()
        inv = pd.merge(inv, resumo, on=chaves, how="left").fillna(0)
    else: inv["Impacto"] = 0
    inv["Saldo_Pecas"] = inv["Qtd_Inicial"] + inv["Impacto"]
    inv["Saldo_KG"] = inv["Saldo_Pecas"] * pd.to_numeric(inv["Peso"], errors="coerce").fillna(0)
    return inv[inv["Saldo_Pecas"] > 0].sort_values(by=["Obra", "LVM"])

# --- 4. EXPORTAÇÃO PDF ---

class EstoquePDF(FPDF):
    def header(self):
        self.set_font("helvetica", "B", 14)
        self.cell(0, 10, "MAPA DE ESTOQUE - GESTÃO DE ESTOQUE", ln=True, align="C")
        self.ln(10)

def gerar_pdf(df):
    pdf = EstoquePDF()
    pdf.add_page()
    pdf.set_font("helvetica", "B", 8)
    cols = ["LVM", "Obra", "Grau", "Esp.", "Qtd"]
    for c in cols: pdf.cell(38, 8, c, 1, 0, "C")
    pdf.ln()
    pdf.set_font("helvetica", "", 7)
    for _, r in df.iterrows():
        pdf.cell(38, 7, str(r['LVM']), 1)
        pdf.cell(38, 7, str(r['Obra']), 1)
        pdf.cell(38, 7, str(r['Grau']), 1)
        pdf.cell(38, 7, str(r['Esp']), 1)
        pdf.cell(38, 7, f"{int(r['Saldo_Pecas'])}", 1, 1, "R")
    return bytes(pdf.output(dest='S'))

# --- 5. INTERFACE ---

def main():
    st.set_page_config(page_title="Gestão de Estoque", layout="wide", page_icon="🏗️")

    st.markdown("""
        <style>
            .stButton>button { width: 100%; border-radius: 12px; height: 3.5em; font-weight: bold; background-color: #f8f9fa; border: 1px solid #d1d3e2; }
            .stMetric { background-color: white; padding: 15px; border-radius: 12px; border: 1px solid #eee; }
            .login-card { background-color: #ffffff; padding: 30px; border-radius: 20px; box-shadow: 0 10px 25px rgba(0,0,0,0.05); border: 1px solid #eee; }
        </style>
    """, unsafe_allow_html=True)

    with st.sidebar:
        if os.path.exists("logo_empresa.png"):
            st.image("logo_empresa.png", use_container_width=True)
        if db is not None:
            st.markdown("<div style='color: green; font-size: 0.85em; text-align: center;'>● Ligação Ativa</div>", unsafe_allow_html=True)
        st.divider()

    if "logado" not in st.session_state: st.session_state.logado = False
    users = carregar_users()

    if not st.session_state.logado:
        st.markdown("<br><h1 style='text-align: center; color: #1e3a8a;'>🏗️ Sistema de Gestão de Estoque</h1>", unsafe_allow_html=True)
        col_a, col_b, col_c = st.columns([1, 4, 1])
        with col_b:
            st.markdown("<div class='login-card'>", unsafe_allow_html=True)
            u_in = st.text_input("Usuário").lower().strip()
            p_in = st.text_input("Senha", type="password").strip()
            if st.button("ENTRAR"):
                if u_in in users and users[u_in]["password"] == p_in:
                    st.session_state.logado, st.session_state.user = True, users[u_in]
                    st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)
        return

    nav = ["🔍 Filtros", "📊 Dashboard", "🔄 Movimentações", "👤 Minha Conta"]
    if st.session_state.user['nivel'] == "Admin": nav += ["📂 Base Mestra", "👥 Gestão de Acessos"]
    
    menu = st.sidebar.radio("Navegação", nav)
    
    if st.sidebar.button("🚪 Terminar Sessão"):
        st.session_state.logado = False
        st.rerun()

    # --- ABA: FILTROS (CASCATA TOTAL INCLUINDO LVM) ---
    if menu == "🔍 Filtros":
        st.title("🔍 Configurar Filtros de Estoque")
        df_full = calcular_saldos()
        
        if not df_full.empty:
            filtros_cols = ["Material", "Obra", "Grau", "Esp", "Larg", "Comp"]
            for col in filtros_cols:
                if f"filter_{col}" not in st.session_state: st.session_state[f"filter_{col}"] = []
            if "filter_lvm" not in st.session_state: st.session_state.filter_lvm = ""

            # 1. Primeiro capturamos a LVM para filtrar a base de opções dos outros campos
            st.session_state.filter_lvm = st.text_input("1. Digite a LVM para restringir as opções", value=st.session_state.filter_lvm).upper().strip()

            def obter_opcoes(coluna_alvo):
                temp_df = df_full.copy()
                # Aplica o filtro de texto da LVM primeiro
                if st.session_state.filter_lvm:
                    temp_df = temp_df[temp_df["LVM"].str.contains(st.session_state.filter_lvm, na=False)]
                # Depois aplica os outros multiselects selecionados
                for col in filtros_cols:
                    if col != coluna_alvo and st.session_state[f"filter_{col}"]:
                        temp_df = temp_df[temp_df[col].isin(st.session_state[f"filter_{col}"])]
                return sorted(temp_df[coluna_alvo].unique().tolist())

            st.divider()
            st.write("2. Agora refine pelos campos específicos (as opções abaixo já estão filtradas pela LVM acima):")
            
            col1, col2 = st.columns(2)
            with col1:
                st.session_state.filter_Material = st.multiselect("Material", obter_opcoes("Material"), key="ms_mat", default=[v for v in st.session_state.filter_Material if v in obter_opcoes("Material")])
                st.session_state.filter_Grau = st.multiselect("Grau", obter_opcoes("Grau"), key="ms_grau", default=[v for v in st.session_state.filter_Grau if v in obter_opcoes("Grau")])
                st.session_state.filter_Larg = st.multiselect("Largura", obter_opcoes("Larg"), key="ms_larg", default=[v for v in st.session_state.filter_Larg if v in obter_opcoes("Larg")])
            with col2:
                st.session_state.filter_Obra = st.multiselect("Obra", obter_opcoes("Obra"), key="ms_obra", default=[v for v in st.session_state.filter_Obra if v in obter_opcoes("Obra")])
                st.session_state.filter_Esp = st.multiselect("Espessura", obter_opcoes("Esp"), key="ms_esp", default=[v for v in st.session_state.filter_Esp if v in obter_opcoes("Esp")])
                st.session_state.filter_Comp = st.multiselect("Comprimento", obter_opcoes("Comp"), key="ms_comp", default=[v for v in st.session_state.filter_Comp if v in obter_opcoes("Comp")])
            
            if st.button("🧹 Limpar Todos os Filtros"):
                for col in filtros_cols: st.session_state[f"filter_{col}"] = []
                st.session_state.filter_lvm = ""
                st.rerun()
            
            st.info("Configuração salva. Acesse o '📊 Dashboard' para ver o inventário final.")

    elif menu == "📊 Dashboard":
        st.title("📊 Painel de Estoque Real")
        df_full = calcular_saldos()
        df_v = df_full.copy()
        
        # Aplica os filtros salvos na aba anterior
        if "filter_lvm" in st.session_state and st.session_state.filter_lvm:
            df_v = df_v[df_v["LVM"].str.contains(st.session_state.filter_lvm, na=False)]
        
        filtros_cols = ["Material", "Obra", "Grau", "Esp", "Larg", "Comp"]
        for col in filtros_cols:
            if f"filter_{col}" in st.session_state and st.session_state[f"filter_{col}"]:
                df_v = df_v[df_v[col].isin(st.session_state[f"filter_{col}"])]

        c1, c2, c3 = st.columns(3)
        c1.metric("Peças Filtradas", f"{int(df_v['Saldo_Pecas'].sum()):,}")
        c2.metric("Peso Filtrado (KG)", f"{df_v['Saldo_KG'].sum():,.2f}")
        c3.metric("Registros em Tela", len(df_v))

        st.divider()
        if not df_v.empty:
            col_btn, _ = st.columns([1, 3])
            if col_btn.button("📥 Baixar Relatório PDF"):
                st.download_button("💾 Clique para Salvar", gerar_pdf(df_v), f"estoque_{datetime.now().strftime('%d%m%Y')}.pdf", "application/pdf")

            st.plotly_chart(px.pie(df_v, values="Saldo_Pecas", names="Obra", hole=0.4, title="Peças por Obra"), use_container_width=True)
            st.dataframe(df_v, use_container_width=True, hide_index=True)
        else:
            st.warning("Nenhum dado encontrado para os filtros selecionados.")

    elif menu == "🔄 Movimentações":
        st.title("🔄 Registro de Movimentos")
        tipo = st.selectbox("Tipo", ["SAIDA", "ENTRADA", "TMA", "TDMA"])
        f = st.file_uploader("Arquivo Excel", type="xlsx")
        if f and st.button("Importar"):
            df_up = pd.read_excel(f, dtype=str)
            coll = get_coll("movements")
            for _, r in df_up.iterrows():
                d = r.to_dict(); d["Tipo"] = tipo; d["timestamp"] = firestore.SERVER_TIMESTAMP
                coll.add(d)
            st.success("Importado com sucesso!")

    elif menu == "📂 Base Mestra":
        st.title("📂 Sincronização")
        f = st.file_uploader("Excel Master", type="xlsx")
        if f and st.button("Sincronizar"):
            df_m = pd.read_excel(f, dtype=str)
            coll = get_coll("master_csv_store")
            for d in coll.stream(): d.reference.delete()
            csv_t = df_m.to_csv(index=False)
            for i, p in enumerate([csv_t[x:x+800000] for x in range(0, len(csv_t), 800000)]):
                coll.document(f"p_{i}").set({"part": i, "csv_data": p})
            st.cache_data.clear()
            st.success("Sincronizado!")

    elif menu == "👥 Gestão de Acessos":
        st.title("👥 Gestão de Usuários")
        with st.form("u"):
            nu, np = st.text_input("User"), st.text_input("Senha")
            nv = st.selectbox("Nível", ["Operador", "Admin"])
            if st.form_submit_button("Criar"):
                get_coll("users").add({"username": nu.lower(), "password": np, "nivel": nv})
                st.rerun()
        for u, d in users.items():
            c1, c2 = st.columns([3, 1])
            c1.write(f"• **{u}** ({d['nivel']})")
            if u != "marcius.arruda" and c2.button("Remover", key=u):
                for doc in get_coll("users").where("username", "==", u).get(): doc.reference.delete()
                st.rerun()

    elif menu == "👤 Minha Conta":
        st.title("👤 Configurações")
        st.write(f"Logado como: {st.session_state.user['username']}")
        nova = st.text_input("Nova Senha", type="password")
        if st.button("Salvar"):
            docs = get_coll("users").where("username", "==", st.session_state.user['username']).get()
            for d in docs: d.reference.update({"password": nova})
            st.success("Senha atualizada!")

if __name__ == "__main__":
    main()