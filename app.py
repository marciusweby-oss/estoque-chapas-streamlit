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
            return None, "Aba 'Secrets' não configurada no Streamlit Cloud. O Administrador precisa configurar as chaves do banco de dados."
        
        config = dict(st.secrets["firebase"])
        app_id = "marcius-stock-v14-final"
        
        if not firebase_admin._apps:
            cred = credentials.Certificate(config)
            firebase_admin.initialize_app(cred, name=app_id)
        else:
            try:
                firebase_admin.get_app(app_id)
            except ValueError:
                cred = credentials.Certificate(config)
                firebase_admin.initialize_app(cred, name=app_id)
        
        app_inst = firebase_admin.get_app(app_id)
        return firestore.client(app=app_inst), None
    except Exception as e:
        return None, f"Erro de conexão: {str(e)}"

db, erro_conexao = inicializar_firebase()
APP_ID = "marcius-stock-pro-v14"

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
        lista_docs = [d.to_dict() for d in sorted(docs, key=lambda x: x.to_dict().get("part", 0))]
        partes = "".join([d.get("csv_data", "") for d in lista_docs])
        if not partes: return pd.DataFrame()
        df = pd.read_csv(io.StringIO(partes), dtype=str)
        # Limpeza técnica de números para filtros e cálculos
        for c in ["Peso", "Larg", "Comp", "Esp"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c].astype(str).str.replace(",", "."), errors="coerce").fillna(0)
        return df
    except: return pd.DataFrame()

def carregar_movimentacoes_nuvem():
    coll = get_coll("movements")
    if coll is None: return pd.DataFrame()
    try:
        docs = coll.stream()
        data = [d.to_dict() for d in docs]
        return pd.DataFrame(data) if data else pd.DataFrame()
    except: return pd.DataFrame()

def carregar_utilizadores():
    coll = get_coll("users")
    if coll is None: return {}
    try:
        docs = coll.stream()
        users = {d.to_dict()["username"]: d.to_dict() for d in docs}
        if not users:
            # Login mestre padrão para o Marcius
            admin_data = {"username": "marcius.arruda", "password": "MwsArruda", "nivel": "Admin"}
            coll.add(admin_data)
            return {"marcius.arruda": admin_data}
        return users
    except: return {}

# --- 3. LÓGICA DE CÁLCULO DE STOCK ---

def calcular_estoque_atual():
    base = carregar_catalogo_nuvem()
    if base.empty: return pd.DataFrame()
    
    chaves = ["LVM", "Material", "Obra", "ElementoPEP"]
    atributos = ["Grau", "Esp", "Larg", "Comp"]
    cols_tec = chaves + atributos
    
    for c in cols_tec:
        if c in base.columns: 
            base[c] = base[c].astype(str).str.strip().str.upper()

    inv = base.groupby(cols_tec).agg({
        "DescritivoMaterial": "first", "Peso": "first", "Material": "count"
    }).rename(columns={"Material": "Pecas_Iniciais"}).reset_index()
    
    movs = carregar_movimentacoes_nuvem()
    
    if not movs.empty and "Tipo" in movs.columns and "Qtde" in movs.columns:
        for c in chaves:
            if c not in movs.columns: movs[c] = ""
            else: movs[c] = movs[c].astype(str).str.strip().str.upper()
        
        movs["Qtd_N"] = pd.to_numeric(movs["Qtde"], errors="coerce").fillna(0)
        movs["Impacto"] = movs.apply(
            lambda x: x["Qtd_N"] if str(x.get("Tipo", "")).upper() in ["ENTRADA", "TDMA"] else -x["Qtd_N"], 
            axis=1
        )
        
        resumo = movs.groupby(chaves)["Impacto"].sum().reset_index()
        inv = pd.merge(inv, resumo, on=chaves, how="left").fillna(0)
    else:
        inv["Impacto"] = 0
        
    inv["Saldo_Pecas"] = inv["Pecas_Iniciais"] + inv["Impacto"]
    inv["Saldo_KG"] = inv["Saldo_Pecas"] * inv["Peso"]
    
    return inv[inv["Saldo_Pecas"] > 0].sort_values(by=["Obra", "LVM"])

# --- 4. RELATÓRIO PDF ---

class PDF_Stock(FPDF):
    def header(self):
        if os.path.exists("logo_empresa.png"):
            self.image("logo_empresa.png", 10, 8, 25)
        self.set_font("helvetica", "B", 14)
        self.cell(0, 10, "INVENTÁRIO DE STOCK - MARCIUS STOCK", ln=True, align="R")
        self.set_font("helvetica", "I", 8)
        self.cell(0, 5, f"Relatório: {datetime.now().strftime('%d/%m/%Y %H:%M')}", ln=True, align="R")
        self.ln(10)

def gerar_pdf_estoque(df):
    pdf = PDF_Stock()
    pdf.alias_nb_pages()
    pdf.add_page()
    pdf.set_font("helvetica", "B", 7)
    pdf.set_fill_color(240, 240, 240)
    
    headers = ["LVM", "Material", "Obra", "Grau", "Esp.", "Larg.", "Comp.", "Saldo"]
    widths = [25, 35, 30, 20, 15, 20, 20, 25]
    
    for i in range(len(headers)):
        pdf.cell(widths[i], 8, headers[i], 1, 0, "C", 1)
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
    pdf.cell(0, 10, f"TOTAL GERAL: {int(df['Saldo_Pecas'].sum())} Peças | {df['Saldo_KG'].sum():,.2f} KG", ln=True, align="R")
    return pdf.output()

# --- 5. INTERFACE ---

def main():
    st.set_page_config(page_title="Marcius Stock Pro", layout="wide", page_icon="🏗️")

    # CSS para melhorar o visual no telemóvel
    st.markdown("""
        <style>
            .stButton>button { width: 100%; border-radius: 8px; height: 3em; font-weight: bold; }
            .stTextInput>div>div>input { height: 3em; }
        </style>
    """, unsafe_allow_html=True)

    with st.sidebar:
        if os.path.exists("logo_empresa.png"):
            st.image("logo_empresa.png", use_container_width=True)
        else:
            st.markdown("<h2 style='text-align: center; color: #FF4B4B;'>🏗️ MARCIUS STOCK</h2>", unsafe_allow_html=True)
        st.divider()

    if db is None:
        st.error("🔴 PROBLEMA DE LIGAÇÃO")
        st.warning(erro_conexao)
        return

    utilizadores = carregar_utilizadores()
    if "logado" not in st.session_state: st.session_state.logado = False
    
    if not st.session_state.logado:
        st.markdown("<h1 style='text-align: center;'>🏗️ Gestão Marcius Stock</h1>", unsafe_allow_html=True)
        st.markdown("<p style='text-align: center;'>Faça login para aceder ao sistema</p>", unsafe_allow_html=True)
        
        c1, c2, c3 = st.columns([1, 4, 1]) if st.sidebar.checkbox("Modo Mobile", value=True) else st.columns([1, 2, 1])
        
        with c2:
            u = st.text_input("Utilizador").lower().strip()
            p = st.text_input("Senha", type="password")
            if st.button("ENTRAR"):
                if u in utilizadores and utilizadores[u]["password"] == p:
                    st.session_state.logado = True
                    st.session_state.user = utilizadores[u]
                    st.rerun()
                else: 
                    st.error("Credenciais inválidas. Verifique o nome de utilizador e a senha.")
        return

    # Navegação
    opcoes = ["📊 Dashboard", "🔄 Movimentações", "👤 Minha Conta"]
    if st.session_state.user['nivel'] == "Admin":
        opcoes += ["📂 Base Mestra", "👥 Gestão de Acessos"]
    
    menu = st.sidebar.radio("Navegação Principal", opcoes)
    
    if st.sidebar.button("Terminar Sessão"):
        st.session_state.logado = False
        st.rerun()

    # --- TELA: DASHBOARD ---
    if menu == "📊 Dashboard":
        st.title("📊 Controle de Stock")
        df = calcular_estoque_atual()
        
        if df.empty:
            st.info("💡 Catálogo vazio. Administrador: carregue a Base Mestra para começar.")
        else:
            st.sidebar.markdown("### 🔍 Filtros")
            f_mat = st.sidebar.multiselect("Material", sorted(df["Material"].unique()))
            f_obra = st.sidebar.multiselect("Obra", sorted(df["Obra"].unique()))
            f_pep = st.sidebar.multiselect("Elemento PEP", sorted(df["ElementoPEP"].unique()))
            f_grau = st.sidebar.multiselect("Grau do Aço", sorted(df["Grau"].unique()))
            f_esp = st.sidebar.multiselect("Espessura", sorted(df["Esp"].unique()))
            f_larg = st.sidebar.multiselect("Largura", sorted(df["Larg"].unique()))
            f_comp = st.sidebar.multiselect("Comprimento", sorted(df["Comp"].unique()))
            f_lvm = st.sidebar.text_input("Busca LVM").upper().strip()

            df_v = df.copy()
            if f_mat: df_v = df_v[df_v["Material"].isin(f_mat)]
            if f_obra: df_v = df_v[df_v["Obra"].isin(f_obra)]
            if f_pep: df_v = df_v[df_v["ElementoPEP"].isin(f_pep)]
            if f_grau: df_v = df_v[df_v["Grau"].isin(f_grau)]
            if f_esp: df_v = df_v[df_v["Esp"].isin(f_esp)]
            if f_larg: df_v = df_v[df_v["Larg"].isin(f_larg)]
            if f_comp: df_v = df_v[df_v["Comp"].isin(f_comp)]
            if f_lvm: df_v = df_v[df_v["LVM"].str.contains(f_lvm)]

            k1, k2, k3 = st.columns(3)
            k1.metric("Peças", f"{int(df_v['Saldo_Pecas'].sum()):,}")
            k2.metric("Total KG", f"{df_v['Saldo_KG'].sum():,.2f}")
            k3.metric("LVMs", len(df_v["LVM"].unique()))

            st.divider()
            if st.button("📥 Exportar Dashboard para PDF"):
                pdf_bytes = gerar_pdf_estoque(df_v)
                st.download_button("💾 Baixar PDF", pdf_bytes, f"stock_{datetime.now().strftime('%d%m%Y')}.pdf", "application/pdf")

            g1, g2 = st.columns(2)
            with g1:
                fig1 = px.pie(df_v.groupby("Obra")["Saldo_Pecas"].sum().reset_index().nlargest(10, "Saldo_Pecas"), values="Saldo_Pecas", names="Obra", title="Top 10 Obras", hole=0.3)
                st.plotly_chart(fig1, use_container_width=True)
            with g2:
                fig2 = px.bar(df_v.groupby("Grau")["Saldo_KG"].sum().reset_index(), x="Grau", y="Saldo_KG", title="Peso por Grau", color="Grau")
                st.plotly_chart(fig2, use_container_width=True)

            st.dataframe(df_v, use_container_width=True, hide_index=True)

    # --- TELA: MOVIMENTAÇÕES ---
    elif menu == "🔄 Movimentações":
        st.title("🔄 Movimentações")
        base = carregar_catalogo_nuvem()
        if base.empty: st.error("A Base Mestra não está carregada."); return
        
        tab1, tab2 = st.tabs(["📝 Individual", "📁 Lote (Excel)"])
        with tab1:
            with st.form("f_ind"):
                tp = st.selectbox("Operação", ["SAIDA", "ENTRADA", "TMA", "TDMA"])
                mat = st.selectbox("Material", sorted(base["Material"].unique()))
                lvm = st.text_input("LVM").upper().strip()
                qtd = st.number_input("Qtd", min_value=1, step=1)
                o = st.text_input("Obra").upper().strip()
                p = st.text_input("PEP").upper().strip()
                if st.form_submit_button("GRAVAR REGISTO"):
                    coll = get_coll("movements")
                    dt = datetime.now().strftime("%d/%m/%Y")
                    coll.add({"Tipo": tp, "Material": mat, "LVM": lvm, "Qtde": qtd, "Obra": o, "ElementoPEP": p, "Data": dt, "timestamp": firestore.SERVER_TIMESTAMP})
                    st.success("Registo efetuado com sucesso!")
                    time.sleep(1)
                    st.rerun()
        
        with tab2:
            st.subheader("📁 Upload Excel")
            tipo_up = st.selectbox("Tipo de Movimento no Arquivo", ["SAIDA", "ENTRADA", "TMA", "TDMA"])
            st.info("O Excel deve conter: Material, LVM, Qtde, Obra, ElementoPEP, Data")
            up = st.file_uploader(f"Ficheiro de {tipo_up}", type="xlsx")
            if up and st.button("PROCESSAR FICHEIRO"):
                df_up = pd.read_excel(up, dtype=str)
                coll = get_coll("movements")
                ts = firestore.SERVER_TIMESTAMP
                for _, r in df_up.iterrows():
                    d = r.to_dict(); d["Tipo"] = tipo_up; d["timestamp"] = ts
                    coll.add(d)
                st.success("Importação concluída!")
                st.rerun()

    # --- TELA: MINHA CONTA ---
    elif menu == "👤 Minha Conta":
        st.title("👤 Minha Conta")
        st.subheader("Alterar Palavra-passe")
        with st.form("f_perfil"):
            senha_atual = st.text_input("Senha Atual", type="password")
            nova_senha = st.text_input("Nova Senha", type="password")
            confirmar = st.text_input("Confirmar Nova Senha", type="password")
            
            if st.form_submit_button("ATUALIZAR SENHA"):
                if senha_atual != st.session_state.user['password']:
                    st.error("A senha atual está incorreta.")
                elif nova_senha != confirmar:
                    st.error("As novas senhas não coincidem.")
                elif len(nova_senha) < 4:
                    st.error("A nova senha deve ter pelo menos 4 caracteres.")
                else:
                    user_ref = get_coll("users").where("username", "==", st.session_state.user['username']).stream()
                    for doc in user_ref:
                        doc.reference.update({"password": nova_senha})
                    st.session_state.user['password'] = nova_senha
                    st.success("Palavra-passe atualizada com sucesso!")
                    time.sleep(1)
                    st.rerun()

    # --- TELA: GESTÃO DE ACESSOS ---
    elif menu == "👥 Gestão de Acessos":
        st.title("👥 Gestão de Utilizadores")
        with st.form("f_u"):
            nu = st.text_input("Nome de Utilizador").lower().strip()
            np = st.text_input("Senha Provisória", type="password")
            nv = st.selectbox("Nível de Permissão", ["Operador", "Admin"])
            if st.form_submit_button("CRIAR UTILIZADOR"):
                if nu and np:
                    get_coll("users").add({"username": nu, "password": np, "nivel": nv})
                    st.success(f"Utilizador '{nu}' criado! Peça ao colega para entrar e mudar a senha em 'Minha Conta'.")
                    time.sleep(1)
                    st.rerun()
                else: st.error("Preencha todos os campos.")
        
        st.divider()
        st.subheader("Lista de Utilizadores Ativos")
        for n, d in utilizadores.items():
            c1, c2 = st.columns([4, 1])
            c1.write(f"🏷️ **{n}** | Permissão: {d['nivel']}")
            if n != "marcius.arruda":
                if c2.button("Eliminar", key=f"d_{n}"):
                    docs = get_coll("users").where("username", "==", n).stream()
                    for doc in docs: doc.reference.delete()
                    st.rerun()

    # --- TELA: BASE MESTRA ---
    elif menu == "📂 Base Mestra":
        st.title("📂 Gestão da Base Mestra")
        st.warning("Atenção: Carregar uma nova base substituirá a anterior na nuvem.")
        f = st.file_uploader("Ficheiro Excel Principal", type="xlsx")
        if f and st.button("🚀 SINCRONIZAR COM A NUVEM"):
            df_m = pd.read_excel(f, dtype=str)
            coll = get_coll("master_csv_store")
            for d in coll.stream(): d.reference.delete()
            csv_data = df_m.to_csv(index=False)
            size = 800000
            parts = [csv_data[i:i+size] for i in range(0, len(csv_data), size)]
            for i, p in enumerate(parts):
                coll.document(f"p_{i}").set({"part": i, "csv_data": p})
            st.cache_data.clear()
            st.success("Catálogo sincronizado com sucesso!")

if __name__ == "__main__":
    main()