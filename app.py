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
            return None, "Aba 'Secrets' não configurada no Streamlit Cloud."
        
        config = dict(st.secrets["firebase"])
        app_id = "marcius-stock-final-v11"
        
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
        return None, str(e)

db, erro_conexao = inicializar_firebase()
# O APP_ID garante que os dados fiquem isolados no Firestore
APP_ID = "marcius-stock-pro-v11"

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
            # Utilizador Mestre Padrão
            admin_data = {"username": "marcius.arruda", "password": "MwsArruda", "nivel": "Admin"}
            coll.add(admin_data)
            return {"marcius.arruda": admin_data}
        return users
    except: return {}

# --- 3. LÓGICA DE INVENTÁRIO (SOMA POR LVM + OBRA + PEP) ---

def calcular_estoque_atual():
    base = carregar_catalogo_nuvem()
    if base.empty: return pd.DataFrame()
    
    cols_tec = ["LVM", "Material", "Obra", "ElementoPEP", "Grau", "Esp", "Larg", "Comp"]
    for c in cols_tec:
        if c in base.columns: base[c] = base[c].astype(str).str.strip().str.upper()

    inv = base.groupby(cols_tec).agg({
        "DescritivoMaterial": "first", "Peso": "first", "Material": "count"
    }).rename(columns={"Material": "Pecas_Iniciais"}).reset_index()
    
    movs = carregar_movimentacoes_nuvem()
    if not movs.empty and "Tipo" in movs.columns and "Qtde" in movs.columns:
        for c in ["LVM", "Material", "Obra", "ElementoPEP"]:
            if c not in movs.columns: movs[c] = ""
            else: movs[c] = movs[c].astype(str).str.strip().str.upper()
        
        movs["Qtd_N"] = pd.to_numeric(movs["Qtde"], errors="coerce").fillna(0)
        # Impacto: ENTRADA e TDMA somam (+) | SAIDA subtrai (-)
        movs["Impacto"] = movs.apply(
            lambda x: x["Qtd_N"] if str(x.get("Tipo", "")).upper() in ["ENTRADA", "TDMA"] else -x["Qtd_N"], 
            axis=1
        )
        
        resumo = movs.groupby(["LVM", "Material", "Obra", "ElementoPEP"])["Impacto"].sum().reset_index()
        inv = pd.merge(inv, resumo, on=["LVM", "Material", "Obra", "ElementoPEP"], how="left").fillna(0)
    else:
        inv["Impacto"] = 0
        
    inv["Saldo_Pecas"] = inv["Pecas_Iniciais"] + inv["Impacto"]
    inv["Saldo_KG"] = inv["Saldo_Pecas"] * inv["Peso"]
    return inv[inv["Saldo_Pecas"] > 0]

# --- 4. RELATÓRIOS PDF (FPDF2) ---

class PDF_Stock(FPDF):
    def header(self):
        if os.path.exists("logo_empresa.png"):
            self.image("logo_empresa.png", 10, 8, 30)
        self.set_font("helvetica", "B", 14)
        self.cell(0, 10, "RELATÓRIO DE ESTOQUE - MARCIUS STOCK", ln=True, align="C")
        self.set_font("helvetica", "", 10)
        self.cell(0, 5, f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}", ln=True, align="C")
        self.ln(15)

    def footer(self):
        self.set_y(-15)
        self.set_font("helvetica", "I", 8)
        self.cell(0, 10, f"Página {self.page_no()}/{{nb}}", align="C")

def gerar_pdf_estoque(df, usuario):
    pdf = PDF_Stock()
    pdf.alias_nb_pages()
    pdf.add_page()
    pdf.set_font("helvetica", "B", 8)
    # Cabeçalho da Tabela
    pdf.set_fill_color(240, 240, 240)
    pdf.cell(30, 8, "LVM", 1, 0, "C", 1)
    pdf.cell(45, 8, "Material", 1, 0, "C", 1)
    pdf.cell(35, 8, "Obra", 1, 0, "C", 1)
    pdf.cell(15, 8, "Esp.", 1, 0, "C", 1)
    pdf.cell(30, 8, "Saldo (PC)", 1, 0, "C", 1)
    pdf.cell(35, 8, "Peso (KG)", 1, 1, "C", 1)
    
    pdf.set_font("helvetica", "", 7)
    for _, r in df.iterrows():
        pdf.cell(30, 7, str(r['LVM']), 1)
        pdf.cell(45, 7, str(r['Material'])[:20], 1)
        pdf.cell(35, 7, str(r['Obra']), 1)
        pdf.cell(15, 7, str(r['Esp']), 1, 0, "C")
        pdf.cell(30, 7, f"{int(r['Saldo_Pecas'])}", 1, 0, "R")
        pdf.cell(35, 7, f"{r['Saldo_KG']:,.2f}", 1, 1, "R")
    
    pdf.ln(5)
    pdf.set_font("helvetica", "B", 10)
    pdf.cell(125, 10, "TOTAL GERAL:", 0, 0, "R")
    pdf.cell(30, 10, f"{int(df['Saldo_Pecas'].sum())} PC", 1, 0, "R")
    pdf.cell(35, 10, f"{df['Saldo_KG'].sum():,.2f} KG", 1, 1, "R")
    
    return pdf.output()

# --- 5. INTERFACE ---

def main():
    st.set_page_config(page_title="Marcius Stock Pro", layout="wide", page_icon="🏗️")

    # BARRA LATERAL (SIDEBAR)
    with st.sidebar:
        if os.path.exists("logo_empresa.png"):
            st.image("logo_empresa.png", use_container_width=True)
        else:
            st.markdown("<h2 style='text-align: center; color: #FF4B4B;'>🏗️ MARCIUS STOCK</h2>", unsafe_allow_html=True)
        st.divider()

    if db is None:
        st.error("🔴 FIREBASE DESCONECTADO")
        return

    # LOGIN
    utilizadores = carregar_utilizadores()
    if "logado" not in st.session_state: st.session_state.logado = False
    
    if not st.session_state.logado:
        st.title("🔐 Acesso ao Sistema")
        u = st.text_input("Utilizador").lower().strip()
        p = st.text_input("Senha", type="password")
        if st.button("Entrar", use_container_width=True):
            if u in utilizadores and utilizadores[u]["password"] == p:
                st.session_state.logado = True
                st.session_state.user = utilizadores[u]
                st.rerun()
            else: st.error("Utilizador ou Senha inválidos.")
        return

    # MENU DE NAVEGAÇÃO
    opcoes = ["📊 Dashboard", "🔄 Movimentações"]
    if st.session_state.user['nivel'] == "Admin":
        opcoes += ["📂 Base Mestra", "👥 Gestão de Acessos"]
    
    menu = st.sidebar.radio("Navegação Principal", opcoes)
    st.sidebar.info(f"👤 Utilizador: {st.session_state.user['username']}")
    
    if st.sidebar.button("Terminar Sessão"):
        st.session_state.logado = False
        st.rerun()

    # --- PÁGINA: DASHBOARD ---
    if menu == "📊 Dashboard":
        st.title("📊 Painel de Controle de Stock")
        df = calcular_estoque_atual()
        
        if df.empty:
            st.info("💡 Catálogo vazio. Por favor, carregue a Base Mestra.")
        else:
            with st.sidebar.expander("🔍 Filtros Avançados", expanded=True):
                f_obra = st.multiselect("Filtrar Obra", sorted(df["Obra"].unique()))
                f_esp = st.multiselect("Filtrar Espessura", sorted(df["Esp"].unique()))
                f_lvm = st.text_input("Busca por LVM").upper().strip()

            df_v = df.copy()
            if f_obra: df_v = df_v[df_v["Obra"].isin(f_obra)]
            if f_esp: df_v = df_v[df_v["Esp"].isin(f_esp)]
            if f_lvm: df_v = df_v[df_v["LVM"].str.contains(f_lvm)]

            # KPIs
            c1, c2, c3 = st.columns(3)
            c1.metric("Peças em Stock", f"{int(df_v['Saldo_Pecas'].sum()):,}")
            c2.metric("Peso Total (KG)", f"{df_v['Saldo_KG'].sum():,.2f}")
            c3.metric("LVMs Ativas", len(df_v["LVM"].unique()))

            # BOTÃO PDF
            st.divider()
            if st.button("📥 Gerar Relatório PDF (Dados Atuais)"):
                pdf_bytes = gerar_pdf_estoque(df_v, st.session_state.user['username'])
                st.download_button(
                    label="💾 Baixar Relatório PDF",
                    data=pdf_bytes,
                    file_name=f"stock_marcius_{datetime.now().strftime('%d%m%Y')}.pdf",
                    mime="application/pdf"
                )

            # GRÁFICOS (RESTAURADOS)
            st.divider()
            g1, g2 = st.columns(2)
            with g1:
                fig1 = px.pie(df_v.groupby("Obra")["Saldo_Pecas"].sum().reset_index().nlargest(10, "Saldo_Pecas"), 
                             values="Saldo_Pecas", names="Obra", title="Top 10 Obras (Peças)", hole=0.3)
                st.plotly_chart(fig1, use_container_width=True)
            with g2:
                fig2 = px.bar(df_v.groupby("Grau")["Saldo_KG"].sum().reset_index(), 
                             x="Grau", y="Saldo_KG", title="Peso por Grau de Aço (KG)", color="Grau")
                st.plotly_chart(fig2, use_container_width=True)

            st.subheader("Lista Detalhada de Itens")
            st.dataframe(df_v, use_container_width=True, hide_index=True)

    # --- PÁGINA: GESTÃO DE ACESSOS ---
    elif menu == "👥 Gestão de Acessos":
        st.title("👥 Gestão de Utilizadores")
        with st.form("add_user"):
            st.subheader("Criar Novo Acesso")
            nu = st.text_input("Nome de Utilizador (ex: jose.silva)").lower().strip()
            np = st.text_input("Senha", type="password")
            nv = st.selectbox("Nível de Permissão", ["Operador", "Admin"])
            if st.form_submit_button("Guardar Novo Utilizador"):
                if nu and np:
                    get_coll("users").add({"username": nu, "password": np, "nivel": nv})
                    st.success(f"Utilizador {nu} criado!")
                    time.sleep(1)
                    st.rerun()
                else: st.error("Preencha todos os campos.")
        
        st.divider()
        st.subheader("Utilizadores Registrados")
        for u_name, u_data in utilizadores.items():
            col_u1, col_u2 = st.columns([3, 1])
            col_u1.write(f"**{u_name}** — Nível: {u_data['nivel']}")
            if u_name != "marcius.arruda":
                if col_u2.button("Eliminar", key=f"del_{u_name}"):
                    docs = get_coll("users").where("username", "==", u_name).stream()
                    for d in docs: d.reference.delete()
                    st.rerun()

    # --- PÁGINA: MOVIMENTAÇÕES ---
    elif menu == "🔄 Movimentações":
        st.title("🔄 Registo de Movimentações")
        base = carregar_catalogo_nuvem()
        if base.empty: st.error("A Base Mestra não foi encontrada. Carregue-a primeiro."); return
        
        t1, t2 = st.tabs(["📝 Individual", "📁 Em Lote (Excel)"])
        
        with t1:
            with st.form("mov_manual"):
                tp = st.selectbox("Tipo de Operação", ["SAIDA", "ENTRADA", "TMA", "TDMA"])
                mat = st.selectbox("Código do Material", sorted(base["Material"].unique()))
                lvm = st.text_input("LVM").upper().strip()
                qtd = st.number_input("Quantidade", min_value=1, step=1)
                
                if tp == "TMA":
                    c1, c2 = st.columns(2)
                    o_orig = c1.text_input("Obra Origem").upper().strip()
                    o_dest = c2.text_input("Obra Destino").upper().strip()
                    p_orig = c1.text_input("PEP Origem").upper().strip()
                    p_dest = c2.text_input("PEP Destino").upper().strip()
                else:
                    c3, c4 = st.columns(2)
                    obr = c3.text_input("Obra").upper().strip()
                    pep = c4.text_input("Elemento PEP").upper().strip()
                
                if st.form_submit_button("Confirmar Movimentação"):
                    coll = get_coll("movements")
                    ts = firestore.SERVER_TIMESTAMP
                    dt = datetime.now().strftime("%d/%m/%Y")
                    if tp == "TMA":
                        coll.add({"Tipo": "SAIDA", "Material": mat, "LVM": lvm, "Qtde": qtd, "Obra": o_orig, "ElementoPEP": p_orig, "Data": dt, "timestamp": ts, "Obs": "TMA Origem"})
                        coll.add({"Tipo": "ENTRADA", "Material": mat, "LVM": lvm, "Qtde": qtd, "Obra": o_dest, "ElementoPEP": p_dest, "Data": dt, "timestamp": ts, "Obs": "TMA Destino"})
                    else:
                        coll.add({"Tipo": tp, "Material": mat, "LVM": lvm, "Qtde": qtd, "Obra": obr, "ElementoPEP": pep, "Data": dt, "timestamp": ts})
                    st.success("Registo efetuado!"); time.sleep(1); st.rerun()

        with t2:
            st.info("Colunas obrigatórias no Excel: Material, LVM, Qtde, Obra, ElementoPEP, Tipo")
            up = st.file_uploader("Subir Ficheiro Excel de Movimentos", type="xlsx")
            if up and st.button("🚀 Processar Movimentações"):
                df_up = pd.read_excel(up, dtype=str)
                coll = get_coll("movements")
                for _, r in df_up.iterrows():
                    coll.add(r.to_dict())
                st.success("Importação concluída!")

    # --- PÁGINA: BASE MESTRA ---
    elif menu == "📂 Base Mestra":
        st.title("📂 Gestão da Base Mestra")
        f = st.file_uploader("Carregar Catálogo Excel (.xlsx)", type="xlsx")
        if f and st.button("🚀 Sincronizar Catálogo Completo"):
            df_up = pd.read_excel(f, dtype=str)
            coll = get_coll("master_csv_store")
            for d in coll.stream(): d.reference.delete()
            
            csv_text = df_up.to_csv(index=False)
            size = 800000
            parts = [csv_text[i:i+size] for i in range(0, len(csv_text), size)]
            for i, p in enumerate(parts):
                coll.document(f"p_{i}").set({"part": i, "csv_data": p})
            st.cache_data.clear()
            st.success("Base Mestra atualizada com sucesso!")

if __name__ == "__main__":
    main()