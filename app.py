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
        # Nome único para a instância da App
        app_id = "marcius-stock-v13-final"
        
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
# O APP_ID no banco de dados para evitar conflitos de versões anteriores
APP_ID = "marcius-stock-pro-v13"

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
        # Limpeza técnica de números
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
            # Login mestre se a lista estiver vazia
            admin_data = {"username": "marcius.arruda", "password": "MwsArruda", "nivel": "Admin"}
            coll.add(admin_data)
            return {"marcius.arruda": admin_data}
        return users
    except: return {}

# --- 3. LÓGICA DE CÁLCULO DE STOCK ---

def calcular_estoque_atual():
    base = carregar_catalogo_nuvem()
    if base.empty: return pd.DataFrame()
    
    # Colunas que definem um item único para o sistema
    chaves = ["LVM", "Material", "Obra", "ElementoPEP"]
    cols_tec = chaves + ["Grau", "Esp", "Larg", "Comp"]
    
    # Limpeza profunda da Base para evitar erros de match
    for c in cols_tec:
        if c in base.columns: 
            base[c] = base[c].astype(str).str.strip().str.upper()

    # Agrupa a Base Mestra (Inventário de Abertura)
    inv = base.groupby(cols_tec).agg({
        "DescritivoMaterial": "first", "Peso": "first", "Material": "count"
    }).rename(columns={"Material": "Pecas_Iniciais"}).reset_index()
    
    movs = carregar_movimentacoes_nuvem()
    
    if not movs.empty and "Tipo" in movs.columns and "Qtde" in movs.columns:
        # Limpeza profunda dos movimentos
        for c in chaves:
            if c not in movs.columns: movs[c] = ""
            else: movs[c] = movs[c].astype(str).str.strip().str.upper()
        
        movs["Qtd_N"] = pd.to_numeric(movs["Qtde"], errors="coerce").fillna(0)
        
        # Regra de Impacto: ENTRADA/TDMA soma | SAIDA/TMA subtrai
        movs["Impacto"] = movs.apply(
            lambda x: x["Qtd_N"] if str(x.get("Tipo", "")).upper() in ["ENTRADA", "TDMA"] else -x["Qtd_N"], 
            axis=1
        )
        
        # Agrupa impactos por chave única
        resumo = movs.groupby(chaves)["Impacto"].sum().reset_index()
        
        # Cruza inventário inicial com movimentações
        inv = pd.merge(inv, resumo, on=chaves, how="left").fillna(0)
    else:
        inv["Impacto"] = 0
        
    inv["Saldo_Pecas"] = inv["Pecas_Iniciais"] + inv["Impacto"]
    inv["Saldo_KG"] = inv["Saldo_Pecas"] * inv["Peso"]
    
    # Apenas itens com saldo positivo
    return inv[inv["Saldo_Pecas"] > 0].sort_values(by="Obra")

# --- 4. GERAÇÃO DE RELATÓRIO PDF (FPDF2) ---

class PDF_Stock(FPDF):
    def header(self):
        # Logo no topo se existir
        if os.path.exists("logo_empresa.png"):
            self.image("logo_empresa.png", 10, 8, 25)
        self.set_font("helvetica", "B", 14)
        self.cell(0, 10, "INVENTÁRIO DE STOCK - MARCIUS STOCK", ln=True, align="R")
        self.set_font("helvetica", "I", 8)
        self.cell(0, 5, f"Relatório gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}", ln=True, align="R")
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font("helvetica", "I", 8)
        self.cell(0, 10, f"Página {self.page_no()}/{{nb}}", align="C")

def gerar_pdf_estoque(df):
    pdf = PDF_Stock()
    pdf.alias_nb_pages()
    pdf.add_page()
    pdf.set_font("helvetica", "B", 8)
    # Estilo da tabela
    pdf.set_fill_color(240, 240, 240)
    cols = ["LVM", "Material", "Obra", "Esp.", "Saldo", "Peso (KG)"]
    w = [35, 45, 35, 15, 25, 35]
    
    for i in range(len(cols)):
        pdf.cell(w[i], 8, cols[i], 1, 0, "C", 1)
    pdf.ln()
    
    pdf.set_font("helvetica", "", 7)
    for _, r in df.iterrows():
        pdf.cell(35, 7, str(r['LVM']), 1)
        pdf.cell(45, 7, str(r['Material'])[:20], 1)
        pdf.cell(35, 7, str(r['Obra']), 1)
        pdf.cell(15, 7, str(r['Esp']), 1, 0, "C")
        pdf.cell(25, 7, f"{int(r['Saldo_Pecas'])}", 1, 0, "R")
        pdf.cell(35, 7, f"{r['Saldo_KG']:,.2f}", 1, 1, "R")
    
    pdf.ln(5)
    pdf.set_font("helvetica", "B", 10)
    pdf.cell(130, 10, "TOTAL EM STOCK:", 0, 0, "R")
    pdf.cell(25, 10, f"{int(df['Saldo_Pecas'].sum())}", 1, 0, "R")
    pdf.cell(35, 10, f"{df['Saldo_KG'].sum():,.2f} KG", 1, 1, "R")
    
    return pdf.output()

# --- 5. INTERFACE PRINCIPAL ---

def main():
    st.set_page_config(page_title="Marcius Stock Pro", layout="wide", page_icon="🏗️")

    # Sidebar UI
    with st.sidebar:
        if os.path.exists("logo_empresa.png"):
            st.image("logo_empresa.png", use_container_width=True)
        else:
            st.markdown("<h2 style='text-align: center; color: #FF4B4B;'>🏗️ MARCIUS STOCK</h2>", unsafe_allow_html=True)
        st.divider()

    if db is None:
        st.error("🔴 FIREBASE DESCONECTADO")
        st.info("Verifique os 'Secrets' no painel do Streamlit.")
        return

    # --- LÓGICA DE LOGIN ---
    utilizadores = carregar_utilizadores()
    if "logado" not in st.session_state: st.session_state.logado = False
    
    if not st.session_state.logado:
        st.markdown("<h1 style='text-align: center;'>🔐 Acesso Restrito</h1>", unsafe_allow_html=True)
        col_log1, col_log2, col_log3 = st.columns([1, 2, 1])
        with col_log2:
            u = st.text_input("Utilizador").lower().strip()
            p = st.text_input("Senha", type="password")
            if st.button("Entrar no Sistema", use_container_width=True):
                if u in utilizadores and utilizadores[u]["password"] == p:
                    st.session_state.logado = True
                    st.session_state.user = utilizadores[u]
                    st.rerun()
                else: st.error("Utilizador ou Senha incorretos.")
        return

    # --- MENU DE NAVEGAÇÃO ---
    opcoes = ["📊 Dashboard", "🔄 Movimentações"]
    if st.session_state.user['nivel'] == "Admin":
        opcoes += ["📂 Base Mestra", "👥 Gestão de Acessos"]
    
    menu = st.sidebar.radio("Navegação Principal", opcoes)
    st.sidebar.markdown(f"**👤 Utilizador:** {st.session_state.user['username']}")
    
    if st.sidebar.button("Terminar Sessão", use_container_width=True):
        st.session_state.logado = False
        st.rerun()

    # --- TELA: DASHBOARD (COM FILTROS MOBILE) ---
    if menu == "📊 Dashboard":
        st.title("📊 Painel de Controle")
        df = calcular_estoque_atual()
        
        if df.empty:
            st.warning("💡 O catálogo está vazio. Carregue a Base Mestra para começar.")
        else:
            # Filtros na Sidebar para Mobile
            st.sidebar.markdown("### 🔍 Filtros")
            f_obra = st.sidebar.multiselect("Filtrar Obra", sorted(df["Obra"].unique()))
            f_esp = st.sidebar.multiselect("Filtrar Espessura", sorted(df["Esp"].unique()))
            f_lvm = st.sidebar.text_input("Busca rápida LVM").upper().strip()

            df_v = df.copy()
            if f_obra: df_v = df_v[df_v["Obra"].isin(f_obra)]
            if f_esp: df_v = df_v[df_v["Esp"].isin(f_esp)]
            if f_lvm: df_v = df_v[df_v["LVM"].str.contains(f_lvm)]

            # KPIs Responsivos
            k1, k2, k3 = st.columns(3)
            k1.metric("Peças Totais", f"{int(df_v['Saldo_Pecas'].sum()):,}")
            k2.metric("Peso Total (KG)", f"{df_v['Saldo_KG'].sum():,.2f}")
            k3.metric("LVMs Ativas", len(df_v["LVM"].unique()))

            # Ações de Exportação
            st.divider()
            if st.button("📥 Exportar Saldo para PDF", use_container_width=True):
                pdf_bytes = gerar_pdf_estoque(df_v)
                st.download_button("💾 Baixar Relatório PDF", pdf_bytes, f"stock_{datetime.now().strftime('%d%m%Y')}.pdf", "application/pdf", use_container_width=True)

            # Gráficos
            st.divider()
            g1, g2 = st.columns(2)
            with g1:
                fig1 = px.pie(df_v.groupby("Obra")["Saldo_Pecas"].sum().reset_index().nlargest(10, "Saldo_Pecas"), 
                             values="Saldo_Pecas", names="Obra", title="Divisão por Obra", hole=0.3)
                st.plotly_chart(fig1, use_container_width=True)
            with g2:
                fig2 = px.bar(df_v.groupby("Grau")["Saldo_KG"].sum().reset_index(), 
                             x="Grau", y="Saldo_KG", title="Peso por Grau (KG)", color="Grau")
                st.plotly_chart(fig2, use_container_width=True)

            st.subheader("Lista de Itens em Stock")
            st.dataframe(df_v, use_container_width=True, hide_index=True)

    # --- TELA: MOVIMENTAÇÕES (NOVA ARQUITETURA DE IMPORTAÇÃO) ---
    elif menu == "🔄 Movimentações":
        st.title("🔄 Registo de Entradas e Saídas")
        base = carregar_catalogo_nuvem()
        if base.empty: st.error("Carregue a Base Mestra antes de movimentar."); return
        
        tab_ind, tab_lote = st.tabs(["📝 Lançamento Individual", "📁 Importação Excel"])
        
        with tab_ind:
            with st.form("form_ind"):
                t_op = st.selectbox("Tipo de Operação", ["SAIDA", "ENTRADA", "TMA", "TDMA"])
                t_mat = st.selectbox("Código Material", sorted(base["Material"].unique()))
                t_lvm = st.text_input("LVM").upper().strip()
                t_qtd = st.number_input("Quantidade", min_value=1, step=1)
                
                if t_op == "TMA":
                    o1, o2 = st.columns(2)
                    o_orig = o1.text_input("Obra Origem").upper().strip()
                    o_dest = o2.text_input("Obra Destino").upper().strip()
                    p_orig = o1.text_input("PEP Origem").upper().strip()
                    p_dest = o2.text_input("PEP Destino").upper().strip()
                else:
                    o3, o4 = st.columns(2)
                    obr_m = o3.text_input("Obra").upper().strip()
                    pep_m = o4.text_input("Elemento PEP").upper().strip()
                
                if st.form_submit_button("Gravar", use_container_width=True):
                    coll = get_coll("movements")
                    dt_now = datetime.now().strftime("%d/%m/%Y")
                    ts_now = firestore.SERVER_TIMESTAMP
                    if t_op == "TMA":
                        coll.add({"Tipo": "SAIDA", "Material": t_mat, "LVM": t_lvm, "Qtde": t_qtd, "Obra": o_orig, "ElementoPEP": p_orig, "Data": dt_now, "timestamp": ts_now})
                        coll.add({"Tipo": "ENTRADA", "Material": t_mat, "LVM": t_lvm, "Qtde": t_qtd, "Obra": o_dest, "ElementoPEP": p_dest, "Data": dt_now, "timestamp": ts_now})
                    else:
                        coll.add({"Tipo": t_op, "Material": t_mat, "LVM": t_lvm, "Qtde": t_qtd, "Obra": obr_m, "ElementoPEP": pep_m, "Data": dt_now, "timestamp": ts_now})
                    st.success("Registado!"); time.sleep(1); st.rerun()

        with tab_lote:
            st.subheader("📁 Upload Massivo")
            # O Utilizador seleciona o tipo aqui, e o sistema aplica a todas as linhas do Excel
            tipo_arquivo = st.selectbox("Tipo de Movimento deste Arquivo", ["SAIDA", "ENTRADA", "TMA", "TDMA"])
            st.info(f"O Excel deve conter: Material, LVM, Qtde, Obra, ElementoPEP, Data")
            
            up_mov = st.file_uploader(f"Subir Excel de {tipo_arquivo}", type="xlsx")
            if up_mov and st.button("🚀 Processar Importação", use_container_width=True):
                df_up = pd.read_excel(up_mov, dtype=str)
                coll = get_coll("movements")
                ts_now = firestore.SERVER_TIMESTAMP
                
                # Validação de colunas básica
                required = ["Material", "LVM", "Qtde", "Obra", "ElementoPEP", "Data"]
                if not all(c in df_up.columns for c in required):
                    st.error("O arquivo não possui as colunas necessárias.")
                else:
                    prog = st.progress(0)
                    total = len(df_up)
                    for i, r in df_up.iterrows():
                        dados = r.to_dict()
                        dados["Tipo"] = tipo_arquivo
                        dados["timestamp"] = ts_now
                        coll.add(dados)
                        prog.progress((i + 1) / total)
                    st.success(f"Importação de {total} linhas concluída!"); st.balloons(); time.sleep(1); st.rerun()

    # --- TELA: GESTÃO DE ACESSOS ---
    elif menu == "👥 Gestão de Acessos":
        st.title("👥 Gestão de Utilizadores")
        with st.form("form_u"):
            nu = st.text_input("Novo Utilizador").lower().strip()
            np = st.text_input("Senha", type="password")
            nv = st.selectbox("Nível", ["Operador", "Admin"])
            if st.form_submit_button("Criar Acesso"):
                if nu and np:
                    get_coll("users").add({"username": nu, "password": np, "nivel": nv})
                    st.success("Criado!"); time.sleep(1); st.rerun()
        
        st.divider()
        for n, d in utilizadores.items():
            c_u1, c_u2 = st.columns([4, 1])
            c_u1.write(f"🏷️ **{n}** | {d['nivel']}")
            if n != "marcius.arruda" and c_u2.button("Remover", key=f"del_{n}"):
                docs = get_coll("users").where("username", "==", n).stream()
                for doc in docs: doc.reference.delete()
                st.rerun()

    # --- TELA: BASE MESTRA ---
    elif menu == "📂 Base Mestra":
        st.title("📂 Carregar Catálogo")
        f_mestre = st.file_uploader("Ficheiro Excel Principal", type="xlsx")
        if f_mestre and st.button("🚀 Sincronizar Catálogo", use_container_width=True):
            df_m = pd.read_excel(f_mestre, dtype=str)
            coll = get_coll("master_csv_store")
            for d in coll.stream(): d.reference.delete()
            
            csv_data = df_m.to_csv(index=False)
            size = 800000
            parts = [csv_data[i:i+size] for i in range(0, len(csv_data), size)]
            for i, p in enumerate(parts):
                coll.document(f"p_{i}").set({"part": i, "csv_data": p})
            st.cache_data.clear()
            st.success("Base Mestra sincronizada!"); st.balloons()

if __name__ == "__main__":
    main()