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
        app_id = "marcius-stock-final"
        
        if not firebase_admin._apps:
            cred = credentials.Certificate(config)
            firebase_admin.initialize_app(cred, name=app_id)
        
        app_inst = firebase_admin.get_app(app_id)
        return firestore.client(app=app_inst), None
    except Exception as e:
        return None, str(e)

db, erro_conexao = inicializar_firebase()
APP_ID = "marcius-stock-final"

# --- 2. GESTÃO DE DADOS (FIRESTORE) ---

def get_coll(nome_colecao):
    """Obtém a colecção seguindo a regra de caminhos do ambiente."""
    if db is None: return None
    return db.collection("artifacts").document(APP_ID).collection("public").document("data").collection(nome_colecao)

@st.cache_data(ttl=30)
def carregar_catalogo_nuvem():
    """Lê a Base Mestra (10.000+ itens) via protocolo Super-CSV."""
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
        
        # Tratamento numérico para cálculos
        if not df.empty:
            df["Peso"] = pd.to_numeric(df["Peso"].astype(str).str.replace(",", "."), errors="coerce").fillna(0)
            for c in ["Larg", "Comp", "Esp"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c].astype(str).str.replace(",", "."), errors="coerce").fillna(0)
        return df
    except: return pd.DataFrame()

def carregar_movimentacoes_nuvem():
    """Lê o histórico de entradas e saídas."""
    coll = get_coll("movements")
    if coll is None: return pd.DataFrame()
    try:
        docs = coll.stream()
        data = [doc.to_dict() for doc in docs]
        return pd.DataFrame(data)
    except: return pd.DataFrame()

# --- 3. LÓGICA DE INVENTÁRIO (SOMA DINÂMICA) ---

def calcular_estoque_atual():
    """Cruza a base mestra com as movimentações para dar o saldo real."""
    base = carregar_catalogo_nuvem()
    if base.empty: return pd.DataFrame()
    
    # Padronização de colunas
    cols_tec = ["Material", "Obra", "ElementoPEP", "Grau", "Esp", "Larg", "Comp"]
    for c in cols_tec:
        if c in base.columns:
            base[c] = base[c].astype(str).str.strip().str.upper()

    # Inventário Inicial (1 linha = 1 peça)
    inv = base.groupby(cols_tec).agg({
        "DescritivoMaterial": "first",
        "Peso": "first",
        "LVM": "first",
        "Material": "count"
    }).rename(columns={"Material": "Pecas_Iniciais"}).reset_index()
    
    # Aplicar Movimentações
    movs = carregar_movimentacoes_nuvem()
    if not movs.empty:
        for c in ["Material", "Obra", "ElementoPEP"]:
            movs[c] = movs[c].astype(str).str.strip().str.upper()
        
        movs["Qtd_N"] = pd.to_numeric(movs["Qtde"], errors="coerce").fillna(0)
        # Impacto: Entrada e TDMA somam | Saída e TMA subtraem
        movs["Impacto"] = movs.apply(lambda x: x["Qtd_N"] if x["Tipo"] in ["ENTRADA", "TDMA"] else -x["Qtd_N"], axis=1)
        
        resumo_movs = movs.groupby(["Material", "Obra", "ElementoPEP"])["Impacto"].sum().reset_index()
        inv = pd.merge(inv, resumo_movs, on=["Material", "Obra", "ElementoPEP"], how="left").fillna(0)
    else:
        inv["Impacto"] = 0
        
    inv["Saldo_Final_Pecas"] = inv["Pecas_Iniciais"] + inv["Impacto"]
    inv["Saldo_Final_KG"] = inv["Saldo_Final_Pecas"] * inv["Peso"]
    
    # Remover itens com saldo zero (opcional)
    return inv[inv["Saldo_Final_Pecas"] > 0]

# --- 4. INTERFACE STREAMLIT ---

def main():
    st.set_page_config(page_title="Marcius Stock Pro", layout="wide", page_icon="🏗️")

    # --- LOGÓTIPO E CABEÇALHO ---
    st.sidebar.markdown("<h1 style='text-align: center; color: #FF4B4B;'>🏗️ MARCIUS STOCK</h1>", unsafe_allow_html=True)
    st.sidebar.markdown("<p style='text-align: center; font-size: 0.8em;'>Gestão de Chapas e Estruturas</p>", unsafe_allow_html=True)
    st.sidebar.divider()

    if db is None:
        st.error("🔴 FIREBASE DESCONECTADO")
        st.info("Configure as Secrets para ativar o sistema.")
        return
    
    # Login
    if "logado" not in st.session_state: st.session_state.logado = False
    if not st.session_state.logado:
        st.title("Acesso ao Sistema")
        u = st.text_input("Utilizador")
        p = st.text_input("Palavra-passe", type="password")
        if st.button("Entrar", use_container_width=True):
            if u == "marcius.arruda" and p == "MwsArruda":
                st.session_state.logado = True
                st.rerun()
            else: st.error("Incorreto.")
        return

    # Menu de Navegação
    menu = st.sidebar.radio("Navegação", ["📊 Dashboard", "🔄 Registo Manual", "📂 Base Mestra"])
    
    if st.sidebar.button("Terminar Sessão"):
        st.session_state.logado = False
        st.rerun()

    # --- PÁGINA: DASHBOARD ---
    if menu == "📊 Dashboard":
        st.title("📊 Painel de Controle de Saldos")
        df = calcular_estoque_atual()
        
        if df.empty:
            st.info("Aguardando carregamento da Base Mestra...")
            return

        # FILTROS TÉCNICOS (Onde estão todos os filtros pedidos)
        with st.sidebar.expander("🔍 Filtros de Chapa", expanded=True):
            f_obra = st.multiselect("Obra", sorted(df["Obra"].unique()))
            f_pep = st.multiselect("PEP", sorted(df["ElementoPEP"].unique()))
            f_grau = st.multiselect("Grau", sorted(df["Grau"].unique()))
            f_esp = st.multiselect("Espessura", sorted(df["Esp"].unique()))
            f_larg = st.multiselect("Largura", sorted(df["Larg"].unique()))
            f_comp = st.multiselect("Comprimento", sorted(df["Comp"].unique()))

        # Aplicação dos Filtros
        df_v = df.copy()
        if f_obra: df_v = df_v[df_v["Obra"].isin(f_obra)]
        if f_pep: df_v = df_v[df_v["ElementoPEP"].isin(f_pep)]
        if f_grau: df_v = df_v[df_v["Grau"].isin(f_grau)]
        if f_esp: df_v = df_v[df_v["Esp"].isin(f_esp)]
        if f_larg: df_v = df_v[df_v["Larg"].isin(f_larg)]
        if f_comp: df_v = df_v[df_v["Comp"].isin(f_comp)]

        # KPIs PRINCIPAIS
        c1, c2, c3 = st.columns(3)
        c1.metric("Peças em Stock", f"{int(df_v['Saldo_Final_Pecas'].sum()):,}")
        c2.metric("Peso Total (KG)", f"{df_v['Saldo_Final_KG'].sum():,.2f}")
        c3.metric("Obras Ativas", len(df_v["Obra"].unique()))

        # GRÁFICOS (Solicitados)
        st.divider()
        g1, g2 = st.columns(2)
        
        with g1:
            st.subheader("Distribuição por Obra (Top 10)")
            fig_obra = px.pie(df_v.groupby("Obra")["Saldo_Final_Pecas"].sum().reset_index().nlargest(10, "Saldo_Final_Pecas"), 
                             values="Saldo_Final_Pecas", names="Obra", hole=0.4)
            st.plotly_chart(fig_obra, use_container_width=True)
            
        with g2:
            st.subheader("Peso por Grau de Aço")
            fig_grau = px.bar(df_v.groupby("Grau")["Saldo_Final_KG"].sum().reset_index(), 
                             x="Grau", y="Saldo_Final_KG", color="Grau")
            st.plotly_chart(fig_grau, use_container_width=True)

        st.subheader("Detalhamento de Stock")
        st.dataframe(df_v, use_container_width=True, hide_index=True)

    # --- PÁGINA: REGISTO MANUAL (ENTRADA, SAIDA, TMA, TDMA) ---
    elif menu == "🔄 Registo Manual":
        st.title("🔄 Registo de Movimentações")
        base = carregar_catalogo_nuvem()
        
        if base.empty:
            st.warning("Carregue a Base Mestra antes de realizar registos.")
            return

        col1, col2 = st.columns(2)
        with col1:
            tipo = st.selectbox("Tipo de Operação", ["SAIDA", "ENTRADA", "TMA", "TDMA"], 
                               help="SAIDA: Obra | ENTRADA: Compra | TMA: Transf. | TDMA: Devolução")
            mat = st.selectbox("Código do Material", sorted(base["Material"].unique()))
            qtd = st.number_input("Quantidade (Peças)", min_value=1, step=1)
        
        with col2:
            obra = st.text_input("Obra Destino/Origem").upper()
            pep = st.text_input("Elemento PEP").upper()
            obs = st.text_area("Observações")

        if st.button("Confirmar Movimentação", type="primary", use_container_width=True):
            with st.spinner("A registar..."):
                dados_mov = {
                    "Tipo": tipo,
                    "Material": mat,
                    "Qtde": qtd,
                    "Obra": obra,
                    "ElementoPEP": pep,
                    "Observacao": obs,
                    "Data": datetime.now().strftime("%d/%m/%Y"),
                    "timestamp": firestore.SERVER_TIMESTAMP
                }
                get_coll("movements").add(dados_mov)
                st.success(f"Movimento de {tipo} registado com sucesso!")
                st.balloons()
                time.sleep(1)
                st.rerun()

        st.divider()
        st.subheader("Últimos Registos")
        movs_df = carregar_movimentacoes_nuvem()
        if not movs_df.empty:
            st.dataframe(movs_df.sort_values(by="Data", ascending=False).head(10), use_container_width=True)

    # --- PÁGINA: BASE MESTRA ---
    elif menu == "📂 Base Mestra":
        st.title("📂 Gestão da Base Mestra")
        st.write("Sincronize o catálogo principal de 10.000+ linhas.")
        
        f = st.file_uploader("Subir Ficheiro Excel (.xlsx)", type=["xlsx"])
        if f:
            df_up = pd.read_excel(f, dtype=str)
            st.success(f"{len(df_up)} linhas detectadas.")
            
            if st.button("🚀 Sincronizar Catálogo com a Nuvem"):
                # Protocolo Super-CSV (compactação para 10k itens)
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
                st.success("Catálogo Sincronizado!")
                st.rerun()

        if st.sidebar.button("🗑️ Limpar Tudo (RESET)"):
            for c in ["master_csv_store", "movements"]:
                docs = get_coll(c).stream()
                for d in docs: d.reference.delete()
            st.cache_data.clear()
            st.rerun()

if __name__ == "__main__":
    main()