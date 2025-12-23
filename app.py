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

# --- 1. CONFIGURAÇÃO DE SEGURANÇA E CONEXÃO ---

def inicializar_firebase():
    """Inicializa a ligação ao Firebase usando as Secrets do Streamlit."""
    try:
        if "firebase" not in st.secrets:
            return None, "Aba 'Secrets' não configurada no Streamlit Cloud."
        
        config = dict(st.secrets["firebase"])
        app_id = "marcius-stock-pro-v7"
        
        if not firebase_admin._apps:
            cred = credentials.Certificate(config)
            firebase_admin.initialize_app(cred, name=app_id)
        
        app_inst = firebase_admin.get_app(app_id)
        return firestore.client(app=app_inst), None
    except Exception as e:
        return None, str(e)

db, erro_conexao = inicializar_firebase()
APP_ID = "marcius-stock-pro-v7"

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
        return pd.DataFrame([doc.to_dict() for doc in docs])
    except: return pd.DataFrame()

# --- 3. LÓGICA DE INVENTÁRIO ---

def calcular_estoque_atual():
    base = carregar_catalogo_nuvem()
    if base.empty: return pd.DataFrame()
    
    cols_tec = ["LVM", "Material", "Obra", "ElementoPEP", "Grau", "Esp", "Larg", "Comp"]
    for c in cols_tec:
        if c in base.columns: base[c] = base[c].astype(str).str.strip().str.upper()

    # Inventário Inicial
    inv = base.groupby(cols_tec).agg({
        "DescritivoMaterial": "first", "Peso": "first", "Material": "count"
    }).rename(columns={"Material": "Pecas_Iniciais"}).reset_index()
    
    movs = carregar_movimentacoes_nuvem()
    if not movs.empty:
        for c in ["LVM", "Material", "Obra", "ElementoPEP"]:
            if c in movs.columns: movs[c] = movs[c].astype(str).str.strip().str.upper()
        
        movs["Qtd_N"] = pd.to_numeric(movs["Qtde"], errors="coerce").fillna(0)
        # Impacto: ENTRADA e TDMA somam | SAIDA subtrai | TMA já terá dois registos (+ e -)
        movs["Impacto"] = movs.apply(lambda x: x["Qtd_N"] if x["Tipo"] in ["ENTRADA", "TDMA"] else -x["Qtd_N"], axis=1)
        
        resumo_movs = movs.groupby(["LVM", "Material", "Obra", "ElementoPEP"])["Impacto"].sum().reset_index()
        inv = pd.merge(inv, resumo_movs, on=["LVM", "Material", "Obra", "ElementoPEP"], how="left").fillna(0)
    else:
        inv["Impacto"] = 0
        
    inv["Saldo_Pecas"] = inv["Pecas_Iniciais"] + inv["Impacto"]
    inv["Saldo_KG"] = inv["Saldo_Pecas"] * inv["Peso"]
    return inv[inv["Saldo_Pecas"] > 0]

# --- 4. INTERFACE ---

def main():
    st.set_page_config(page_title="Marcius Stock Pro", layout="wide", page_icon="🏗️")

    # --- LOGÓTIPO E SIDEBAR ---
    with st.sidebar:
        if os.path.exists("logo_empresa.png"):
            st.image("logo_empresa.png", use_container_width=True)
        else:
            st.markdown("<h1 style='text-align: center; color: #FF4B4B;'>🏗️ MARCIUS STOCK</h1>", unsafe_allow_html=True)
            st.warning("⚠️ Ficheiro 'logo_empresa.png' não encontrado no repositório.")
        
        st.divider()

    if db is None:
        st.error("🔴 FIREBASE DESCONECTADO")
        st.info("Verifique se as chaves TOML foram coladas corretamente na aba Secrets.")
        return
    
    # Login
    if "logado" not in st.session_state: st.session_state.logado = False
    if not st.session_state.logado:
        st.title("🔐 Acesso Restrito")
        u = st.text_input("Utilizador").lower().strip()
        p = st.text_input("Senha", type="password")
        if st.button("Entrar", use_container_width=True):
            if u == "marcius.arruda" and p == "MwsArruda":
                st.session_state.logado = True
                st.rerun()
            else:
                st.error("Credenciais inválidas.")
        return

    # Menu de Navegação Principal
    menu = st.sidebar.radio("Navegação Principal", ["📊 Dashboard", "🔄 Movimentações", "📂 Base Mestra"])
    
    if st.sidebar.button("Terminar Sessão"):
        st.session_state.logado = False
        st.rerun()

    # --- TELA: DASHBOARD ---
    if menu == "📊 Dashboard":
        st.title("📊 Painel de Controle de Stock")
        df = calcular_estoque_atual()
        
        if df.empty:
            st.info("💡 A nuvem está vazia. Por favor, carregue a Base Mestra.")
        else:
            with st.sidebar.expander("🔍 Filtros de Consulta", expanded=True):
                f_lvm = st.multiselect("LVM", sorted(df["LVM"].unique()))
                f_obra = st.multiselect("Obra", sorted(df["Obra"].unique()))
                f_pep = st.multiselect("Elemento PEP", sorted(df["ElementoPEP"].unique()))
                f_grau = st.multiselect("Grau", sorted(df["Grau"].unique()))

            df_v = df.copy()
            if f_lvm: df_v = df_v[df_lvm["LVM"].isin(f_lvm)]
            if f_obra: df_v = df_v[df_v["Obra"].isin(f_obra)]
            if f_pep: df_v = df_v[df_v["ElementoPEP"].isin(f_pep)]
            if f_grau: df_v = df_v[df_v["Grau"].isin(f_grau)]

            c1, c2, c3 = st.columns(3)
            c1.metric("Peças em Stock", f"{int(df_v['Saldo_Pecas'].sum()):,}")
            c2.metric("Peso Total (KG)", f"{df_v['Saldo_KG'].sum():,.2f}")
            c3.metric("LVMs Ativas", len(df_v["LVM"].unique()))

            st.divider()
            g1, g2 = st.columns(2)
            with g1:
                fig1 = px.pie(df_v.groupby("Obra")["Saldo_Pecas"].sum().reset_index().nlargest(10, "Saldo_Pecas"), values="Saldo_Pecas", names="Obra", title="Top 10 Obras (Peças)", hole=0.3)
                st.plotly_chart(fig1, use_container_width=True)
            with g2:
                fig2 = px.bar(df_v.groupby("Grau")["Saldo_KG"].sum().reset_index(), x="Grau", y="Saldo_KG", title="Peso por Grau de Aço", color="Grau")
                st.plotly_chart(fig2, use_container_width=True)

            st.subheader("Lista Detalhada de Itens em Stock")
            st.dataframe(df_v, use_container_width=True, hide_index=True)

    # --- TELA: MOVIMENTAÇÕES (Registo Manual e Upload) ---
    elif menu == "🔄 Movimentações":
        st.title("🔄 Registro de Movimentações")
        base = carregar_catalogo_nuvem()
        
        tab_unit, tab_bulk = st.tabs(["📝 Registro Individual", "📁 Importação em Lote (Excel)"])
        
        with tab_unit:
            st.subheader("Entrada, Saída, TMA ou TDMA")
            with st.form("unit_form"):
                tipo = st.selectbox("Tipo de Operação", ["SAIDA", "ENTRADA", "TMA", "TDMA"])
                mat = st.selectbox("Material", sorted(base["Material"].unique()) if not base.empty else [])
                lvm = st.text_input("LVM").upper().strip()
                qtd = st.number_input("Quantidade (Peças)", min_value=1, step=1)
                
                if tipo == "TMA":
                    col_tma1, col_tma2 = st.columns(2)
                    o_orig = col_tma1.text_input("Obra Origem").upper().strip()
                    o_dest = col_tma2.text_input("Obra Destino").upper().strip()
                    p_orig = col_tma1.text_input("PEP Origem").upper().strip()
                    p_dest = col_tma2.text_input("PEP Destino").upper().strip()
                else:
                    col_std1, col_std2 = st.columns(2)
                    obr = col_std1.text_input("Obra").upper().strip()
                    pep = col_std2.text_input("Elemento PEP").upper().strip()
                
                if st.form_submit_button("Confirmar Registro"):
                    if not lvm or not mat:
                        st.error("Material e LVM são obrigatórios.")
                    else:
                        coll = get_coll("movements")
                        ts = firestore.SERVER_TIMESTAMP
                        dt = datetime.now().strftime("%d/%m/%Y")
                        
                        if tipo == "TMA":
                            # Gera dois registos para TMA: Saída da Origem e Entrada no Destino
                            coll.add({"Tipo": "SAIDA", "Material": mat, "LVM": lvm, "Qtde": qtd, "Obra": o_orig, "ElementoPEP": p_orig, "Data": dt, "timestamp": ts, "Obs": "TMA Origem"})
                            coll.add({"Tipo": "ENTRADA", "Material": mat, "LVM": lvm, "Qtde": qtd, "Obra": o_dest, "ElementoPEP": p_dest, "Data": dt, "timestamp": ts, "Obs": "TMA Destino"})
                        else:
                            coll.add({"Tipo": tipo, "Material": mat, "LVM": lvm, "Qtde": qtd, "Obra": obr, "ElementoPEP": pep, "Data": dt, "timestamp": ts})
                        
                        st.success(f"Movimentação registada com sucesso!")
                        time.sleep(1)
                        st.rerun()

        with tab_bulk:
            tipo_l = st.selectbox("Escolha o Tipo de Arquivo para Upload", ["ENTRADA", "SAIDA", "TMA", "TDMA"], key="bulk_tipo")
            
            # Definição de colunas para o usuário adequar os arquivos
            cols_req = {
                "ENTRADA": ["Material", "LVM", "Qtde", "Obra", "ElementoPEP", "Data"],
                "SAIDA": ["Material", "LVM", "Qtde", "Obra", "ElementoPEP", "Data"],
                "TDMA": ["Material", "LVM", "Qtde", "Obra", "ElementoPEP", "Data"],
                "TMA": ["Material", "LVM", "Qtde", "Obra_Origem", "Obra_Destino", "PEP_Origem", "PEP_Destino", "Data"]
            }
            
            st.info(f"O Excel deve conter exatamente estas colunas: `{', '.join(cols_req[tipo_l])}`")
            f_mov = st.file_uploader(f"Carregar Excel de {tipo_l}", type=["xlsx"])
            
            if f_mov:
                df_mov = pd.read_excel(f_mov, dtype=str)
                df_mov.columns = [c.strip() for c in df_mov.columns]
                faltas = [c for c in cols_req[tipo_l] if c not in df_mov.columns]
                
                if faltas:
                    st.error(f"Ficheiro inválido. Faltam as colunas: {', '.join(faltas)}")
                else:
                    st.success(f"Identificadas {len(df_mov)} linhas para processamento.")
                    if st.button(f"🚀 Iniciar Importação de {tipo_l}"):
                        coll = get_coll("movements")
                        prog = st.progress(0)
                        for idx, row in df_mov.iterrows():
                            ts = firestore.SERVER_TIMESTAMP
                            if tipo_l == "TMA":
                                # Processamento de TMA em lote (gera entrada e saída)
                                coll.add({"Tipo": "SAIDA", "Material": row["Material"], "LVM": row["LVM"], "Qtde": row["Qtde"], "Obra": row["Obra_Origem"], "ElementoPEP": row["PEP_Origem"], "Data": row["Data"], "timestamp": ts})
                                coll.add({"Tipo": "ENTRADA", "Material": row["Material"], "LVM": row["LVM"], "Qtde": row["Qtde"], "Obra": row["Obra_Destino"], "ElementoPEP": row["PEP_Destino"], "Data": row["Data"], "timestamp": ts})
                            else:
                                d = row.to_dict()
                                d["Tipo"] = tipo_l
                                d["timestamp"] = ts
                                coll.add(d)
                            prog.progress((idx + 1) / len(df_mov))
                        
                        st.success("Importação concluída com sucesso!")
                        st.balloons()
                        time.sleep(1)
                        st.rerun()

    # --- TELA: BASE MESTRA ---
    elif menu == "📂 Base Mestra":
        st.title("📂 Gestão da Base Mestra")
        st.warning("⚠️ O carregamento de uma nova Base Mestra irá substituir a atual na nuvem.")
        f = st.file_uploader("Selecione o Ficheiro Excel Principal", type=["xlsx"])
        if f:
            df_up = pd.read_excel(f, dtype=str)
            st.success(f"Catálogo pronto para sincronização ({len(df_up)} linhas).")
            if st.button("🚀 Sincronizar Tudo com o Firebase"):
                coll = get_coll("master_csv_store")
                # Limpa a base anterior para evitar duplicados
                for d in coll.stream(): d.reference.delete()
                
                # Protocolo Super-CSV para performance
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
                st.success("Base Mestra sincronizada com sucesso!")

if __name__ == "__main__":
    main()