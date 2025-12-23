# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import io
import time

# --- 1. CONFIGURAÇÃO FIREBASE ---

def init_db():
    try:
        config = None
        if hasattr(st, "secrets") and "firebase" in st.secrets:
            config = dict(st.secrets["firebase"])
        elif "__firebase_config" in globals():
            config = globals()["__firebase_config"]
            if isinstance(config, str): config = json.loads(config)
        
        if not config:
            return None, "stock-marcius", "Configuração Firebase não encontrada."

        app_id = globals().get("__app_id", "stock-marcius")
        app_name = f"app_{app_id}"

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
        return firestore.client(app=app_inst), app_id, None
    except Exception as e:
        return None, "stock-marcius", str(e)

db, APP_ID, conn_error = init_db()

# --- 2. ESTADO DA SESSÃO ---
if "logged_in" not in st.session_state: st.session_state["logged_in"] = False
if "staging_df" not in st.session_state: st.session_state["staging_df"] = None

# --- 3. GESTÃO DE DADOS (PROTOCOLO SUPER-CSV) ---

def get_path(coll_name):
    if not db: return None
    # Path obrigatório seguindo a regra 1 do sistema
    return db.collection("artifacts").document(APP_ID).collection("public").document("data").collection(coll_name)

@st.cache_data(ttl=10)
def carregar_base_mestra_nuvem():
    """Lê a string CSV da nuvem e reconstrói o DataFrame."""
    coll = get_path("master_csv_store")
    if not coll: return pd.DataFrame()
    try:
        docs = coll.order_by("part").stream()
        csv_completo = ""
        for doc in docs:
            csv_completo += doc.to_dict().get("csv_data", "")
        
        if not csv_completo: return pd.DataFrame()
        
        df = pd.read_csv(io.StringIO(csv_completo), dtype=str)
        
        # Tratamento numérico para cálculos
        if not df.empty:
            df["Peso"] = pd.to_numeric(df["Peso"].astype(str).str.replace(",", "."), errors="coerce").fillna(0)
            for c in ["Larg", "Comp", "Esp"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c].astype(str).str.replace(",", "."), errors="coerce").fillna(0)
        return df
    except:
        return pd.DataFrame()

def guardar_base_como_csv(df):
    """Converte para CSV e salva em blocos no Firestore."""
    coll = get_path("master_csv_store")
    if not coll: return False
    
    try:
        # 1. Limpar anterior
        for d in coll.stream(): d.reference.delete()
        
        # 2. Gerar CSV
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_string = csv_buffer.getvalue()
        
        # 3. Blocos de 800KB
        chunk_size = 800000 
        parts = [csv_string[i:i+chunk_size] for i in range(0, len(csv_string), chunk_size)]
        
        prog = st.progress(0, text="Sincronizando com a Cloud...")
        for idx, part in enumerate(parts):
            coll.document(f"part_{idx}").set({
                "part": idx,
                "csv_data": part,
                "timestamp": firestore.SERVER_TIMESTAMP
            })
            prog.progress((idx + 1) / len(parts))
        
        prog.empty()
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Erro na sincronização: {e}")
        return False

def limpar_colecao(nome_colecao):
    """Apaga todos os documentos de uma coleção específica."""
    coll = get_path(nome_colecao)
    if not coll: return False
    try:
        docs = list(coll.list_documents())
        if not docs: return True
        for i in range(0, len(docs), 400):
            batch = db.batch()
            for doc in docs[i:i+400]:
                batch.delete(doc)
            batch.commit()
        return True
    except:
        return False

def load_movements():
    coll = get_path("movements")
    if not coll: return []
    try:
        docs = coll.stream()
        movs = []
        for doc in docs:
            d = doc.to_dict()
            ts = d.get("timestamp")
            d["Data_Exibicao"] = ts.strftime("%d/%m/%Y %H:%M") if ts and hasattr(ts, "strftime") else d.get("Data", "N/A")
            movs.append(d)
        return movs
    except: return []

# --- 4. INTERFACE ---

def main():
    if not st.session_state["logged_in"]:
        st.markdown("<h2 style='text-align: center;'>🏗️ Stock Marcius Arruda</h2>", unsafe_allow_html=True)
        u = st.text_input("Usuário").lower().strip()
        p = st.text_input("Senha", type="password")
        if st.button("Entrar"):
            if u == "marcius.arruda" and p == "MwsArruda":
                st.session_state["logged_in"] = True
                st.rerun()
        return

    st.sidebar.title("🏗️ Gestão")
    menu = ["📊 Dashboard", "📂 Base Mestra", "🔄 Registro", "🔧 Diagnóstico"]
    choice = st.sidebar.radio("Navegação", menu)

    if st.sidebar.button("Sair"):
        st.session_state["logged_in"] = False
        st.rerun()

    # --- TELA: DIAGNÓSTICO ---
    if choice == "🔧 Diagnóstico":
        st.header("🔧 Diagnóstico de Dados na Nuvem")
        st.write(f"Conectado ao App ID: `{APP_ID}`")
        
        c1, c2 = st.columns(2)
        
        coll_base = get_path("master_csv_store")
        docs_base = list(coll_base.stream())
        c1.metric("Partes de Catálogo", len(docs_base))
        
        coll_movs = get_path("movements")
        docs_movs = list(coll_movs.stream())
        c2.metric("Movimentações", len(docs_movs))
        
        if docs_base:
            st.subheader("Conteúdo do Catálogo (Primeiros 200 caracteres)")
            for d in docs_base:
                data = d.to_dict()
                with st.expander(f"Documento: {d.id} (Parte {data.get('part')})"):
                    st.code(data.get('csv_data', '')[:200] + "...")
        else:
            st.warning("Nenhum dado de catálogo encontrado no caminho especificado.")

    # --- TELA: DASHBOARD ---
    elif choice == "📊 Dashboard":
        st.header("📊 Painel de Controle")
        df_base = carregar_base_mestra_nuvem()
        
        if df_base.empty:
            st.info("💡 Nuvem vazia. Carregue o catálogo na aba 'Base Mestra'.")
            return

        with st.sidebar.expander("🔍 Filtros Técnicos", expanded=True):
            f_obra = st.multiselect("Obra", sorted(df_base["Obra"].unique()))
            f_grau = st.multiselect("Grau", sorted(df_base["Grau"].unique()) if "Grau" in df_base.columns else [])
        
        df_v = df_base.copy()
        if f_obra: df_v = df_v[df_v["Obra"].isin(f_obra)]
        if f_grau: df_v = df_v[df_v["Grau"].isin(f_grau)]

        # KPIs: 1 linha = 1 peça
        total_pcs = len(df_v)
        total_kg = df_v["Peso"].sum() if "Peso" in df_v.columns else 0

        st.subheader("📈 Somatório da Seleção")
        k1, k2 = st.columns(2)
        k1.metric("Peças Selecionadas", f"{total_pcs:,} PC")
        k2.metric("Peso Selecionado", f"{total_kg:,.2f} KG")

        st.divider()
        st.dataframe(df_v, use_container_width=True, hide_index=True)

    # --- TELA: BASE MESTRA (RESET + CARGA) ---
    elif choice == "📂 Base Mestra":
        st.header("📂 Gestão de Dados")
        
        tab_carga, tab_reset = st.tabs(["📤 Carregar Catálogo", "⚠️ Zona de Perigo"])
        
        with tab_carga:
            st.write("Suba o Excel de 10.000+ linhas. O sistema processará via CSV.")
            f = st.file_uploader("Arquivo XLSX", type=["xlsx"])
            if f:
                if st.session_state["staging_df"] is None:
                    with st.spinner("Processando..."):
                        df_raw = pd.read_excel(f, dtype=str)
                        df_raw.columns = [str(c).strip() for c in df_raw.columns]
                        cols = ["Obra", "LVM", "Material", "DescritivoMaterial", "Grau", "Esp", "Larg", "Comp", "Peso", "MaterialAplicado", "ElementoPEP"]
                        st.session_state["staging_df"] = df_raw[cols]
                
                st.success(f"✅ {len(st.session_state['staging_df'])} linhas prontas.")
                if st.button("🚀 Confirmar e Sincronizar Agora"):
                    if guardar_base_como_csv(st.session_state["staging_df"]):
                        st.success("Sincronizado!")
                        st.session_state["staging_df"] = None
                        time.sleep(1)
                        st.rerun()

        with tab_reset:
            st.error("Ações permanentes. Use com cautela.")
            if st.button("🗑️ Zerar Todo o Catálogo (Base Mestra)"):
                if limpar_colecao("master_csv_store"):
                    st.success("Catálogo removido.")
                    st.cache_data.clear()
                    st.rerun()
            
            if st.button("🗑️ Zerar Histórico de Movimentações"):
                if limpar_colecao("movements"):
                    st.success("Histórico limpo.")
                    st.rerun()

    # --- TELA: REGISTRO ---
    elif choice == "🔄 Registro":
        st.header("🔄 Registro de Movimento")
        base = carregar_base_mestra_nuvem()
        if base.empty: st.error("Carregue a base primeiro."); return
        
        tipo = st.selectbox("Tipo", ["ENTRADA", "SAIDA", "TMA", "TDMA"])
        with st.form("manual"):
            mat = st.selectbox("Material", sorted(base["Material"].unique()))
            qtd = st.number_input("Qtd", min_value=1, step=1)
            obr = st.text_input("Obra").upper().strip()
            if st.form_submit_button("Confirmar"):
                data = {"Material": mat, "Qtde": qtd, "Tipo": tipo, "Obra": obr, "Data": datetime.now().strftime("%d/%m/%Y")}
                get_path("movements").add(data | {"timestamp": firestore.SERVER_TIMESTAMP})
                st.success("Registrado!")

if __name__ == "__main__":
    main()