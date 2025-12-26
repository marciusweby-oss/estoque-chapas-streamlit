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
            
        app_name = "marcius-estoque-v42"
        
        if not firebase_admin._apps:
            cred = credentials.Certificate(config)
            firebase_admin.initialize_app(cred, name=app_name)
        
        app_inst = firebase_admin.get_app(app_name)
        return firestore.client(app=app_inst), None
    except Exception as e:
        return None, f"Erro: {str(e)}"

db, erro_conexao = inicializar_firebase()
PROJECT_ID = "marcius-estoque-pro-v42"

# --- 2. GESTÃO E PADRONIZAÇÃO DE DADOS ---

def padronizar_colunas(df):
    """Garante que os cabeçalhos sejam idênticos independentemente do Excel."""
    if df.empty: return df
    
    # Converte todos os nomes de colunas para minúsculas e remove espaços para comparar
    df.columns = [str(c).strip().lower() for c in df.columns]
    
    # Mapa de tradução (De qualquer variação para o nosso padrão)
    mapa_traducoes = {
        'codigodomaterial': 'Material',
        'codigomaterial': 'Material',
        'codigo': 'Material',
        'material': 'Material',
        'cinza': 'Grau',
        'grau': 'Grau',
        'espessura': 'Esp',
        'esp': 'Esp',
        'largura': 'Larg',
        'larg': 'Larg',
        'comprimento': 'Comp',
        'comp': 'Comp',
        'peso_unitario': 'Peso',
        'peso': 'Peso',
        'elementopep': 'ElementoPEP',
        'pep': 'ElementoPEP',
        'qtde': 'Qtde',
        'quantidade': 'Qtde',
        'qtd': 'Qtde'
    }
    
    # Aplica a renomeação baseada no que for encontrado
    novas_cols = {}
    for col_original in df.columns:
        if col_original in mapa_traducoes:
            novas_cols[col_original] = mapa_traducoes[col_original]
        else:
            novas_cols[col_original] = col_original.capitalize()
            
    return df.rename(columns=novas_cols)

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
        return padronizar_colunas(df)
    except: return pd.DataFrame()

def carregar_movimentos():
    coll = get_coll("movements")
    if coll is None: return pd.DataFrame()
    try:
        docs = coll.stream()
        df = pd.DataFrame([d.to_dict() for d in docs])
        return padronizar_colunas(df)
    except: return pd.DataFrame()

def carregar_users():
    """Carrega utilizadores e garante a existência do administrador padrão."""
    coll = get_coll("users")
    if coll is None: return {}
    try:
        docs = list(coll.stream())
        users_map = {d.to_dict()["username"].lower().strip(): d.to_dict() for d in docs}
        
        # Se a base estiver vazia, cria o admin padrão para garantir o acesso inicial
        if not users_map:
            admin_data = {"username": "marcius.arruda", "password": "MwsArruda", "nivel": "admin"}
            coll.add(admin_data)
            return {"marcius.arruda": admin_data}
            
        return users_map
    except: 
        return {}

# --- 3. LÓGICA DE CÁLCULO (SOMA E SALDO) ---

def calcular_saldos():
    base = carregar_base_mestra()
    if base.empty: return pd.DataFrame()
    
    # Chaves cruciais para o estoque
    chaves = ["LVM", "Material", "Obra", "ElementoPEP"]
    especs = ["Grau", "Esp", "Larg", "Comp"]
    
    # Limpeza e Padronização de Valores
    for c in chaves + especs:
        if c in base.columns:
            base[c] = base[c].astype(str).str.strip().str.upper()
            base[c] = base[c].apply(lambda x: x.replace(".0", "") if x.endswith(".0") else x)

    # Inventário Base
    inv = base.groupby(chaves + especs).agg({
        "Descritivomaterial": "first", 
        "Peso": "first"
    }).reset_index()
    
    contagem_inicial = base.groupby(chaves).size().reset_index(name='Qtd_Inicial')
    inv = pd.merge(inv, contagem_inicial, on=chaves, how="left")
    
    movs = carregar_movimentos()
    if not movs.empty:
        for c in chaves:
            if c in movs.columns:
                movs[c] = movs[c].astype(str).str.strip().str.upper()
                movs[c] = movs[c].apply(lambda x: x.replace(".0", "") if x.endswith(".0") else x)
        
        if "Qtde" in movs.columns:
            movs["Qtd_N"] = pd.to_numeric(movs["Qtde"], errors="coerce").fillna(0)
            movs["Impacto"] = movs.apply(
                lambda x: x["Qtd_N"] if str(x.get("Tipo", "")).strip().upper() in ["ENTRADA", "TDMA"] 
                else -x["Qtd_N"], axis=1
            )
            
            resumo = movs.groupby(chaves)["Impacto"].sum().reset_index()
            inv = pd.merge(inv, resumo, on=chaves, how="left").fillna(0)
        else:
            inv["Impacto"] = 0
    else:
        inv["Impacto"] = 0
        
    inv["Saldo_Pecas"] = inv["Qtd_Inicial"] + inv["Impacto"]
    inv["Peso_N"] = pd.to_numeric(inv["Peso"].astype(str).str.replace(",", "."), errors="coerce").fillna(0)
    inv["Saldo_KG"] = inv["Saldo_Pecas"] * inv["Peso_N"]
    
    return inv[inv["Saldo_Pecas"] > 0].sort_values(by=["Obra", "LVM"])

# --- 4. RELATÓRIOS ---

def gerar_pdf(df):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("helvetica", "B", 14)
    pdf.cell(0, 10, "Mapa de Estoque - Gestão de Chapas", ln=True, align="C")
    pdf.set_font("helvetica", "", 8)
    pdf.ln(5)
    headers = ["LVM", "Material", "Obra", "Grau", "Qtd", "Kg"]
    col_width = 31.5
    for h in headers: pdf.cell(col_width, 7, h, 1, 0, "C")
    pdf.ln()
    for _, r in df.iterrows():
        pdf.cell(col_width, 6, str(r['LVM'])[:12], 1)
        pdf.cell(col_width, 6, str(r['Material'])[:12], 1)
        pdf.cell(col_width, 6, str(r['Obra'])[:12], 1)
        pdf.cell(col_width, 6, str(r['Grau'])[:12], 1)
        pdf.cell(col_width, 6, str(int(r['Saldo_Pecas'])), 1, 0, "R")
        pdf.cell(col_width, 6, f"{r['Saldo_KG']:,.1f}", 1, 1, "R")
    return pdf.output()

# --- 5. INTERFACE PRINCIPAL ---

def main():
    st.set_page_config(page_title="Gestão de Estoque Chapas", layout="wide", page_icon="🏗️")

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

    # Tenta carregar os utilizadores da nuvem
    users = carregar_users()
    
    if "logado" not in st.session_state: st.session_state.logado = False

    if not st.session_state.logado:
        st.markdown("<br><h1 style='text-align: center; color: #1e3a8a;'>🏗️ Sistema de Gestão de Estoque</h1>", unsafe_allow_html=True)
        _, col, _ = st.columns([1, 2, 1])
        with col:
            st.markdown("<div class='login-card'>", unsafe_allow_html=True)
            u_input = st.text_input("Utilizador").lower().strip()
            p_input = st.text_input("Senha", type="password").strip()
            
            if st.button("ACESSAR SISTEMA"):
                if u_input in users:
                    if users[u_input]["password"] == p_input:
                        st.session_state.logado = True
                        st.session_state.user = users[u_input]
                        st.rerun()
                    else:
                        st.error("Senha incorreta. Verifique maiúsculas e minúsculas.")
                else:
                    st.error(f"Utilizador '{u_input}' não encontrado na base de dados.")
            st.markdown("</div>", unsafe_allow_html=True)
            
            with st.expander("💡 Dica de Acesso"):
                st.write(f"Utilizadores detectados: {', '.join(users.keys())}")
                st.write("Admin padrão: marcius.arruda / MwsArruda")
        return

    nav = ["📊 Dashboard", "🔄 Movimentações", "👤 Conta"]
    if st.session_state.user.get('nivel') == "admin": 
        nav += ["📂 Base Mestra", "👥 Gestão de Acessos"]
    
    menu = st.sidebar.radio("Navegação", nav)
    st.sidebar.divider()
    if st.sidebar.button("Terminar Sessão"):
        st.session_state.logado = False
        st.rerun()

    # --- TELA: DASHBOARD ---
    if menu == "📊 Dashboard":
        st.title("📊 Controle de Estoque de Chapas")
        df = calcular_saldos()
        
        if df.empty:
            st.info("💡 Nenhum saldo encontrado. Importe a Base Mestra ou registre Entradas.")
        else:
            st.sidebar.header("🔍 Filtros de Busca")
            def get_opts(col): return sorted(df[col].unique().tolist())

            f_obra = st.sidebar.multiselect("Obra", get_opts("Obra"))
            f_pep = st.sidebar.multiselect("Elemento PEP", get_opts("ElementoPEP"))
            f_grau = st.sidebar.multiselect("Grau", get_opts("Grau"))
            f_esp = st.sidebar.multiselect("Espessura", get_opts("Esp"))
            f_larg = st.sidebar.multiselect("Largura", get_opts("Larg"))
            f_comp = st.sidebar.multiselect("Comprimento", get_opts("Comp"))
            f_lvm = st.sidebar.text_input("Pesquisar LVM").upper().strip()

            df_v = df.copy()
            if f_obra: df_v = df_v[df_v["Obra"].isin(f_obra)]
            if f_pep: df_v = df_v[df_v["ElementoPEP"].isin(f_pep)]
            if f_grau: df_v = df_v[df_v["Grau"].isin(f_grau)]
            if f_esp: df_v = df_v[df_v["Esp"].isin(f_esp)]
            if f_larg: df_v = df_v[df_v["Larg"].isin(f_larg)]
            if f_comp: df_v = df_v[df_v["Comp"].isin(f_comp)]
            if f_lvm: df_v = df_v[df_v["LVM"].str.contains(f_lvm)]

            c1, c2, c3 = st.columns(3)
            c1.metric("Peças Totais", f"{int(df_v['Saldo_Pecas'].sum()):,}")
            c2.metric("Peso Total (KG)", f"{df_v['Saldo_KG'].sum():,.1f}")
            c3.metric("LVMs Ativas", len(df_v["LVM"].unique()))
            
            st.divider()
            
            g1, g2 = st.columns(2)
            with g1:
                st.plotly_chart(px.pie(df_v.groupby("Obra")["Saldo_Pecas"].sum().reset_index().nlargest(10, "Saldo_Pecas"), values="Saldo_Pecas", names="Obra", title="Top 10 Obras", hole=0.4), use_container_width=True)
            with g2:
                st.plotly_chart(px.bar(df_v.groupby("Grau")["Saldo_KG"].sum().reset_index(), x="Grau", y="Saldo_KG", title="Peso por Grau", color="Grau"), use_container_width=True)
            
            if st.button("📥 Exportar PDF"):
                pdf_data = gerar_pdf(df_v)
                st.download_button("💾 Baixar PDF", pdf_data, f"estoque_{datetime.now().strftime('%d%m%Y')}.pdf", "application/pdf")
            
            st.dataframe(df_v.drop(columns=["Peso_N", "Impacto"]), use_container_width=True, hide_index=True)

    # --- TELA: MOVIMENTAÇÕES ---
    elif menu == "🔄 Movimentações":
        st.title("🔄 Registro de Entradas e Saídas")
        base_cat = carregar_base_mestra()
        if base_cat.empty: st.error("Carregue a Base Mestra primeiro."); return
        
        t1, t2 = st.tabs(["📝 Individual", "📁 Lote (Excel)"])
        
        with t1:
            with st.form("f_ind"):
                tipo = st.selectbox("Operação", ["SAIDA", "ENTRADA", "TMA", "TDMA"])
                mat = st.selectbox("Material", sorted(base_cat["Material"].unique()))
                lvm = st.text_input("LVM").upper().strip()
                qtd = st.number_input("Quantidade", min_value=1, step=1)
                obr = st.text_input("Obra").upper().strip()
                pep = st.text_input("PEP").upper().strip()
                if st.form_submit_button("GRAVAR REGISTRO"):
                    get_coll("movements").add({
                        "Tipo": tipo, "Material": mat, "LVM": lvm, "Qtde": qtd, 
                        "Obra": obr, "ElementoPEP": pep, 
                        "Data": datetime.now().strftime('%d/%m/%Y'), 
                        "timestamp": firestore.SERVER_TIMESTAMP
                    })
                    st.success("Gravado!"); time.sleep(0.5); st.rerun()
        
        with t2:
            st.subheader("📁 Importação Excel")
            tp_lote = st.selectbox("Tipo de Movimento", ["ENTRADA", "SAIDA", "TMA", "TDMA"])
            f_lote = st.file_uploader(f"Selecione o Excel de {tp_lote}", type="xlsx")
            if f_lote and st.button("🚀 Iniciar Importação"):
                df_up = pd.read_excel(f_lote, dtype=str)
                df_up = padronizar_colunas(df_up)
                
                coll = get_coll("movements")
                ts = firestore.SERVER_TIMESTAMP
                p_bar = st.progress(0)
                for i, r in df_up.iterrows():
                    d = {str(k).strip(): str(v).strip() for k, v in r.to_dict().items() if pd.notna(v)}
                    d["Tipo"] = tp_lote
                    d["timestamp"] = ts
                    coll.add(d)
                    p_bar.progress((i + 1) / len(df_up))
                st.success("Importação concluída!"); time.sleep(1); st.rerun()

    # --- TELA: BASE MESTRA ---
    elif menu == "📂 Base Mestra":
        st.title("📂 Catálogo de Materiais")
        f = st.file_uploader("Carregar Excel Master", type="xlsx")
        if f and st.button("🚀 SINCRONIZAR"):
            df_m = pd.read_excel(f, dtype=str)
            coll = get_coll("master_csv_store")
            for d in coll.stream(): d.reference.delete()
            csv_t = df_m.to_csv(index=False)
            size = 800000
            for i, p in enumerate([csv_t[i:i+size] for i in range(0, len(csv_t), size)]):
                coll.document(f"p_{i}").set({"part": i, "csv_data": p})
            st.cache_data.clear()
            st.success("Catálogo atualizado!"); st.balloons()

    # --- TELA: GESTÃO DE ACESSOS ---
    elif menu == "👥 Gestão de Acessos":
        st.title("👥 Gestão de Equipa")
        with st.form("f_add_user"):
            new_u = st.text_input("Novo Utilizador").lower().strip()
            new_p = st.text_input("Senha")
            new_n = st.selectbox("Nível", ["operador", "admin", "consulta"])
            if st.form_submit_button("CRIAR CONTA"):
                if new_u and new_p:
                    get_coll("users").add({"username": new_u, "password": new_p, "nivel": new_n})
                    st.success(f"Utilizador {new_u} criado!")
                    st.rerun()
        st.divider()
        st.subheader("Utilizadores Ativos")
        for u_name, u_data in users.items():
            st.write(f"• **{u_name}** | Nível: {u_data.get('nivel', 'N/A')}")

if __name__ == "__main__":
    main()