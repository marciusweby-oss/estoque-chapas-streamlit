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
        return firestore.client(app=firebase_admin.get_app(app_name)), None
    except Exception as e:
        return None, str(e)

db, erro_conexao = inicializar_firebase()
PROJECT_ID = "marcius-estoque-pro-v28"

# --- 2. GESTÃO DE DADOS ---

def get_coll(nome):
    if db is None: return None
    return db.collection("artifacts").document(PROJECT_ID).collection("public").document("data").collection(nome)

def padronizar_nome_colunas(df):
    if df.empty: return df
    df.columns = [str(c).strip() for c in df.columns]
    # Troca definitiva de Cinza para Grau e padronização técnica
    df = df.rename(columns={
        'Cinza': 'Grau', 'cinza': 'Grau', 'GRAU': 'Grau',
        'Espessura': 'Esp', 'Largura': 'Larg', 'Comprimento': 'Comp',
        'Descritivomaterial': 'DescritivoMaterial', 'DescritivoMaterial': 'DescritivoMaterial'
    })
    return df

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
        df = padronizar_nome_colunas(df)
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
        return padronizar_nome_colunas(df)
    except: return pd.DataFrame()

def carregar_users():
    coll = get_coll("users")
    if coll is None: return {}
    try:
        docs = coll.stream()
        return {d.to_dict()["username"].lower().strip(): d.to_dict() for d in docs}
    except: return {}

# --- 3. LÓGICA DE NEGÓCIO (SOMA E CONSOLIDAÇÃO) ---

def calcular_saldos():
    base = carregar_base_mestra()
    movs = carregar_movimentos()
    chaves = ["LVM", "Material", "Obra", "ElementoPEP", "Grau", "Esp", "Larg", "Comp"]
    
    if not base.empty:
        for c in chaves: 
            if c in base.columns: base[c] = base[c].astype(str).str.strip().str.upper().replace(r'\.0$', '', regex=True)
        
        inv_base = base.groupby(chaves).agg({
            "DescritivoMaterial": "first", "Peso": "first"
        }).reset_index()
        inv_base["Qtd_Inicial"] = base.groupby(chaves).size().values
    else:
        inv_base = pd.DataFrame(columns=chaves + ["Qtd_Inicial", "Peso", "DescritivoMaterial"])

    if not movs.empty:
        for c in chaves:
            if c in movs.columns: movs[c] = movs[c].astype(str).str.strip().str.upper().replace(r'\.0$', '', regex=True)
        
        movs["Qtd_N"] = pd.to_numeric(movs["Qtde"], errors="coerce").fillna(0)
        movs["Impacto"] = movs.apply(lambda x: x["Qtd_N"] if str(x.get("Tipo","")).upper() in ["ENTRADA", "TDMA"] else -x["Qtd_N"], axis=1)
        resumo = movs.groupby(chaves)["Impacto"].sum().reset_index()
    else:
        resumo = pd.DataFrame(columns=chaves + ["Impacto"])

    # MERGE OUTER PARA UNIR TUDO SEM PERDER ENTRADAS
    df_f = pd.merge(inv_base, resumo, on=chaves, how="outer").fillna(0)
    
    # Consolidação final para garantir que o cartão não duplique a soma
    df_f = df_f.groupby(chaves).agg({
        "DescritivoMaterial": "max", "Peso": "max", "Qtd_Inicial": "sum", "Impacto": "sum"
    }).reset_index()

    df_f["Saldo_Pecas"] = df_f["Qtd_Inicial"] + df_f["Impacto"]
    df_f["Peso_N"] = pd.to_numeric(df_f["Peso"].astype(str).str.replace(",", "."), errors="coerce").fillna(0)
    df_f["Saldo_KG"] = df_f["Saldo_Pecas"] * df_f["Peso_N"]
    
    return df_f[df_f["Saldo_Pecas"] > 0].sort_values(by=["Obra", "LVM"])

# --- 4. EXPORTAÇÃO PDF CORRIGIDA ---

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

# --- 5. INTERFACE COMPLETA ---

def main():
    st.set_page_config(page_title="Gestão de Estoque", layout="wide", page_icon="🏗️")

    # Identidade Visual no Topo
    with st.container():
        col_l, col_t = st.columns([1, 4])
        with col_l:
            if os.path.exists("logo_empresa.png"): st.image("logo_empresa.png", width=120)
            else: st.markdown("## 🏗️")
        with col_t: st.markdown("<h1 style='color: #1e3a8a;'>Gestão de Estoque</h1>", unsafe_allow_html=True)
    st.divider()

    if "logado" not in st.session_state: st.session_state.logado = False
    users = carregar_users()

    # --- TELA DE LOGIN ---
    if not st.session_state.logado:
        st.subheader("🔑 Controle de Acesso")
        u = st.text_input("Utilizador").lower().strip()
        p = st.text_input("Senha", type="password")
        if st.button("Acessar Sistema"):
            if u in users and users[u]["password"] == p:
                st.session_state.logado, st.session_state.user = True, users[u]
                st.rerun()
            else: st.error("Credenciais inválidas.")
        return

    # Navegação Completa
    nav = ["📊 Dashboard", "🔄 Movimentações", "👤 Minha Conta"]
    if st.session_state.user.get('nivel') == "Admin": nav += ["📂 Base Mestra", "👥 Gestão de Acessos"]
    menu = st.sidebar.radio("Navegação", nav)
    
    st.sidebar.divider()
    if st.sidebar.button("🚪 Terminar Sessão"):
        st.session_state.logado = False
        st.rerun()

    if menu == "📊 Dashboard":
        st.subheader("📊 Painel de Estoque")
        df = calcular_saldos()
        
        if not df.empty:
            # Filtros na lateral
            st.sidebar.header("🔍 Filtros")
            f_lvm = st.sidebar.text_input("Pesquisar LVM").upper().strip()
            f_mat = st.sidebar.multiselect("Material", sorted(df["Material"].unique()))
            f_obra = st.sidebar.multiselect("Obra", sorted(df["Obra"].unique()))
            f_pep = st.sidebar.multiselect("Elemento PEP", sorted(df["ElementoPEP"].unique()))
            f_grau = st.sidebar.multiselect("Grau", sorted(df["Grau"].unique()))
            f_esp = st.sidebar.multiselect("Espessura", sorted(df["Esp"].unique()))
            f_larg = st.sidebar.multiselect("Largura", sorted(df["Larg"].unique()))
            f_comp = st.sidebar.multiselect("Comprimento", sorted(df["Comp"].unique()))

            # Aplicar filtros dinâmicos
            df_v = df.copy()
            if f_lvm: df_v = df_v[df_v["LVM"].str.contains(f_lvm, na=False)]
            if f_mat: df_v = df_v[df_v["Material"].isin(f_mat)]
            if f_obra: df_v = df_v[df_v["Obra"].isin(f_obra)]
            if f_pep: df_v = df_v[df_v["ElementoPEP"].isin(f_pep)]
            if f_grau: df_v = df_v[df_v["Grau"].isin(f_grau)]
            if f_esp: df_v = df_v[df_v["Esp"].isin(f_esp)]
            if f_larg: df_v = df_v[df_v["Larg"].isin(f_larg)]
            if f_comp: df_v = df_v[df_v["Comp"].isin(f_comp)]

            # MÉTRICAS DINÂMICAS (MUDAM COM O FILTRO LVM)
            c1, c2, c3 = st.columns(3)
            c1.metric("Peças Filtradas", f"{int(df_v['Saldo_Pecas'].sum()):,}")
            c2.metric("Peso Filtrado (KG)", f"{df_v['Saldo_KG'].sum():,.2f}")
            c3.metric("LVMs Ativas", len(df_v["LVM"].unique()))

            # Gráficos e PDF
            st.divider()
            col_btn, _ = st.columns([1, 3])
            if col_btn.button("📥 Gerar Relatório PDF"):
                st.download_button("💾 Baixar PDF", gerar_pdf(df_v), "estoque.pdf", "application/pdf")

            g1, g2 = st.columns(2)
            with g1: st.plotly_chart(px.pie(df_v, values="Saldo_Pecas", names="Obra", hole=0.4, title="Peças por Obra"), use_container_width=True)
            with g2: st.plotly_chart(px.bar(df_v.groupby("Grau")["Saldo_KG"].sum().reset_index(), x="Grau", y="Saldo_KG", title="Peso por Grau (KG)", color="Grau"), use_container_width=True)

            st.dataframe(df_v.drop(columns=["Impacto", "Qtd_Inicial", "Peso_N"], errors="ignore"), use_container_width=True, hide_index=True)
        else: st.info("Sem dados disponíveis no estoque.")

    elif menu == "🔄 Movimentações":
        st.subheader("🔄 Registro de Entradas e Saídas")
        tipo = st.selectbox("Operação", ["ENTRADA", "SAIDA", "TMA", "TDMA"])
        f = st.file_uploader("Arquivo Excel para Importação", type="xlsx")
        if f and st.button("🚀 Importar"):
            df_up = pd.read_excel(f, dtype=str)
            df_up = padronizar_nome_colunas(df_up)
            coll = get_coll("movements")
            for _, r in df_up.iterrows():
                d = r.to_dict(); d["Tipo"] = tipo; d["timestamp"] = firestore.SERVER_TIMESTAMP
                coll.add(d)
            st.success("Importado com sucesso!"); st.rerun()

    elif menu == "📂 Base Mestra":
        st.subheader("📂 Gestão da Base Mestra")
        f = st.file_uploader("Upload Master", type="xlsx")
        if f and st.button("🚀 Sincronizar"):
            df_m = pd.read_excel(f, dtype=str)
            coll = get_coll("master_csv_store")
            for d in coll.stream(): d.reference.delete()
            csv_t = df_m.to_csv(index=False)
            for i, p in enumerate([csv_t[x:x+800000] for x in range(0, len(csv_t), 800000)]):
                coll.document(f"p_{i}").set({"part": i, "csv_data": p})
            st.cache_data.clear()
            st.success("Sincronizado!")
        
        st.divider()
        st.subheader("🗑️ Zona de Limpeza")
        c1, c2 = st.columns(2)
        if c1.button("🗑️ APAGAR BASE MESTRA"):
            for d in get_coll("master_csv_store").stream(): d.reference.delete()
            st.cache_data.clear(); st.rerun()
        if c2.button("🗑️ APAGAR MOVIMENTAÇÕES"):
            for d in get_coll("movements").stream(): d.reference.delete()
            st.rerun()

    elif menu == "👤 Minha Conta":
        st.subheader("👤 Minha Conta")
        st.write(f"Utilizador logado: {st.session_state.user['username']}")
        nova_p = st.text_input("Nova Senha", type="password")
        if st.button("Atualizar"):
            docs = get_coll("users").where("username", "==", st.session_state.user['username']).get()
            for d in docs: d.reference.update({"password": nova_p})
            st.success("Senha atualizada!")

    elif menu == "👥 Gestão de Acessos":
        st.subheader("👥 Usuários do Sistema")
        with st.form("u"):
            nu, np = st.text_input("Username"), st.text_input("Senha")
            nv = st.selectbox("Nível", ["Operador", "Admin"])
            if st.form_submit_button("Criar"):
                get_coll("users").add({"username": nu.lower().strip(), "password": np, "nivel": nv})
                st.rerun()
        for u, d in users.items():
            c1, c2 = st.columns([3, 1])
            c1.write(f"• **{u}** ({d['nivel']})")
            if u != "marcius.arruda" and c2.button("Remover", key=u):
                docs = get_coll("users").where("username", "==", u).get()
                for doc in docs: doc.reference.delete()
                st.rerun()

if __name__ == "__main__":
    main()