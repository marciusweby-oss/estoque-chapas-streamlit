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
        dados = []
        for d in docs:
            item = d.to_dict()
            item['id'] = d.id
            dados.append(item)
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
        if os.path.exists("logo_empresa.png"):
            self.image("logo_empresa.png", 10, 8, 33)
        self.set_font("helvetica", "B", 14)
        self.cell(0, 10, "RELATÓRIO DE ESTOQUE DETALHADO", ln=True, align="C")
        self.set_font("helvetica", "", 10)
        self.cell(0, 10, f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}", ln=True, align="C")
        self.ln(10)

def gerar_pdf(df):
    pdf = EstoquePDF(orientation='L', unit='mm', format='A4')
    pdf.add_page()
    pdf.set_font("helvetica", "B", 8)
    pdf.set_fill_color(200, 220, 255)
    widths = [35, 45, 45, 30, 20, 25, 25, 30, 22]
    cols = ["LVM", "Material", "Obra", "Grau", "Esp", "Larg", "Comp", "Peso Unit", "Qtd"]
    for i in range(len(cols)):
        pdf.cell(widths[i], 8, cols[i], 1, 0, "C", 1)
    pdf.ln()
    pdf.set_font("helvetica", "", 7)
    for _, r in df.iterrows():
        pdf.cell(widths[0], 7, str(r['LVM']), 1)
        pdf.cell(widths[1], 7, str(r['Material'])[:25], 1)
        pdf.cell(widths[2], 7, str(r['Obra'])[:25], 1)
        pdf.cell(widths[3], 7, str(r['Grau']), 1)
        pdf.cell(widths[4], 7, str(r['Esp']), 1, 0, "C")
        pdf.cell(widths[5], 7, str(r['Larg']), 1, 0, "C")
        pdf.cell(widths[6], 7, str(r['Comp']), 1, 0, "C")
        pdf.cell(widths[7], 7, f"{float(r['Peso']):.2f} KG", 1, 0, "R")
        pdf.cell(widths[8], 7, f"{int(r['Saldo_Pecas'])}", 1, 1, "R")
    return bytes(pdf.output(dest='S'))
# --- 5. INTERFACE ---

def main():
    st.set_page_config(page_title="Gestão de Estoque", layout="wide", page_icon="🏗️")

    with st.sidebar:
        if os.path.exists("logo_empresa.png"):
            st.image("logo_empresa.png", use_container_width=True)
        if db is not None:
            st.markdown("<p style='color:green; text-align:center; font-size:0.8em;'>● Ligação Ativa</p>", unsafe_allow_html=True)
        else:
            st.markdown("<p style='color:red; text-align:center; font-size:0.8em;'>● Sem Ligação</p>", unsafe_allow_html=True)
        st.divider()

    if "logado" not in st.session_state: st.session_state.logado = False
    users = carregar_users()

    if not st.session_state.logado:
        col_a, col_b, col_c = st.columns([1, 2, 1])
        with col_b:
            st.markdown("<h2 style='text-align: center;'>Acesso ao Sistema</h2>", unsafe_allow_html=True)
            u_in = st.text_input("Utilizador").lower().strip()
            p_in = st.text_input("Palavra-passe", type="password").strip()
            if st.button("ENTRAR"):
                if u_in in users and users[u_in]["password"] == p_in:
                    st.session_state.logado, st.session_state.user = True, users[u_in]
                    st.rerun()
                else: st.error("Incorreto.")
        return

    user_level = st.session_state.user.get('nivel', 'Consulta')
    nav = ["🔍 Filtros", "📊 Dashboard", "🔄 Movimentações", "👤 Minha Conta"]
    if user_level == "Admin": nav += ["📂 Base Mestra", "👥 Gestão de Acessos"]
    
    menu = st.sidebar.radio("Navegação", nav)

    if st.sidebar.button("🚪 Terminar Sessão"):
        for k in list(st.session_state.keys()): del st.session_state[k]
        st.session_state.logado = False
        st.rerun()

    # --- ABA: FILTROS ---
    if menu == "🔍 Filtros":
        st.title("🔍 Central de Filtros")
        df_full = calcular_saldos()
        if not df_full.empty:
            filtros_cols = ["Material", "Obra", "Grau", "Esp", "Larg", "Comp"]
            for col in filtros_cols:
                if f"filter_{col}" not in st.session_state: st.session_state[f"filter_{col}"] = []
            if "filter_lvm" not in st.session_state: st.session_state.filter_lvm = ""
            st.session_state.filter_lvm = st.text_input("Pesquisar LVM", value=st.session_state.filter_lvm).upper().strip()
            
            def obter_opcoes(coluna_alvo):
                temp_df = df_full.copy()
                if st.session_state.filter_lvm:
                    temp_df = temp_df[temp_df["LVM"].str.contains(st.session_state.filter_lvm, na=False)]
                for col in filtros_cols:
                    if col != coluna_alvo and st.session_state.get(f"filter_{col}"):
                        temp_df = temp_df[temp_df[col].isin(st.session_state[f"filter_{col}"])]
                return sorted(temp_df[coluna_alvo].unique().tolist())
            
            c1, c2 = st.columns(2)
            with c1:
                st.session_state.filter_Material = st.multiselect("Material", obter_opcoes("Material"), key="f1", default=st.session_state.filter_Material)
                st.session_state.filter_Grau = st.multiselect("Grau", obter_opcoes("Grau"), key="f2", default=st.session_state.filter_Grau)
                st.session_state.filter_Larg = st.multiselect("Largura", obter_opcoes("Larg"), key="f3", default=st.session_state.filter_Larg)
            with c2:
                st.session_state.filter_Obra = st.multiselect("Obra", obter_opcoes("Obra"), key="f4", default=st.session_state.filter_Obra)
                st.session_state.filter_Esp = st.multiselect("Espessura", obter_opcoes("Esp"), key="f5", default=st.session_state.filter_Esp)
                st.session_state.filter_Comp = st.multiselect("Comprimento", obter_opcoes("Comp"), key="f6", default=st.session_state.filter_Comp)
            if st.button("Limpar Filtros"):
                for col in filtros_cols: st.session_state[f"filter_{col}"] = []
                st.session_state.filter_lvm = ""
                st.rerun()

    # --- ABA: DASHBOARD ---
    elif menu == "📊 Dashboard":
        st.title("📊 Painel de Stock")
        df_full = calcular_saldos()
        df_v = df_full.copy()
        if st.session_state.get('filter_lvm'):
            df_v = df_v[df_v["LVM"].str.contains(st.session_state.filter_lvm, na=False)]
        for col in ["Material", "Obra", "Grau", "Esp", "Larg", "Comp"]:
            if st.session_state.get(f"filter_{col}"):
                df_v = df_v[df_v[col].isin(st.session_state[f"filter_{col}"])]

        # MÉTRICAS 100% SINCRONIZADAS
        valor_soma_pecas = int(df_v['Saldo_Pecas'].sum()) if not df_v.empty else 0
        valor_soma_peso = df_v['Saldo_KG'].sum() if not df_v.empty else 0.0

        c1, c2, c3 = st.columns(3)
        c1.metric("Peças Filtradas", f"{valor_soma_pecas:,}")
        c2.metric("Peso Filtrado (KG)", f"{valor_soma_peso:,.2f}")
        c3.metric("Total em Tela", f"{valor_soma_pecas:,}")

        st.divider()
        col_p, col_e, _ = st.columns([1,1,2])
        if col_p.button("📥 Baixar PDF"):
            st.download_button("💾 PDF", gerar_pdf(df_v), "relatorio.pdf", "application/pdf")
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df_v.to_excel(writer, index=False, sheet_name='Stock')
        col_e.download_button("📥 Excel", output.getvalue(), "stock_export.xlsx")
        st.dataframe(df_v, use_container_width=True, hide_index=True)

    # --- ABA: MOVIMENTAÇÕES ---
    elif menu == "🔄 Movimentações":
        st.title("🔄 Registro de Movimentações")
        if user_level == "Consulta": st.warning("Restrito."); st.dataframe(carregar_movimentos()); return
        
        tab1, tab2, tab3 = st.tabs(["📝 Manual", "📂 Excel", "📋 Histórico"])
        with tab1:
            with st.form("fm"):
                tipo = st.selectbox("Tipo", ["SAIDA", "ENTRADA", "TMA", "TDMA"])
                c_m1, c_m2 = st.columns(2); lvm = c_m1.text_input("LVM").upper(); qtd = c_m2.number_input("Qtde", min_value=1)
                mat = c_m1.text_input("Material").upper(); obr = c_m2.text_input("Obra").upper()
                if st.form_submit_button("Gravar"):
                    get_coll("movements").add({"Tipo": tipo, "LVM": lvm, "Qtde": str(qtd), "Material": mat, "Obra": obr, "timestamp": firestore.SERVER_TIMESTAMP, "user_owner": st.session_state.user['username']})
                    st.success("OK!")
        with tab2:
            tipo_l = st.selectbox("Operação Lote", ["SAIDA", "ENTRADA", "TMA", "TDMA"])
            f_u = st.file_uploader("Excel", type="xlsx")
            if f_u and st.button("Processar"):
                df_u = pd.read_excel(f_u, dtype=str)
                coll = get_coll("movements")
                for _, r in df_u.iterrows():
                    d = r.to_dict(); d["Tipo"], d["user_owner"] = tipo_l, st.session_state.user['username']; d["timestamp"] = firestore.SERVER_TIMESTAMP; coll.add(d)
                st.success("Importado!")
        with tab3:
            df_h = carregar_movimentos()
            if not df_h.empty:
                if user_level == "Colaborador": df_h = df_h[df_h['user_owner'] == st.session_state.user['username']]
                st.dataframe(df_h)
            
            if user_level == "Admin":
                st.divider(); st.subheader("⚠️ Zona de Perigo")
                if st.checkbox("Confirmar eliminação de movimentos"):
                    if st.button("ZERAR MOVIMENTAÇÕES"):
                        for doc in get_coll("movements").stream(): doc.reference.delete()
                        st.warning("Apagado!"); time.sleep(1); st.rerun()

    # --- ABA: BASE MESTRA ---
    elif menu == "📂 Base Mestra":
        st.title("📂 Gestão Master")
        f = st.file_uploader("Ficheiro Master", type="xlsx")
        if f and st.button("Sincronizar"):
            df_m = pd.read_excel(f, dtype=str)
            coll = get_coll("master_csv_store")
            for d in coll.stream(): d.reference.delete()
            csv_t = df_m.to_csv(index=False)
            for i, p in enumerate([csv_t[x:x+800000] for x in range(0, len(csv_t), 800000)]):
                coll.document(f"p_{i}").set({"part": i, "csv_data": p})
            st.cache_data.clear(); st.success("OK!")
        
        st.divider(); st.subheader("⚠️ Zona de Perigo")
        if st.checkbox("Confirmar eliminação da Base Mestra"):
            if st.button("ZERAR BASE MESTRA"):
                for d in get_coll("master_csv_store").stream(): d.reference.delete()
                st.cache_data.clear(); st.rerun()

    # --- OUTRAS ABAS ---
    elif menu == "👥 Gestão de Acessos":
        st.title("👥 Usuários")
        with st.form("u"):
            nu, np = st.text_input("User"), st.text_input("Senha")
            nv = st.selectbox("Nível", ["Consulta", "Colaborador", "Admin"])
            if st.form_submit_button("Criar"):
                get_coll("users").add({"username": nu.lower(), "password": np, "nivel": nv}); st.rerun()
        for u, d in users.items():
            c1, c2 = st.columns([3, 1]); c1.write(f"• **{u}** ({d.get('nivel')})")
            if u != "marcius.arruda" and c2.button("Remover", key=u):
                for doc in get_coll("users").where("username", "==", u).get(): doc.reference.delete()
                st.rerun()
    elif menu == "👤 Minha Conta":
        st.title("👤 Conta")
        nova_s = st.text_input("Nova Senha", type="password")
        if st.button("Guardar"):
            docs = get_coll("users").where("username", "==", st.session_state.user['username']).get()
            for d in docs: d.reference.update({"password": nova_s})
            st.success("Atualizada!")

if __name__ == "__main__":
    main()