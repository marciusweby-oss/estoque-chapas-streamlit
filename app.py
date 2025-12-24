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
            return None, "ERRO: Secrets não configuradas no Streamlit Cloud."
        
        config = dict(st.secrets["firebase"])
        
        # Tratamento da Private Key para evitar erros de Padding/PEM
        if "private_key" in config:
            pk = config["private_key"]
            pk = pk.replace("\\n", "\n")
            pk = pk.strip().strip('"').strip("'")
            if "-----BEGIN PRIVATE KEY-----" not in pk:
                pk = "-----BEGIN PRIVATE KEY-----\n" + pk
            if "-----END PRIVATE KEY-----" not in pk:
                pk = pk + "\n-----END PRIVATE KEY-----\n"
            config["private_key"] = pk
            
        app_name = "marcius-stock-v25"
        
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
        return firestore.client(app=app_inst), None
    except Exception as e:
        return None, f"Erro de conexão: {str(e)}"

db, erro_conexao = inicializar_firebase()
PROJECT_ID = "marcius-stock-pro-v25"

# --- 2. GESTÃO DE DADOS ---

def get_coll(nome):
    if db is None: return None
    # Estrutura obrigatória para persistência no Firestore
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
        # Conversão de colunas técnicas para filtros numéricos
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
        return pd.DataFrame(dados) if dados else pd.DataFrame()
    except: return pd.DataFrame()

def carregar_users():
    coll = get_coll("users")
    if coll is None: return {}
    try:
        docs = coll.stream()
        users = {d.to_dict()["username"]: d.to_dict() for d in docs}
        if not users:
            admin = {"username": "marcius.arruda", "password": "MwsArruda", "nivel": "Admin"}
            coll.add(admin)
            return {"marcius.arruda": admin}
        return users
    except: return {}

# --- 3. LÓGICA DE NEGÓCIO ---

def calcular_saldos():
    base = carregar_base_mestra()
    if base.empty: return pd.DataFrame()
    
    chaves = ["LVM", "Material", "Obra", "ElementoPEP"]
    especs = ["Grau", "Esp", "Larg", "Comp"]
    todas_cols = chaves + especs
    
    for c in todas_cols:
        if c in base.columns: base[c] = base[c].astype(str).str.strip().str.upper()

    # Agrupamento inicial do cadastro
    inv = base.groupby(todas_cols).agg({
        "DescritivoMaterial": "first", "Peso": "first", "Material": "count"
    }).rename(columns={"Material": "Qtd_Inicial"}).reset_index()
    
    movs = carregar_movimentos()
    if not movs.empty and "Tipo" in movs.columns:
        for c in chaves:
            if c in movs.columns: movs[c] = movs[c].astype(str).str.strip().str.upper()
        
        movs["Qtd_N"] = pd.to_numeric(movs["Qtde"], errors="coerce").fillna(0)
        # Lógica de impacto no estoque
        movs["Impacto"] = movs.apply(lambda x: x["Qtd_N"] if str(x["Tipo"]).upper() in ["ENTRADA", "TDMA"] else -x["Qtd_N"], axis=1)
        
        resumo = movs.groupby(chaves)["Impacto"].sum().reset_index()
        inv = pd.merge(inv, resumo, on=chaves, how="left").fillna(0)
    else:
        inv["Impacto"] = 0
        
    inv["Saldo_Pecas"] = inv["Qtd_Inicial"] + inv["Impacto"]
    inv["Saldo_KG"] = inv["Saldo_Pecas"] * inv["Peso"]
    
    return inv[inv["Saldo_Pecas"] > 0].sort_values(by=["Obra", "LVM"])

# --- 4. EXPORTAÇÃO PDF ---

class StockPDF(FPDF):
    def header(self):
        if os.path.exists("logo_empresa.png"):
            self.image("logo_empresa.png", 10, 8, 25)
        self.set_font("helvetica", "B", 14)
        self.cell(0, 10, "MAPA DE STOCK - MARCIUS STOCK", ln=True, align="R")
        self.set_font("helvetica", "I", 8)
        self.cell(0, 5, f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M')}", ln=True, align="R")
        self.ln(10)

def gerar_pdf(df):
    pdf = StockPDF()
    pdf.alias_nb_pages()
    pdf.add_page()
    pdf.set_font("helvetica", "B", 7)
    pdf.set_fill_color(240, 240, 240)
    cols = ["LVM", "Material", "Obra", "Grau", "Esp.", "Larg.", "Comp.", "Qtd"]
    ws = [25, 35, 30, 20, 15, 20, 20, 25]
    for i in range(len(cols)):
        pdf.cell(ws[i], 8, cols[i], 1, 0, "C", 1)
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
    pdf.cell(0, 10, f"TOTAL: {int(df['Saldo_Pecas'].sum())} Peças | {df['Saldo_KG'].sum():,.2f} KG", align="R")
    return pdf.output()

# --- 5. INTERFACE ---

def main():
    st.set_page_config(page_title="Marcius Stock Pro", layout="wide", page_icon="🏗️")

    st.markdown("""
        <style>
            .stButton>button { width: 100%; border-radius: 12px; height: 3.8em; font-weight: bold; background-color: #f8f9fa; border: 1px solid #d1d3e2; }
            .stTextInput>div>div>input { height: 3.5em; border-radius: 8px; }
            .login-card { background-color: #ffffff; padding: 25px; border-radius: 15px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
            .stTabs [data-baseweb="tab-list"] { gap: 20px; }
            .stTabs [data-baseweb="tab"] { height: 50px; white-space: pre-wrap; font-weight: bold; }
        </style>
    """, unsafe_allow_html=True)

    with st.sidebar:
        if os.path.exists("logo_empresa.png"):
            st.image("logo_empresa.png", use_container_width=True)
        else:
            st.markdown("<h2 style='text-align: center; color: #FF4B4B;'>🏗️ STOCK PRO</h2>", unsafe_allow_html=True)
        
        if db is not None:
            st.markdown("<div style='color: green; font-size: 0.85em; text-align: center;'>● Sistema Conectado</div>", unsafe_allow_html=True)
        else:
            st.markdown("<div style='color: red; font-size: 0.85em; text-align: center;'>● Sistema Desconectado</div>", unsafe_allow_html=True)
        st.divider()

    if db is None:
        st.error("🔴 FALHA CRÍTICA DE CONEXÃO")
        st.code(erro_conexao)
        return

    users = carregar_users()
    if "logado" not in st.session_state: st.session_state.logado = False
    
    # --- TELA DE LOGIN ---
    if not st.session_state.logado:
        st.markdown("<h1 style='text-align: center;'>🏗️ Gestão de Stock</h1>", unsafe_allow_html=True)
        col_a, col_b, col_c = st.columns([1, 5, 1])
        with col_b:
            st.markdown("<div class='login-card'>", unsafe_allow_html=True)
            u_in = st.text_input("Nome de Utilizador").lower().strip()
            p_in = st.text_input("Palavra-passe", type="password").strip()
            if st.button("ENTRAR NO SISTEMA"):
                if u_in in users and users[u_in]["password"] == p_in:
                    st.session_state.logado = True
                    st.session_state.user = users[u_in]
                    st.rerun()
                else: st.error("Utilizador ou senha incorretos.")
            st.markdown("</div>", unsafe_allow_html=True)
            with st.expander("❓ Ajuda no Acesso"):
                st.write("Verifique maiúsculas/minúsculas e espaços extras.")
                if st.button("🔄 LIMPAR CACHE"):
                    st.cache_data.clear()
                    st.rerun()
        return

    # --- MENU NAVEGAÇÃO ---
    nav = ["📊 Dashboard", "🔄 Movimentações", "👤 Minha Conta"]
    if st.session_state.user['nivel'] == "Admin": 
        nav += ["📂 Base Mestra", "👥 Gestão de Acessos"]
    
    menu = st.sidebar.radio("Menu Principal", nav)
    st.sidebar.markdown(f"**Logado como:** {st.session_state.user['username']}")
    
    if st.sidebar.button("Terminar Sessão"):
        st.session_state.logado = False
        st.rerun()

    # --- TELA: DASHBOARD ---
    if menu == "📊 Dashboard":
        st.title("📊 Saldo Atual de Estoque")
        df = calcular_saldos()
        if df.empty:
            st.info("💡 Catálogo vazio. Administrador: carregue a Base Mestra.")
        else:
            # Filtros Expandidos
            st.sidebar.markdown("### 🔍 Filtros Detalhados")
            f_mat = st.sidebar.multiselect("Material", sorted(df["Material"].unique()))
            f_obra = st.sidebar.multiselect("Obra", sorted(df["Obra"].unique()))
            f_pep = st.sidebar.multiselect("Elemento PEP", sorted(df["ElementoPEP"].unique()))
            f_grau = st.sidebar.multiselect("Grau", sorted(df["Grau"].unique()))
            f_esp = st.sidebar.multiselect("Espessura", sorted(df["Esp"].unique()))
            f_larg = st.sidebar.multiselect("Largura", sorted(df["Larg"].unique()))
            f_comp = st.sidebar.multiselect("Comprimento", sorted(df["Comp"].unique()))
            f_lvm = st.sidebar.text_input("Busca por LVM").upper().strip()

            df_v = df.copy()
            if f_mat: df_v = df_v[df_v["Material"].isin(f_mat)]
            if f_obra: df_v = df_v[df_v["Obra"].isin(f_obra)]
            if f_pep: df_v = df_v[df_v["ElementoPEP"].isin(f_pep)]
            if f_grau: df_v = df_v[df_v["Grau"].isin(f_grau)]
            if f_esp: df_v = df_v[df_v["Esp"].isin(f_esp)]
            if f_larg: df_v = df_v[df_v["Larg"].isin(f_larg)]
            if f_comp: df_v = df_v[df_v["Comp"].isin(f_comp)]
            if f_lvm: df_v = df_v[df_v["LVM"].str.contains(f_lvm)]

            c1, c2, c3 = st.columns(3)
            c1.metric("Peças Totais", f"{int(df_v['Saldo_Pecas'].sum()):,}")
            c2.metric("Total KG", f"{df_v['Saldo_KG'].sum():,.2f}")
            c3.metric("LVMs Ativas", len(df_v["LVM"].unique()))
            
            st.divider()
            
            # --- SEÇÃO DE GRÁFICOS ---
            g_col1, g_col2 = st.columns(2)
            with g_col1:
                # Gráfico de Peças por Obra
                df_pie = df_v.groupby("Obra")["Saldo_Pecas"].sum().reset_index().nlargest(10, "Saldo_Pecas")
                fig_pie = px.pie(df_pie, values="Saldo_Pecas", names="Obra", title="Top 10 Obras (Peças)", hole=0.3)
                st.plotly_chart(fig_pie, use_container_width=True)
            
            with g_col2:
                # Gráfico de Peso por Grau de Aço
                df_bar = df_v.groupby("Grau")["Saldo_KG"].sum().reset_index()
                fig_bar = px.bar(df_bar, x="Grau", y="Saldo_KG", title="Peso por Grau de Aço", color="Grau", 
                                 labels={'Saldo_KG': 'Peso Total (KG)', 'Grau': 'Grau Material'})
                st.plotly_chart(fig_bar, use_container_width=True)
            
            st.divider()

            if st.button("📥 Gerar Mapa de Stock (PDF)"):
                pdf_data = gerar_pdf(df_v)
                st.download_button("💾 Baixar PDF", pdf_data, f"stock_{datetime.now().strftime('%d%m%Y')}.pdf", "application/pdf")
            
            st.dataframe(df_v, use_container_width=True, hide_index=True)

    # --- TELA: MOVIMENTAÇÕES (COM ABAS) ---
    elif menu == "🔄 Movimentações":
        st.title("🔄 Registro de Movimentos")
        base = carregar_base_mestra()
        if base.empty: st.error("Catálogo vazio. Sincronize a Base Mestra primeiro."); return
        
        tab_ind, tab_lote = st.tabs(["📝 Lançamento Individual", "📁 Importação em Lote (Excel)"])
        
        with tab_ind:
            with st.form("form_ind"):
                col1, col2 = st.columns(2)
                tipo = col1.selectbox("Tipo de Operação", ["SAIDA", "ENTRADA", "TMA", "TDMA"])
                mat = col2.selectbox("Código Material", sorted(base["Material"].unique()))
                lvm = st.text_input("LVM").upper().strip()
                qtd = st.number_input("Quantidade (Peças)", min_value=1, step=1)
                obr = st.text_input("Obra").upper().strip()
                pep = st.text_input("Elemento PEP").upper().strip()
                
                if st.form_submit_button("GRAVAR REGISTRO"):
                    if (tipo in ["SAIDA", "TMA"]) and (not lvm or not obr):
                        st.error("Para SAÍDA e TMA, LVM e Obra são obrigatórios.")
                    else:
                        coll = get_coll("movements")
                        dt = datetime.now().strftime("%d/%m/%Y")
                        coll.add({
                            "Tipo": tipo, "Material": mat, "LVM": lvm, "Qtde": qtd, 
                            "Obra": obr, "ElementoPEP": pep, "Data": dt, 
                            "timestamp": firestore.SERVER_TIMESTAMP
                        })
                        st.success("Salvo com sucesso!"); time.sleep(1); st.rerun()

        with tab_lote:
            st.subheader("📁 Upload de Arquivos de Movimentação")
            st.info("O Excel deve conter as colunas: Material, LVM, Qtde, Obra, ElementoPEP, Data")
            tipo_up = st.selectbox("Escolha o tipo para o ficheiro", ["SAIDA", "ENTRADA", "TMA", "TDMA"])
            up_mov = st.file_uploader(f"Selecione o ficheiro de {tipo_up}", type="xlsx")
            
            if up_mov and st.button(f"🚀 Importar Lote de {tipo_up}"):
                try:
                    df_up = pd.read_excel(up_mov, dtype=str)
                    coll = get_coll("movements")
                    ts = firestore.SERVER_TIMESTAMP
                    prog = st.progress(0)
                    for i, r in df_up.iterrows():
                        d = r.to_dict()
                        d["Tipo"] = tipo_up
                        d["timestamp"] = ts
                        coll.add(d)
                        prog.progress((i + 1) / len(df_up))
                    st.success(f"Importação de {len(df_up)} registros de {tipo_up} concluída!")
                    time.sleep(1); st.rerun()
                except Exception as e:
                    st.error(f"Erro ao processar ficheiro: {e}")

    # --- TELA: MINHA CONTA ---
    elif menu == "👤 Minha Conta":
        st.title("👤 Configurações de Senha")
        with st.form("f_p"):
            nova = st.text_input("Definir Nova Palavra-passe", type="password")
            if st.form_submit_button("Atualizar"):
                if len(nova) >= 4:
                    ref = get_coll("users").where("username", "==", st.session_state.user['username']).stream()
                    for d in ref: d.reference.update({"password": nova})
                    st.success("Senha atualizada!")
                else: st.error("A senha deve ter pelo menos 4 caracteres.")

    # --- TELA: GESTÃO DE ACESSOS ---
    elif menu == "👥 Gestão de Acessos":
        st.title("👥 Gerir Equipa")
        with st.form("f_u"):
            nu = st.text_input("Utilizador (nome.sobrenome)").lower().strip()
            np = st.text_input("Senha", type="password")
            nv = st.selectbox("Nível de Permissão", ["Operador", "Admin"])
            if st.form_submit_button("Cadastrar Utilizador"):
                get_coll("users").add({"username": nu, "password": np, "nivel": nv})
                st.success("Utilizador criado!"); st.rerun()
        st.divider()
        for n, d in users.items():
            c1, c2 = st.columns([4, 1])
            c1.write(f"🏷️ **{n}** | {d['nivel']}")
            if n != "marcius.arruda" and c2.button("Remover", key=f"d_{n}"):
                docs = get_coll("users").where("username", "==", n).stream()
                for doc in docs: doc.reference.delete()
                st.rerun()

    # --- TELA: BASE MESTRA ---
    elif menu == "📂 Base Mestra":
        st.title("📂 Sincronizar Catálogo Principal")
        st.warning("Atenção: Carregar um novo catálogo substituirá o estoque inicial.")
        f = st.file_uploader("Ficheiro Excel da Base Mestra", type="xlsx")
        if f and st.button("🚀 SINCRONIZAR AGORA"):
            df_m = pd.read_excel(f, dtype=str)
            coll = get_coll("master_csv_store")
            # Limpa catálogo antigo
            for d in coll.stream(): d.reference.delete()
            # Fragmentação do CSV para o Firestore (evita limites de tamanho)
            csv_t = df_m.to_csv(index=False)
            size = 800000
            parts = [csv_t[i:i+size] for i in range(0, len(csv_t), size)]
            for i, p in enumerate(parts):
                coll.document(f"p_{i}").set({"part": i, "csv_data": p})
            st.cache_data.clear()
            st.success("Base Mestra Sincronizada com Sucesso!")
            st.balloons()

if __name__ == "__main__":
    main()