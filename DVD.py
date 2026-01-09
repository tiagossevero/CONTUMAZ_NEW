# =============================================================================
# SISTEMA DE MONITORAMENTO DE DEVEDORES CONTUMAZ - DVD CONT V1.0
# Secretaria de Estado da Fazenda de Santa Catarina - SEF/SC
# =============================================================================

import streamlit as st
import hashlib

SENHA = "contumaz2025"

def check_password():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if not st.session_state.authenticated:
        st.markdown("<div style='text-align: center; padding: 50px;'><h1>🔐 Acesso Restrito</h1><p>Sistema de Devedores Contumaz</p></div>", unsafe_allow_html=True)
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            senha_input = st.text_input("Digite a senha:", type="password", key="pwd_input")
            if st.button("Entrar", use_container_width=True):
                if senha_input == SENHA:
                    st.session_state.authenticated = True
                    st.rerun()
                else:
                    st.error("❌ Senha incorreta")
        st.stop()

check_password()

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from sqlalchemy import create_engine
import warnings
import ssl

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

warnings.filterwarnings('ignore')

st.set_page_config(
    page_title="Devedores Contumaz - SEF/SC", 
    page_icon="⚖️", 
    layout="wide", 
    initial_sidebar_state="collapsed"
)

# =============================================================================
# CSS CUSTOMIZADO
# =============================================================================
st.markdown("""
<style>
    /* =========================================================================
       SIDEBAR SEMPRE COLAPSADO - EXPANDE AO PASSAR O MOUSE
       ========================================================================= */
    /* Sidebar sempre colapsado por padrão */
    section[data-testid="stSidebar"] {
        width: 0px !important;
        min-width: 0px !important;
        transform: translateX(-100%);
        transition: transform 0.3s ease-in-out, width 0.3s ease-in-out;
    }
    section[data-testid="stSidebar"]:hover,
    section[data-testid="stSidebar"]:focus-within {
        width: 300px !important;
        min-width: 300px !important;
        transform: translateX(0);
    }
    /* Indicador visual para expandir (hambúrguer) */
    section[data-testid="stSidebar"]::before {
        content: "☰";
        position: absolute;
        right: -30px;
        top: 50%;
        transform: translateY(-50%);
        font-size: 24px;
        color: #1565C0;
        cursor: pointer;
        z-index: 1000;
    }
    
    /* =========================================================================
       ESTILOS GERAIS
       ========================================================================= */
    .main-header {
        font-size: 2.2rem; 
        font-weight: bold; 
        text-align: center; 
        margin-bottom: 1.5rem; 
        padding: 18px; 
        background: linear-gradient(135deg, #1565C0 0%, #0D47A1 100%); 
        border-radius: 12px; 
        color: white;
        box-shadow: 0 4px 6px rgba(0,0,0,0.15);
    }
    .sub-header {
        font-size: 1.3rem; 
        font-weight: bold; 
        color: #1565C0; 
        margin-top: 1.5rem; 
        margin-bottom: 1rem; 
        border-bottom: 2px solid #1565C0; 
        padding-bottom: 8px;
    }
    
    /* Cards de métricas */
    div[data-testid="stMetric"] {
        background-color: #ffffff; 
        border: 1px solid #e0e0e0; 
        border-radius: 10px; 
        padding: 12px; 
        box-shadow: 0 2px 4px rgba(0,0,0,0.08);
    }
    
    /* Gráficos */
    div[data-testid="stPlotlyChart"] {
        border: 1px solid #e0e0e0; 
        border-radius: 10px; 
        padding: 10px; 
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    
    /* Alertas customizados */
    .alert-critico {
        background-color: #fef2f2; 
        border-left: 5px solid #dc2626; 
        padding: 14px; 
        border-radius: 8px; 
        margin: 12px 0;
    }
    .alert-atencao {
        background-color: #fffbeb; 
        border-left: 5px solid #f59e0b; 
        padding: 14px; 
        border-radius: 8px; 
        margin: 12px 0;
    }
    .alert-positivo {
        background-color: #f0fdf4; 
        border-left: 5px solid #22c55e; 
        padding: 14px; 
        border-radius: 8px; 
        margin: 12px 0;
    }
    .info-box {
        background-color: #eff6ff; 
        border-left: 5px solid #3b82f6; 
        padding: 14px; 
        border-radius: 8px; 
        margin: 12px 0;
    }
    
    /* KPI Cards coloridos */
    .kpi-card {
        padding: 1rem;
        border-radius: 12px;
        color: white;
        box-shadow: 0 4px 6px rgba(0,0,0,0.12);
        margin: 5px 0;
        text-align: center;
    }
    .kpi-enquadrado { background: linear-gradient(135deg, #dc2626 0%, #b91c1c 100%); }
    .kpi-suspenso { background: linear-gradient(135deg, #f59e0b 0%, #d97706 100%); }
    .kpi-intimacao { background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%); }
    .kpi-total { background: linear-gradient(135deg, #6366f1 0%, #4f46e5 100%); }
    .kpi-valor { background: linear-gradient(135deg, #10b981 0%, #059669 100%); }
</style>
""", unsafe_allow_html=True)

# =============================================================================
# CONEXÃO COM IMPALA
# =============================================================================
IMPALA_HOST = 'bdaworkernode02.sef.sc.gov.br'
IMPALA_PORT = 21050
DATABASE = 'gecob'
IMPALA_USER = st.secrets.get("impala_credentials", {}).get("user", "tsevero")
IMPALA_PASSWORD = st.secrets.get("impala_credentials", {}).get("password", "")

@st.cache_resource
def get_impala_engine():
    try:
        engine = create_engine(
            f'impala://{IMPALA_HOST}:{IMPALA_PORT}/{DATABASE}',
            connect_args={
                'user': IMPALA_USER, 
                'password': IMPALA_PASSWORD, 
                'auth_mechanism': 'LDAP', 
                'use_ssl': True
            }
        )
        return engine
    except Exception as e:
        st.sidebar.error(f"❌ Erro conexão: {str(e)[:100]}")
        return None

# =============================================================================
# CARREGAMENTO DE DADOS
# =============================================================================
@st.cache_data(ttl=3600)
def carregar_dados_resumo(_engine):
    """Carrega apenas dados resumidos para performance inicial"""
    dados = {}
    if _engine is None:
        return {}
    
    try:
        with _engine.connect() as conn:
            pass
    except Exception as e:
        st.sidebar.error(f"❌ Falha conexão: {str(e)[:100]}")
        return {}
    
    # Tabelas resumidas (carregamento rápido)
    tabelas_resumo = {
        'resumo_executivo': f"SELECT * FROM {DATABASE}.dvd_cont_resumo_executivo",
        'panorama_valores': f"SELECT * FROM {DATABASE}.dvd_cont_panorama_valores",
        'metricas_gerfe': f"SELECT * FROM {DATABASE}.dvd_cont_metricas_gerfe_detalhada",
        'serie_temporal': f"SELECT * FROM {DATABASE}.dvd_cont_serie_temporal",
        'enquadrados_gerfe': f"SELECT * FROM {DATABASE}.dvd_cont_enquadrados_por_gerfe",
        'suspensos_gerfe': f"SELECT * FROM {DATABASE}.dvd_cont_suspensos_por_gerfe",
        'valores_parcelados': f"SELECT * FROM {DATABASE}.dvd_cont_valores_parcelados_gerfe",
        'alertas_kpis': f"SELECT * FROM {DATABASE}.dvd_cont_alertas_kpis",
        'alertas_resumo': f"SELECT * FROM {DATABASE}.dvd_cont_alertas_resumo",
    }
    
    progress_bar = st.sidebar.progress(0)
    total = len(tabelas_resumo)
    
    for idx, (key, query) in enumerate(tabelas_resumo.items()):
        try:
            progress_bar.progress((idx + 1) / total)
            df = pd.read_sql(query, _engine)
            df.columns = [col.lower() for col in df.columns]
            for col in df.select_dtypes(include=['object']).columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='ignore')
                except:
                    pass
            dados[key] = df
        except Exception as e:
            dados[key] = pd.DataFrame()
    
    progress_bar.empty()
    return dados

@st.cache_data(ttl=1800)
def carregar_situacao_atual(_engine):
    """Carrega tabela de situação atual sob demanda"""
    if _engine is None:
        return pd.DataFrame()
    try:
        query = f"SELECT * FROM {DATABASE}.dvd_cont_situacao_atual ORDER BY saldo_total_dividas DESC"
        df = pd.read_sql(query, _engine)
        df.columns = [col.lower() for col in df.columns]
        return df
    except Exception as e:
        return pd.DataFrame()

@st.cache_data(ttl=1800)
def carregar_processos_encerrados(_engine):
    """Carrega processos encerrados sob demanda"""
    if _engine is None:
        return pd.DataFrame()
    try:
        query = f"SELECT * FROM {DATABASE}.dvd_cont_processos_encerrados ORDER BY data_atualizacao_situacao DESC"
        df = pd.read_sql(query, _engine)
        df.columns = [col.lower() for col in df.columns]
        return df
    except Exception as e:
        return pd.DataFrame()

@st.cache_data(ttl=1800)
def carregar_parcelamentos(_engine):
    """Carrega parcelamentos sob demanda"""
    if _engine is None:
        return pd.DataFrame()
    try:
        query = f"SELECT * FROM {DATABASE}.dvd_cont_parcelamentos ORDER BY dt_pedido DESC"
        df = pd.read_sql(query, _engine)
        df.columns = [col.lower() for col in df.columns]
        return df
    except Exception as e:
        return pd.DataFrame()

@st.cache_data(ttl=1800)
def carregar_alertas_detalhados(_engine):
    """Carrega todos os alertas detalhados sob demanda"""
    if _engine is None:
        return pd.DataFrame()
    try:
        query = f"""
        SELECT * FROM {DATABASE}.dvd_cont_alertas 
        ORDER BY 
            CASE nivel_criticidade 
                WHEN 'URGENTE' THEN 1 
                WHEN 'CRÍTICO' THEN 2 
                WHEN 'ATENÇÃO' THEN 3 
                ELSE 4 
            END,
            saldo_total_dividas DESC
        """
        df = pd.read_sql(query, _engine)
        df.columns = [col.lower() for col in df.columns]
        return df
    except Exception as e:
        return pd.DataFrame()

@st.cache_data(ttl=1800)
def carregar_debitos_parcelados(_engine):
    """Carrega débitos enquadrados com seus parcelamentos"""
    if _engine is None:
        return pd.DataFrame()
    try:
        query = f"SELECT * FROM {DATABASE}.dvd_cont_debitos_parcelados ORDER BY saldo_total_dividas DESC"
        df = pd.read_sql(query, _engine)
        df.columns = [col.lower() for col in df.columns]
        return df
    except Exception as e:
        return pd.DataFrame()

@st.cache_data(ttl=1800)
def carregar_comunicacoes(_engine):
    """Carrega comunicações sob demanda"""
    if _engine is None:
        return pd.DataFrame()
    try:
        query = f"SELECT * FROM {DATABASE}.dvd_cont_comunicacoes_enviadas ORDER BY data_envio DESC"
        df = pd.read_sql(query, _engine)
        df.columns = [col.lower() for col in df.columns]
        return df
    except Exception as e:
        return pd.DataFrame()

@st.cache_data(ttl=1800)
def carregar_historico(_engine):
    """Carrega histórico completo sob demanda"""
    if _engine is None:
        return pd.DataFrame()
    try:
        query = f"SELECT * FROM {DATABASE}.dvd_cont_historico_completo ORDER BY data_atualizacao_situacao DESC"
        df = pd.read_sql(query, _engine)
        df.columns = [col.lower() for col in df.columns]
        return df
    except Exception as e:
        return pd.DataFrame()

@st.cache_data(ttl=1800)
def carregar_extratos_enquadramentos(_engine):
    """Carrega extratos de enquadramentos sob demanda"""
    if _engine is None:
        return pd.DataFrame()
    try:
        query = f"SELECT * FROM {DATABASE}.dvd_cont_extratos_enquadramentos ORDER BY inicio_efeitos DESC"
        df = pd.read_sql(query, _engine)
        df.columns = [col.lower() for col in df.columns]
        return df
    except Exception as e:
        return pd.DataFrame()

@st.cache_data(ttl=600)
def carregar_detalhes_empresa(_engine, cnpj_raiz):
    """Carrega todos os detalhes de uma empresa específica para drill down"""
    if _engine is None or not cnpj_raiz:
        return {}
    
    detalhes = {}
    cnpj_raiz_clean = str(cnpj_raiz).strip().replace('.', '').replace('/', '').replace('-', '')
    
    try:
        # 1. Dados da situação atual
        query_situacao = f"""
        SELECT * FROM {DATABASE}.dvd_cont_situacao_atual 
        WHERE REGEXP_REPLACE(cnpj_raiz, '[^0-9]', '') = '{cnpj_raiz_clean}'
           OR REGEXP_REPLACE(cnpj, '[^0-9]', '') LIKE '{cnpj_raiz_clean}%'
        """
        detalhes['situacao'] = pd.read_sql(query_situacao, _engine)
        
        # 2. Débitos e parcelamentos
        query_debitos = f"""
        SELECT * FROM {DATABASE}.dvd_cont_debitos_parcelados 
        WHERE CAST(cnpj_raiz AS STRING) = '{cnpj_raiz_clean}'
        ORDER BY saldo_debito DESC
        """
        detalhes['debitos'] = pd.read_sql(query_debitos, _engine)
        
        # 3. Parcelamentos gerais
        query_parcelamentos = f"""
        SELECT * FROM {DATABASE}.dvd_cont_parcelamentos 
        WHERE REGEXP_REPLACE(cnpj_raiz, '[^0-9]', '') = '{cnpj_raiz_clean}'
        ORDER BY dt_pedido DESC
        """
        detalhes['parcelamentos'] = pd.read_sql(query_parcelamentos, _engine)
        
        # 4. Comunicações enviadas
        query_comunicacoes = f"""
        SELECT * FROM {DATABASE}.dvd_cont_comunicacoes_enviadas 
        WHERE REGEXP_REPLACE(cnpj, '[^0-9]', '') LIKE '{cnpj_raiz_clean}%'
        ORDER BY data_envio DESC
        """
        detalhes['comunicacoes'] = pd.read_sql(query_comunicacoes, _engine)
        
        # 5. Histórico completo
        query_historico = f"""
        SELECT * FROM {DATABASE}.dvd_cont_historico_completo 
        WHERE REGEXP_REPLACE(cnpj_raiz, '[^0-9]', '') = '{cnpj_raiz_clean}'
        ORDER BY data_atualizacao_situacao DESC
        """
        detalhes['historico'] = pd.read_sql(query_historico, _engine)
        
        # 6. Extratos de enquadramento
        query_extratos = f"""
        SELECT * FROM {DATABASE}.dvd_cont_extratos_enquadramentos 
        WHERE REGEXP_REPLACE(cnpj_raiz, '[^0-9]', '') = '{cnpj_raiz_clean}'
        """
        detalhes['extratos_enq'] = pd.read_sql(query_extratos, _engine)
        
        # 7. Extratos de desenquadramento
        query_desenq = f"""
        SELECT * FROM {DATABASE}.dvd_cont_extratos_desenquadramentos 
        WHERE REGEXP_REPLACE(cnpj_raiz, '[^0-9]', '') = '{cnpj_raiz_clean}'
        """
        detalhes['extratos_desenq'] = pd.read_sql(query_desenq, _engine)
        
        # 8. Alertas da empresa
        query_alertas = f"""
        SELECT * FROM {DATABASE}.dvd_cont_alertas 
        WHERE CAST(cnpj_raiz AS STRING) = '{cnpj_raiz_clean}'
        ORDER BY 
            CASE nivel_criticidade 
                WHEN 'URGENTE' THEN 1 
                WHEN 'CRÍTICO' THEN 2 
                WHEN 'ATENÇÃO' THEN 3 
                ELSE 4 
            END
        """
        detalhes['alertas'] = pd.read_sql(query_alertas, _engine)
        
        # Normalizar colunas
        for key in detalhes:
            if not detalhes[key].empty:
                detalhes[key].columns = [col.lower() for col in detalhes[key].columns]
        
    except Exception as e:
        st.error(f"Erro ao carregar detalhes: {str(e)}")
    
    return detalhes

# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

def formatar_valor_br(valor):
    """Formata valores monetários para padrão brasileiro"""
    if pd.isna(valor) or valor is None:
        return "R$ 0"
    valor = float(valor)
    if valor >= 1e9:
        return f"R$ {valor/1e9:.2f} Bi"
    elif valor >= 1e6:
        return f"R$ {valor/1e6:.2f} Mi"
    elif valor >= 1e3:
        return f"R$ {valor/1e3:.1f} mil"
    else:
        return f"R$ {valor:,.2f}"

def formatar_numero(valor):
    """Formata números com separadores de milhar"""
    if pd.isna(valor) or valor is None:
        return "0"
    return f"{int(valor):,}".replace(",", ".")

def get_cor_situacao(situacao):
    """Retorna cor baseada na situação"""
    cores = {
        30: '#dc2626',  # Enquadrado - Vermelho
        31: '#f59e0b',  # Suspenso - Laranja
        32: '#eab308',  # Efeito suspenso - Amarelo
        10: '#3b82f6',  # A intimar - Azul
        11: '#60a5fa',  # Intimado - Azul claro
        12: '#93c5fd',  # Intimado 30 dias - Azul mais claro
    }
    return cores.get(situacao, '#6b7280')

def criar_kpi_card(titulo, valor, subtitulo="", classe="kpi-total", icone="📊"):
    """Cria um card de KPI estilizado"""
    st.markdown(f"""
    <div class='kpi-card {classe}'>
        <div style='font-size: 1.8rem;'>{icone}</div>
        <div style='font-size: 0.85rem; opacity: 0.9;'>{titulo}</div>
        <div style='font-size: 1.9rem; font-weight: bold;'>{valor}</div>
        <div style='font-size: 0.75rem; opacity: 0.8;'>{subtitulo}</div>
    </div>
    """, unsafe_allow_html=True)

def calcular_kpis_gerais(dados):
    """Calcula KPIs gerais a partir do resumo executivo"""
    df = dados.get('resumo_executivo', pd.DataFrame())
    if df.empty:
        return {k: 0 for k in [
            'total_processos', 'qtd_enquadrados', 'qtd_suspensos', 'qtd_intimacao',
            'vl_total', 'vl_enquadrados', 'vl_suspensos', 'qtd_encerrados',
            'qtd_parcelamento_ativo', 'vl_parcelado'
        ]}
    
    row = df.iloc[0]
    return {
        'total_processos': int(row.get('total_processos_instaurados', 0) or 0),
        'qtd_enquadrados': int(row.get('qtd_enquadrados_ativos', 0) or 0),
        'qtd_suspensos': int(row.get('qtd_suspensos', 0) or 0),
        'qtd_intimacao': int(row.get('qtd_em_intimacao', 0) or 0),
        'qtd_processo_suspenso': int(row.get('qtd_processo_suspenso', 0) or 0),
        'qtd_efeito_suspenso': int(row.get('qtd_efeito_suspenso', 0) or 0),
        'qtd_encerrados': int(row.get('qtd_processos_encerrados', 0) or 0),
        'vl_total': float(row.get('vl_total_debitos', 0) or 0),
        'vl_enquadrados': float(row.get('vl_enquadrados_ativos', 0) or 0),
        'vl_suspensos': float(row.get('vl_suspensos', 0) or 0),
        'vl_declarados': float(row.get('vl_debitos_declarados', 0) or 0),
        'vl_divida_ativa': float(row.get('vl_divida_ativa', 0) or 0),
        'vl_parcelado': float(row.get('vl_parcelado_ativo', 0) or 0),
        'qtd_parcelamento_ativo': int(row.get('qtd_com_parcelamento_ativo', 0) or 0),
        'qtd_alertas': int(row.get('qtd_alertas_parcelamento', 0) or 0),
        'taxa_regularizacao': float(row.get('taxa_regularizacao_pct', 0) or 0),
    }

# =============================================================================
# PÁGINA: DASHBOARD EXECUTIVO
# =============================================================================

def pagina_dashboard_executivo(dados, engine):
    st.markdown("<h1 class='main-header'>📊 Dashboard Executivo - Devedores Contumaz</h1>", unsafe_allow_html=True)
    
    kpis = calcular_kpis_gerais(dados)
    
    # Linha 1: KPIs principais em cards coloridos
    st.markdown("<div class='sub-header'>📈 Situação dos Processos Instaurados</div>", unsafe_allow_html=True)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        criar_kpi_card(
            "Total Processos", 
            formatar_numero(kpis['total_processos']),
            "Processos Instaurados",
            "kpi-total",
            "📋"
        )
    
    with col2:
        criar_kpi_card(
            "Enquadrados", 
            formatar_numero(kpis['qtd_enquadrados']),
            f"{kpis['qtd_enquadrados']/max(kpis['total_processos'],1)*100:.1f}%",
            "kpi-enquadrado",
            "🔴"
        )
    
    with col3:
        criar_kpi_card(
            "Suspensos", 
            formatar_numero(kpis['qtd_suspensos']),
            f"31: {kpis.get('qtd_processo_suspenso',0)} | 32: {kpis.get('qtd_efeito_suspenso',0)}",
            "kpi-suspenso",
            "⏸️"
        )
    
    with col4:
        criar_kpi_card(
            "Em Intimação", 
            formatar_numero(kpis['qtd_intimacao']),
            "A intimar + Intimados",
            "kpi-intimacao",
            "📨"
        )
    
    with col5:
        criar_kpi_card(
            "Valor Total", 
            formatar_valor_br(kpis['vl_total']),
            "Débitos + DVA",
            "kpi-valor",
            "💰"
        )
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Linha 2: Valores detalhados
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "💵 Valor Enquadrados", 
            formatar_valor_br(kpis['vl_enquadrados']),
            help="Valor total de débitos dos contribuintes enquadrados ativamente"
        )
    with col2:
        st.metric(
            "💵 Valor Suspensos", 
            formatar_valor_br(kpis['vl_suspensos']),
            help="Valor total de débitos dos contribuintes com processo suspenso"
        )
    with col3:
        st.metric(
            "📝 Débitos Declarados", 
            formatar_valor_br(kpis['vl_declarados']),
            help="Valor de impostos declarados (DIME)"
        )
    with col4:
        st.metric(
            "📑 Dívida Ativa", 
            formatar_valor_br(kpis['vl_divida_ativa']),
            help="Valor inscrito em Dívida Ativa"
        )
    
    st.divider()
    
    # Gráficos
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("<div class='sub-header'>📊 Distribuição por Situação GEP</div>", unsafe_allow_html=True)
        
        df_panorama = dados.get('panorama_valores', pd.DataFrame())
        if not df_panorama.empty:
            fig = px.pie(
                df_panorama, 
                values='quantidade', 
                names='situacao',
                color='situacao',
                color_discrete_map={
                    'Enquadrado': '#dc2626',
                    'Suspenso Parcelamento': '#f59e0b',
                    'Em processo de Intimação': '#3b82f6'
                },
                hole=0.4
            )
            fig.update_layout(
                height=350, 
                margin=dict(l=20, r=20, t=30, b=20),
                legend=dict(orientation="h", yanchor="bottom", y=-0.2)
            )
            fig.update_traces(
                textposition='inside', 
                textinfo='percent+value',
                hovertemplate='<b>%{label}</b><br>Quantidade: %{value}<br>Percentual: %{percent}<extra></extra>'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Dados não disponíveis")
    
    with col2:
        st.markdown("<div class='sub-header'>🏢 Top 10 GERFEs por Débito</div>", unsafe_allow_html=True)
        
        df_gerfe = dados.get('metricas_gerfe', pd.DataFrame())
        if not df_gerfe.empty and 'debito_total' in df_gerfe.columns:
            df_top = df_gerfe.nlargest(10, 'debito_total').copy()
            df_top['gerfe_label'] = df_top['cd_gerfe'].astype(str) + ' - ' + df_top['gerfe'].fillna('')
            
            fig = px.bar(
                df_top,
                x='gerfe_label',
                y='debito_total',
                color='qtd_enquadrado',
                color_continuous_scale='Reds',
                labels={'debito_total': 'Débito Total (R$)', 'gerfe_label': 'GERFE', 'qtd_enquadrado': 'Qtd Enquadrados'}
            )
            fig.update_layout(
                height=350,
                margin=dict(l=20, r=20, t=30, b=80),
                xaxis_tickangle=-45
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Dados não disponíveis")
    
    st.divider()
    
    # Tabela resumo por GERFE
    st.markdown("<div class='sub-header'>📋 Resumo por GERFE</div>", unsafe_allow_html=True)
    
    df_gerfe = dados.get('metricas_gerfe', pd.DataFrame())
    if not df_gerfe.empty:
        # Calcular totais
        df_gerfe_display = df_gerfe.copy()
        df_gerfe_display['pct_enquadrado'] = (df_gerfe_display['qtd_enquadrado'] / df_gerfe_display['total'] * 100).round(1)
        
        col_config = {
            'cd_gerfe': st.column_config.NumberColumn('Código', width='small'),
            'gerfe': st.column_config.TextColumn('GERFE', width='medium'),
            'total': st.column_config.NumberColumn('Total', format='%d'),
            'qtd_enquadrado': st.column_config.NumberColumn('Enquadrados', format='%d'),
            'qtd_suspenso': st.column_config.NumberColumn('Suspensos', format='%d'),
            'qtd_a_intimar': st.column_config.NumberColumn('A Intimar', format='%d'),
            'qtd_intimado': st.column_config.NumberColumn('Intimados', format='%d'),
            'qtd_intimado_30_dias': st.column_config.NumberColumn('Int. 30d', format='%d'),
            'pct_enquadrado': st.column_config.ProgressColumn(
                '% Enquadrado', 
                format='%.1f%%', 
                min_value=0, 
                max_value=100
            ),
            'debito_total': st.column_config.NumberColumn('Débito Total', format='R$ %.2f'),
        }
        
        cols = ['cd_gerfe', 'gerfe', 'total', 'qtd_enquadrado', 'qtd_suspenso', 
                'qtd_a_intimar', 'qtd_intimado', 'qtd_intimado_30_dias', 'pct_enquadrado', 'debito_total']
        cols_exist = [c for c in cols if c in df_gerfe_display.columns]
        
        st.dataframe(
            df_gerfe_display[cols_exist].sort_values('debito_total', ascending=False),
            column_config=col_config,
            use_container_width=True,
            hide_index=True,
            height=400
        )
    
    # Info box
    st.markdown("""
    <div class='info-box'>
    <b>📌 Legenda das Situações GEP:</b><br>
    • <b style='color:#dc2626'>30 - Enquadrado:</b> Contribuinte formalmente enquadrado como Devedor Contumaz<br>
    • <b style='color:#f59e0b'>31 - Processo Suspenso:</b> Suspenso ANTES do enquadramento (ex: parcelamento na intimação)<br>
    • <b style='color:#eab308'>32 - Efeito Suspenso:</b> Já enquadrado, mas EFEITOS suspensos (ex: parcelamento após enquadramento)<br>
    • <b style='color:#3b82f6'>10/11/12 - Em Intimação:</b> Em processo de intimação (A intimar / Intimado / +30 dias)
    </div>
    """, unsafe_allow_html=True)


# =============================================================================
# PÁGINA: PANORAMA DE VALORES
# =============================================================================

def pagina_panorama_valores(dados, engine):
    st.markdown("<h1 class='main-header'>💰 Panorama de Valores</h1>", unsafe_allow_html=True)
    
    kpis = calcular_kpis_gerais(dados)
    df_panorama = dados.get('panorama_valores', pd.DataFrame())
    
    # KPIs de valores
    st.markdown("<div class='sub-header'>📊 Valores por Situação</div>", unsafe_allow_html=True)
    
    if not df_panorama.empty:
        tabs = st.tabs(["📈 Visão Geral", "📊 Gráficos", "📋 Detalhamento"])
        
        with tabs[0]:
            col1, col2, col3 = st.columns(3)
            
            for idx, row in df_panorama.iterrows():
                with [col1, col2, col3][idx % 3]:
                    situacao = row.get('situacao', 'N/A')
                    qtd = int(row.get('quantidade', 0))
                    total = float(row.get('total_debitos', 0))
                    pct = float(row.get('percentual', 0))
                    
                    classe = 'kpi-enquadrado' if 'Enquadrado' in str(situacao) else \
                             'kpi-suspenso' if 'Suspenso' in str(situacao) else 'kpi-intimacao'
                    
                    criar_kpi_card(
                        situacao,
                        formatar_valor_br(total),
                        f"{qtd} processos | {pct:.1f}%",
                        classe,
                        "📊"
                    )
        
        with tabs[1]:
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.pie(
                    df_panorama,
                    values='total_debitos',
                    names='situacao',
                    title='Distribuição de Valores',
                    color_discrete_sequence=px.colors.sequential.RdBu
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = px.bar(
                    df_panorama,
                    x='situacao',
                    y=['debitos_declarados', 'divida_ativa'],
                    title='Composição dos Débitos',
                    barmode='stack',
                    labels={'value': 'Valor (R$)', 'situacao': 'Situação'}
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
        
        with tabs[2]:
            st.dataframe(
                df_panorama,
                column_config={
                    'situacao': st.column_config.TextColumn('Situação'),
                    'quantidade': st.column_config.NumberColumn('Quantidade', format='%d'),
                    'debitos_declarados': st.column_config.NumberColumn('Déb. Declarados', format='R$ %.2f'),
                    'divida_ativa': st.column_config.NumberColumn('Dívida Ativa', format='R$ %.2f'),
                    'total_debitos': st.column_config.NumberColumn('Total', format='R$ %.2f'),
                    'percentual': st.column_config.ProgressColumn('Percentual', format='%.1f%%', min_value=0, max_value=100)
                },
                use_container_width=True,
                hide_index=True
            )
    
    # Valores parcelados por GERFE
    st.divider()
    st.markdown("<div class='sub-header'>📑 Valores Parcelados por GERFE</div>", unsafe_allow_html=True)
    
    df_parc_gerfe = dados.get('valores_parcelados', pd.DataFrame())
    if not df_parc_gerfe.empty:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            fig = px.bar(
                df_parc_gerfe.sort_values('valor_total', ascending=True).tail(15),
                y='gerfe',
                x='valor_total',
                orientation='h',
                color='valor_processo_ativo',
                color_continuous_scale='Greens',
                labels={'valor_total': 'Valor Total Parcelado', 'gerfe': 'GERFE'}
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            total_parcelado = df_parc_gerfe['valor_total'].sum()
            total_ativo = df_parc_gerfe['valor_processo_ativo'].sum()
            
            st.metric("💰 Total Parcelado", formatar_valor_br(total_parcelado))
            st.metric("✅ Em Processos Ativos", formatar_valor_br(total_ativo))
            st.metric("📊 Qtd GERFEs", len(df_parc_gerfe))


# =============================================================================
# PÁGINA: ANÁLISE POR GERFE
# =============================================================================

def pagina_analise_gerfe(dados, engine):
    st.markdown("<h1 class='main-header'>🏢 Análise por GERFE</h1>", unsafe_allow_html=True)
    
    df_gerfe = dados.get('metricas_gerfe', pd.DataFrame())
    df_enquadrados = dados.get('enquadrados_gerfe', pd.DataFrame())
    df_suspensos = dados.get('suspensos_gerfe', pd.DataFrame())
    
    if df_gerfe.empty:
        st.warning("Dados de GERFE não disponíveis")
        return
    
    # Seletor de GERFE
    gerfe_opcoes = ['Todas'] + sorted(df_gerfe['gerfe'].dropna().unique().tolist())
    gerfe_sel = st.selectbox("🔍 Filtrar por GERFE:", gerfe_opcoes)
    
    if gerfe_sel != 'Todas':
        df_gerfe = df_gerfe[df_gerfe['gerfe'] == gerfe_sel]
        if not df_enquadrados.empty:
            df_enquadrados = df_enquadrados[df_enquadrados['gerfe'] == gerfe_sel]
        if not df_suspensos.empty:
            df_suspensos = df_suspensos[df_suspensos['gerfe'] == gerfe_sel]
    
    # Tabs
    tab1, tab2, tab3 = st.tabs(["📊 Visão Geral", "🔴 Enquadrados", "⏸️ Suspensos"])
    
    with tab1:
        # Métricas gerais
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Processos", formatar_numero(df_gerfe['total'].sum()))
        with col2:
            st.metric("Enquadrados", formatar_numero(df_gerfe['qtd_enquadrado'].sum()))
        with col3:
            st.metric("Suspensos", formatar_numero(df_gerfe['qtd_suspenso'].sum()))
        with col4:
            st.metric("Débito Total", formatar_valor_br(df_gerfe['debito_total'].sum()))
        
        st.divider()
        
        # Gráfico comparativo
        if len(df_gerfe) > 1:
            fig = px.bar(
                df_gerfe.sort_values('total', ascending=False),
                x='gerfe',
                y=['qtd_enquadrado', 'qtd_suspenso', 'qtd_a_intimar', 'qtd_intimado', 'qtd_intimado_30_dias'],
                title='Distribuição por Situação',
                barmode='stack',
                color_discrete_map={
                    'qtd_enquadrado': '#dc2626',
                    'qtd_suspenso': '#f59e0b',
                    'qtd_a_intimar': '#3b82f6',
                    'qtd_intimado': '#60a5fa',
                    'qtd_intimado_30_dias': '#93c5fd'
                }
            )
            fig.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.markdown("<div class='sub-header'>🔴 Enquadrados por GERFE - Medidas Aplicadas</div>", unsafe_allow_html=True)
        
        if not df_enquadrados.empty:
            col_config = {
                'gerfe': st.column_config.TextColumn('GERFE'),
                'apuracao_por_operacao_total': st.column_config.NumberColumn('Apuração Operação', format='%d'),
                'ref_total': st.column_config.NumberColumn('REF', format='%d'),
                'nenhuma_medida': st.column_config.NumberColumn('Sem Medida', format='%d'),
                'total': st.column_config.NumberColumn('Total', format='%d'),
            }
            
            st.dataframe(
                df_enquadrados,
                column_config=col_config,
                use_container_width=True,
                hide_index=True
            )
            
            # Gráfico de medidas
            if 'apuracao_por_operacao_total' in df_enquadrados.columns:
                medidas_total = pd.DataFrame({
                    'Medida': ['Apuração por Operação', 'REF', 'Sem Medida'],
                    'Quantidade': [
                        df_enquadrados['apuracao_por_operacao_total'].sum(),
                        df_enquadrados['ref_total'].sum(),
                        df_enquadrados['nenhuma_medida'].sum()
                    ]
                })
                
                fig = px.pie(medidas_total, values='Quantidade', names='Medida', 
                            title='Distribuição de Medidas Aplicadas',
                            color_discrete_sequence=['#dc2626', '#f59e0b', '#6b7280'])
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.markdown("<div class='sub-header'>⏸️ Suspensos por GERFE</div>", unsafe_allow_html=True)
        
        if not df_suspensos.empty:
            col_config = {
                'gerfe': st.column_config.TextColumn('GERFE'),
                'com_enq_total': st.column_config.NumberColumn('Com Enquadramento (32)', format='%d', 
                    help='Já enquadrado, mas com efeitos suspensos'),
                'sem_enq_total': st.column_config.NumberColumn('Sem Enquadramento (31)', format='%d',
                    help='Processo suspenso antes do enquadramento'),
                'total': st.column_config.NumberColumn('Total', format='%d'),
            }
            
            st.dataframe(
                df_suspensos,
                column_config=col_config,
                use_container_width=True,
                hide_index=True
            )
            
            st.markdown("""
            <div class='info-box'>
            <b>📌 Diferença entre 31 e 32:</b><br>
            • <b>31 - Sem Enquadramento:</b> Processo suspenso ANTES do enquadramento formal (ex: parcelou durante intimação)<br>
            • <b>32 - Com Enquadramento:</b> Contribuinte já foi enquadrado, mas os EFEITOS estão suspensos (ex: parcelou após enquadramento)
            </div>
            """, unsafe_allow_html=True)


# =============================================================================
# PÁGINA: SITUAÇÃO ATUAL (DRILL-DOWN)
# =============================================================================

def pagina_situacao_atual(dados, engine):
    st.markdown("<h1 class='main-header'>📋 Situação Atual - Processos Instaurados</h1>", unsafe_allow_html=True)
    
    # Carregar dados sob demanda
    with st.spinner("Carregando dados detalhados..."):
        df = carregar_situacao_atual(engine)
    
    if df.empty:
        st.warning("Dados não disponíveis")
        return
    
    # Filtros
    st.markdown("<div class='sub-header'>🔍 Filtros</div>", unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        situacoes = ['Todas'] + sorted(df['situacao_atual_no_gep'].dropna().unique().tolist())
        sit_sel = st.selectbox("Situação GEP:", situacoes)
    
    with col2:
        gerfes = ['Todas'] + sorted(df['gerfe'].dropna().unique().tolist())
        gerfe_sel = st.selectbox("GERFE:", gerfes)
    
    with col3:
        valor_min = st.number_input("Débito mínimo (R$):", min_value=0, value=0, step=100000)
    
    with col4:
        busca_empresa = st.text_input("🔎 Buscar empresa (CNPJ/Nome):")
    
    # Aplicar filtros
    df_filtrado = df.copy()
    
    if sit_sel != 'Todas':
        df_filtrado = df_filtrado[df_filtrado['situacao_atual_no_gep'] == sit_sel]
    
    if gerfe_sel != 'Todas':
        df_filtrado = df_filtrado[df_filtrado['gerfe'] == gerfe_sel]
    
    if valor_min > 0:
        df_filtrado = df_filtrado[df_filtrado['saldo_total_dividas'] >= valor_min]
    
    if busca_empresa:
        mask = (
            df_filtrado['cnpj'].astype(str).str.contains(busca_empresa, case=False, na=False) |
            df_filtrado['nome_empresarial'].astype(str).str.contains(busca_empresa, case=False, na=False)
        )
        df_filtrado = df_filtrado[mask]
    
    # Métricas do filtro
    st.divider()
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("📊 Processos", formatar_numero(len(df_filtrado)))
    with col2:
        st.metric("💰 Débito Total", formatar_valor_br(df_filtrado['saldo_total_dividas'].sum()))
    with col3:
        st.metric("📝 Declarado", formatar_valor_br(df_filtrado['imposto_declarado'].sum()))
    with col4:
        st.metric("📑 Dívida Ativa", formatar_valor_br(df_filtrado['divida_ativa'].sum()))
    
    st.divider()
    
    # Tabela de resultados
    st.markdown(f"<div class='sub-header'>📋 Listagem ({len(df_filtrado)} processos)</div>", unsafe_allow_html=True)
    
    col_config = {
        'inscricao_estadual': st.column_config.TextColumn('IE', width='small'),
        'cnpj': st.column_config.TextColumn('CNPJ'),
        'nome_empresarial': st.column_config.TextColumn('Empresa', width='large'),
        'gerfe': st.column_config.TextColumn('GERFE'),
        'situacao_atual_no_gep': st.column_config.TextColumn('Situação'),
        'saldo_total_dividas': st.column_config.NumberColumn('Débito Total', format='R$ %.2f'),
        'imposto_declarado': st.column_config.NumberColumn('Declarado', format='R$ %.2f'),
        'divida_ativa': st.column_config.NumberColumn('Dívida Ativa', format='R$ %.2f'),
        'numero_processo': st.column_config.TextColumn('Processo'),
        'data_atualizacao_situacao': st.column_config.DatetimeColumn('Atualização', format='DD/MM/YYYY'),
    }
    
    cols_display = ['inscricao_estadual', 'cnpj', 'nome_empresarial', 'gerfe', 
                   'situacao_atual_no_gep', 'saldo_total_dividas', 'imposto_declarado',
                   'divida_ativa', 'numero_processo', 'data_atualizacao_situacao']
    cols_exist = [c for c in cols_display if c in df_filtrado.columns]
    
    st.dataframe(
        df_filtrado[cols_exist].head(500),
        column_config=col_config,
        use_container_width=True,
        hide_index=True,
        height=500
    )
    
    if len(df_filtrado) > 500:
        st.warning(f"⚠️ Exibindo 500 de {len(df_filtrado)} registros. Use os filtros para refinar.")
    
    # Detalhes da empresa selecionada
    st.divider()
    st.markdown("<div class='sub-header'>🔎 Detalhes da Empresa</div>", unsafe_allow_html=True)
    
    if not df_filtrado.empty:
        empresas_opcoes = df_filtrado.apply(
            lambda x: f"{x['cnpj']} - {x['nome_empresarial'][:50]}", axis=1
        ).tolist()
        
        empresa_sel = st.selectbox("Selecione uma empresa para detalhes:", [''] + empresas_opcoes[:100])
        
        if empresa_sel:
            cnpj_sel = empresa_sel.split(' - ')[0]
            empresa_dados = df_filtrado[df_filtrado['cnpj'] == cnpj_sel].iloc[0]
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**📋 Dados Cadastrais**")
                st.write(f"• **CNPJ:** {empresa_dados.get('cnpj', 'N/A')}")
                st.write(f"• **IE:** {empresa_dados.get('inscricao_estadual', 'N/A')}")
                st.write(f"• **Razão Social:** {empresa_dados.get('nome_empresarial', 'N/A')}")
                st.write(f"• **GERFE:** {empresa_dados.get('gerfe', 'N/A')}")
                st.write(f"• **Situação Cadastral:** {empresa_dados.get('situacao_cadastral', 'N/A')}")
                st.write(f"• **CNAE:** {empresa_dados.get('cnae_principal', 'N/A')} - {empresa_dados.get('descricao_cnae', '')}")
            
            with col2:
                st.markdown("**💰 Valores e Situação**")
                st.write(f"• **Situação GEP:** {empresa_dados.get('situacao_atual_no_gep', 'N/A')}")
                st.write(f"• **Processo:** {empresa_dados.get('numero_processo', 'N/A')}")
                st.write(f"• **Débito Total:** {formatar_valor_br(empresa_dados.get('saldo_total_dividas', 0))}")
                st.write(f"• **Imposto Declarado:** {formatar_valor_br(empresa_dados.get('imposto_declarado', 0))}")
                st.write(f"• **Dívida Ativa:** {formatar_valor_br(empresa_dados.get('divida_ativa', 0))}")
            
            if empresa_dados.get('observacao'):
                st.markdown(f"""
                <div class='info-box'>
                <b>📝 Observação:</b> {empresa_dados.get('observacao')}
                </div>
                """, unsafe_allow_html=True)


# =============================================================================
# PÁGINA: PARCELAMENTOS
# =============================================================================

def pagina_parcelamentos(dados, engine):
    st.markdown("<h1 class='main-header'>📑 Análise de Parcelamentos</h1>", unsafe_allow_html=True)
    
    kpis = calcular_kpis_gerais(dados)
    
    # KPIs
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            "📊 Com Parcelamento Ativo", 
            formatar_numero(kpis['qtd_parcelamento_ativo']),
            help="Contribuintes com parcelamento ativo"
        )
    with col2:
        st.metric(
            "💰 Valor Parcelado", 
            formatar_valor_br(kpis['vl_parcelado']),
            help="Valor total em parcelamentos ativos"
        )
    with col3:
        st.metric(
            "⚠️ Alertas", 
            formatar_numero(kpis['qtd_alertas']),
            help="Parcelamentos cancelados de contribuintes suspensos"
        )
    with col4:
        st.metric(
            "📈 Taxa Regularização", 
            f"{kpis['taxa_regularizacao']:.1f}%",
            help="Taxa de regularização por parcelamento"
        )
    
    st.divider()
    
    # Carregar detalhes sob demanda
    if st.button("📥 Carregar Detalhes de Parcelamentos", use_container_width=True):
        with st.spinner("Carregando parcelamentos..."):
            df_parc = carregar_parcelamentos(engine)
        
        if df_parc.empty:
            st.warning("Dados não disponíveis")
            return
        
        st.session_state['df_parcelamentos'] = df_parc
    
    if 'df_parcelamentos' in st.session_state:
        df_parc = st.session_state['df_parcelamentos']
        
        # Resumo por status
        st.markdown("<div class='sub-header'>📊 Distribuição por Status</div>", unsafe_allow_html=True)
        
        resumo_status = df_parc.groupby('status_parcelamento').agg({
            'cnpj_raiz': 'nunique',
            'num_parcelamento': 'count',
            'valor_parcelado': 'sum'
        }).reset_index()
        resumo_status.columns = ['Status', 'Contribuintes', 'Parcelamentos', 'Valor Total']
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.pie(
                resumo_status,
                values='Parcelamentos',
                names='Status',
                title='Quantidade por Status',
                color='Status',
                color_discrete_map={
                    'ATIVO': '#22c55e',
                    'CANCELADO': '#ef4444',
                    'QUITADO': '#3b82f6',
                    'PENDENTE AUTORIZAÇÃO': '#f59e0b',
                    'PENDENTE 1ª PARCELA': '#eab308'
                }
            )
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.bar(
                resumo_status.sort_values('Valor Total', ascending=True),
                y='Status',
                x='Valor Total',
                orientation='h',
                title='Valor por Status',
                color='Status',
                color_discrete_map={
                    'ATIVO': '#22c55e',
                    'CANCELADO': '#ef4444',
                    'QUITADO': '#3b82f6',
                    'PENDENTE AUTORIZAÇÃO': '#f59e0b',
                    'PENDENTE 1ª PARCELA': '#eab308'
                }
            )
            fig.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        
        # Alertas de cancelamento
        st.divider()
        st.markdown("<div class='sub-header'>⚠️ Alertas - Parcelamentos Cancelados</div>", unsafe_allow_html=True)
        
        df_alertas = df_parc[df_parc['flag_alerta_cancelamento'] == 1]
        
        if not df_alertas.empty:
            st.markdown(f"""
            <div class='alert-critico'>
            <b>⚠️ {len(df_alertas)} parcelamentos cancelados de contribuintes com processo suspenso!</b><br>
            Estes contribuintes podem precisar de reanálise para possível reativação do enquadramento.
            </div>
            """, unsafe_allow_html=True)
            
            st.dataframe(
                df_alertas[['cnpj_raiz', 'nome_empresarial', 'gerfe', 'situacao_atual_no_gep', 
                           'num_parcelamento', 'valor_parcelado', 'dt_cancelamento']].head(50),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.success("✅ Nenhum alerta de parcelamento cancelado")


# =============================================================================
# PÁGINA: ALERTAS
# =============================================================================

def pagina_alertas(dados, engine):
    st.markdown("<h1 class='main-header'>🚨 Central de Alertas</h1>", unsafe_allow_html=True)
    
    # Carregar KPIs de alertas
    df_kpis = dados.get('alertas_kpis', pd.DataFrame())
    df_resumo = dados.get('alertas_resumo', pd.DataFrame())
    
    # KPIs principais
    st.markdown("<div class='sub-header'>📊 Resumo de Alertas</div>", unsafe_allow_html=True)
    
    if not df_kpis.empty:
        total_alertas = df_kpis['qtd_alertas'].sum()
        total_contribuintes = df_kpis['qtd_contribuintes'].sum()
        total_valor = df_kpis['valor_total_debitos'].sum()
        
        # Contar por nível
        urgentes = df_kpis[df_kpis['nivel_criticidade'] == 'URGENTE']['qtd_alertas'].sum() if 'nivel_criticidade' in df_kpis.columns else 0
        criticos = df_kpis[df_kpis['nivel_criticidade'] == 'CRÍTICO']['qtd_alertas'].sum() if 'nivel_criticidade' in df_kpis.columns else 0
        atencao = df_kpis[df_kpis['nivel_criticidade'] == 'ATENÇÃO']['qtd_alertas'].sum() if 'nivel_criticidade' in df_kpis.columns else 0
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.markdown(f"""
            <div class='kpi-card' style='background: linear-gradient(135deg, #7c3aed 0%, #5b21b6 100%);'>
                <div style='font-size: 1.5rem;'>🚨</div>
                <div style='font-size: 0.85rem; opacity: 0.9;'>Total Alertas</div>
                <div style='font-size: 1.8rem; font-weight: bold;'>{int(total_alertas)}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class='kpi-card' style='background: linear-gradient(135deg, #dc2626 0%, #991b1b 100%);'>
                <div style='font-size: 1.5rem;'>🔴</div>
                <div style='font-size: 0.85rem; opacity: 0.9;'>URGENTES</div>
                <div style='font-size: 1.8rem; font-weight: bold;'>{int(urgentes)}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class='kpi-card' style='background: linear-gradient(135deg, #ea580c 0%, #c2410c 100%);'>
                <div style='font-size: 1.5rem;'>🟠</div>
                <div style='font-size: 0.85rem; opacity: 0.9;'>CRÍTICOS</div>
                <div style='font-size: 1.8rem; font-weight: bold;'>{int(criticos)}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
            <div class='kpi-card' style='background: linear-gradient(135deg, #eab308 0%, #ca8a04 100%);'>
                <div style='font-size: 1.5rem;'>🟡</div>
                <div style='font-size: 0.85rem; opacity: 0.9;'>ATENÇÃO</div>
                <div style='font-size: 1.8rem; font-weight: bold;'>{int(atencao)}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col5:
            st.markdown(f"""
            <div class='kpi-card' style='background: linear-gradient(135deg, #059669 0%, #047857 100%);'>
                <div style='font-size: 1.5rem;'>💰</div>
                <div style='font-size: 0.85rem; opacity: 0.9;'>Valor em Risco</div>
                <div style='font-size: 1.8rem; font-weight: bold;'>{formatar_valor_br(total_valor)}</div>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Tabela de tipos de alerta
        st.markdown("<div class='sub-header'>📋 Alertas por Tipo</div>", unsafe_allow_html=True)
        
        col_config = {
            'tipo_alerta': st.column_config.TextColumn('Tipo de Alerta', width='large'),
            'nivel_criticidade': st.column_config.TextColumn('Nível', width='small'),
            'qtd_alertas': st.column_config.NumberColumn('Qtd Alertas', format='%d'),
            'qtd_contribuintes': st.column_config.NumberColumn('Contribuintes', format='%d'),
            'valor_total_debitos': st.column_config.NumberColumn('Valor Débitos', format='R$ %.2f'),
        }
        
        st.dataframe(
            df_kpis,
            column_config=col_config,
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("📊 Dados de resumo não disponíveis. Execute o script SQL de alertas.")
    
    st.divider()
    
    # Tabs por tipo de alerta
    st.markdown("<div class='sub-header'>🔍 Detalhamento de Alertas</div>", unsafe_allow_html=True)
    
    tabs = st.tabs([
        "🔴 Suspenso - Parc. Cancelado", 
        "🟠 Enquadrado - Parc. Cancelado",
        "⏳ Parc. Pend. 1ª Parcela",
        "⏰ Intimação +60 dias",
        "💰 Débito Alto",
        "📋 Todos os Alertas"
    ])
    
    # Carregar alertas detalhados sob demanda
    if 'df_alertas_detalhados' not in st.session_state:
        if st.button("📥 Carregar Alertas Detalhados", use_container_width=True, key="btn_alertas"):
            with st.spinner("Carregando alertas..."):
                df_alertas = carregar_alertas_detalhados(engine)
            
            if not df_alertas.empty:
                st.session_state['df_alertas_detalhados'] = df_alertas
                st.rerun()
            else:
                st.warning("Nenhum alerta encontrado ou tabela não existe.")
                return
    
    if 'df_alertas_detalhados' in st.session_state:
        df_alertas = st.session_state['df_alertas_detalhados']
        
        with tabs[0]:
            # Alerta 2: Parcelamento cancelado de suspenso (URGENTE)
            df_tipo2 = df_alertas[df_alertas['cd_tipo_alerta'] == 2]
            
            if not df_tipo2.empty:
                st.markdown(f"""
                <div class='alert-critico'>
                <b>🔴 URGENTE: {len(df_tipo2)} DÉBITOS de contribuintes SUSPENSOS com parcelamento CANCELADO!</b><br>
                O parcelamento que justificava a suspensão foi cancelado. 
                <b>AVALIAR REATIVAÇÃO DO ENQUADRAMENTO!</b>
                </div>
                """, unsafe_allow_html=True)
                
                # Filtros
                col1, col2 = st.columns(2)
                with col1:
                    gerfes = ['Todas'] + sorted(df_tipo2['gerfe'].dropna().unique().tolist())
                    gerfe_sel = st.selectbox("Filtrar GERFE:", gerfes, key="gerfe_alerta2")
                with col2:
                    busca = st.text_input("🔎 Buscar empresa:", key="busca_alerta2")
                
                df_filtrado = df_tipo2.copy()
                if gerfe_sel != 'Todas':
                    df_filtrado = df_filtrado[df_filtrado['gerfe'] == gerfe_sel]
                if busca:
                    mask = (
                        df_filtrado['cnpj_raiz_formatado'].astype(str).str.contains(busca, case=False, na=False) |
                        df_filtrado['nome_empresarial'].astype(str).str.contains(busca, case=False, na=False)
                    )
                    df_filtrado = df_filtrado[mask]
                
                # Colunas disponíveis podem variar, usar try/except
                cols_display = ['cnpj_raiz_formatado', 'nome_empresarial', 'gerfe', 'situacao_gep',
                               'grupo_debito_desc', 'num_documento_debito', 'num_parcelamento', 
                               'valor_parcelado', 'dt_cancelamento', 'saldo_total_dividas']
                cols_exist = [c for c in cols_display if c in df_filtrado.columns]
                
                st.dataframe(
                    df_filtrado[cols_exist],
                    column_config={
                        'cnpj_raiz_formatado': 'CNPJ Raiz',
                        'nome_empresarial': 'Empresa',
                        'gerfe': 'GERFE',
                        'situacao_gep': 'Situação',
                        'grupo_debito_desc': 'Tipo Débito',
                        'num_documento_debito': 'Nº Documento',
                        'num_parcelamento': 'Parcelamento',
                        'valor_parcelado': st.column_config.NumberColumn('Vlr Parcelado', format='R$ %.2f'),
                        'dt_cancelamento': st.column_config.DateColumn('Dt Cancelamento', format='DD/MM/YYYY'),
                        'saldo_total_dividas': st.column_config.NumberColumn('Débito Total', format='R$ %.2f'),
                    },
                    use_container_width=True,
                    hide_index=True,
                    height=400
                )
            else:
                st.success("✅ Nenhum alerta deste tipo")
        
        with tabs[1]:
            # Alerta 1: Parcelamento cancelado de enquadrado (CRÍTICO)
            df_tipo1 = df_alertas[df_alertas['cd_tipo_alerta'] == 1]
            
            if not df_tipo1.empty:
                st.markdown(f"""
                <div class='alert-atencao'>
                <b>🟠 CRÍTICO: {len(df_tipo1)} DÉBITOS de contribuintes ENQUADRADOS com parcelamento CANCELADO!</b><br>
                Débitos específicos que foram enquadrados e tinham parcelamento cancelado.
                Verificar intensificação de cobrança.
                </div>
                """, unsafe_allow_html=True)
                
                # Filtros
                col1, col2 = st.columns(2)
                with col1:
                    gerfes = ['Todas'] + sorted(df_tipo1['gerfe'].dropna().unique().tolist())
                    gerfe_sel = st.selectbox("Filtrar GERFE:", gerfes, key="gerfe_alerta1")
                with col2:
                    busca = st.text_input("🔎 Buscar empresa:", key="busca_alerta1")
                
                df_filtrado = df_tipo1.copy()
                if gerfe_sel != 'Todas':
                    df_filtrado = df_filtrado[df_filtrado['gerfe'] == gerfe_sel]
                if busca:
                    mask = (
                        df_filtrado['cnpj_raiz_formatado'].astype(str).str.contains(busca, case=False, na=False) |
                        df_filtrado['nome_empresarial'].astype(str).str.contains(busca, case=False, na=False)
                    )
                    df_filtrado = df_filtrado[mask]
                
                cols_display = ['cnpj_raiz_formatado', 'nome_empresarial', 'gerfe',
                               'grupo_debito_desc', 'num_documento_debito', 'num_parcelamento',
                               'qtd_parcelas', 'valor_parcelado', 
                               'dt_cancelamento', 'saldo_total_dividas']
                cols_exist = [c for c in cols_display if c in df_filtrado.columns]
                
                st.dataframe(
                    df_filtrado[cols_exist],
                    column_config={
                        'cnpj_raiz_formatado': 'CNPJ Raiz',
                        'nome_empresarial': 'Empresa',
                        'gerfe': 'GERFE',
                        'grupo_debito_desc': 'Tipo Débito',
                        'num_documento_debito': 'Nº Documento',
                        'num_parcelamento': 'Parcelamento',
                        'qtd_parcelas': 'Parcelas',
                        'valor_parcelado': st.column_config.NumberColumn('Vlr Parcelado', format='R$ %.2f'),
                        'dt_cancelamento': st.column_config.DateColumn('Dt Cancelamento', format='DD/MM/YYYY'),
                        'saldo_total_dividas': st.column_config.NumberColumn('Débito Total', format='R$ %.2f'),
                    },
                    use_container_width=True,
                    hide_index=True,
                    height=400
                )
            else:
                st.success("✅ Nenhum alerta deste tipo")
        
        with tabs[2]:
            # Alerta 3: Parcelamento pendente 1ª parcela
            df_tipo3 = df_alertas[df_alertas['cd_tipo_alerta'] == 3]
            
            if not df_tipo3.empty:
                st.markdown(f"""
                <div class='alert-atencao'>
                <b>⏳ ATENÇÃO: {len(df_tipo3)} parcelamentos pendentes de 1ª parcela há +30 dias!</b><br>
                Parcelamentos aprovados mas sem pagamento da primeira parcela. Podem ser cancelados automaticamente.
                </div>
                """, unsafe_allow_html=True)
                
                cols_display = ['cnpj_raiz_formatado', 'nome_empresarial', 'gerfe', 'situacao_gep',
                               'grupo_debito_desc', 'num_parcelamento', 'valor_parcelado',
                               'dt_pedido_parcelamento', 'saldo_total_dividas']
                cols_exist = [c for c in cols_display if c in df_tipo3.columns]
                
                st.dataframe(
                    df_tipo3[cols_exist],
                    column_config={
                        'cnpj_raiz_formatado': 'CNPJ Raiz',
                        'nome_empresarial': 'Empresa',
                        'gerfe': 'GERFE',
                        'situacao_gep': 'Situação',
                        'grupo_debito_desc': 'Tipo Débito',
                        'num_parcelamento': 'Parcelamento',
                        'valor_parcelado': st.column_config.NumberColumn('Vlr Parcelado', format='R$ %.2f'),
                        'dt_pedido_parcelamento': st.column_config.DateColumn('Dt Pedido', format='DD/MM/YYYY'),
                        'saldo_total_dividas': st.column_config.NumberColumn('Débito Total', format='R$ %.2f'),
                    },
                    use_container_width=True,
                    hide_index=True,
                    height=400
                )
            else:
                st.success("✅ Nenhum alerta deste tipo")
        
        with tabs[3]:
            # Alerta 4: Intimação há mais de 60 dias
            df_tipo4 = df_alertas[df_alertas['cd_tipo_alerta'] == 4]
            
            if not df_tipo4.empty:
                st.markdown(f"""
                <div class='alert-atencao'>
                <b>⏰ ATENÇÃO: {len(df_tipo4)} contribuintes intimados há mais de 60 dias sem ação!</b><br>
                Processos que podem estar parados. Avaliar próximo passo (enquadramento ou verificar pendências).
                </div>
                """, unsafe_allow_html=True)
                
                st.dataframe(
                    df_tipo4[[
                        'cnpj_raiz_formatado', 'nome_empresarial', 'gerfe', 'numero_processo',
                        'saldo_total_dividas', 'data_evento', 'acao_sugerida'
                    ]],
                    column_config={
                        'cnpj_raiz_formatado': 'CNPJ Raiz',
                        'nome_empresarial': 'Empresa',
                        'gerfe': 'GERFE',
                        'numero_processo': 'Processo',
                        'saldo_total_dividas': st.column_config.NumberColumn('Débito Total', format='R$ %.2f'),
                        'data_evento': st.column_config.DateColumn('Última Atualização', format='DD/MM/YYYY'),
                        'acao_sugerida': 'Ação Sugerida'
                    },
                    use_container_width=True,
                    hide_index=True,
                    height=400
                )
            else:
                st.success("✅ Nenhum alerta deste tipo")
        
        with tabs[4]:
            # Alerta 5: Débito alto ainda em intimação
            df_tipo5 = df_alertas[df_alertas['cd_tipo_alerta'] == 5]
            
            if not df_tipo5.empty:
                valor_total = df_tipo5['saldo_total_dividas'].sum()
                
                st.markdown(f"""
                <div class='alert-atencao'>
                <b>💰 ATENÇÃO: {len(df_tipo5)} contribuintes com débito ≥ R$ 5 milhões ainda em intimação!</b><br>
                Valor total em risco: <b>{formatar_valor_br(valor_total)}</b>. Priorizar conclusão do processo.
                </div>
                """, unsafe_allow_html=True)
                
                st.dataframe(
                    df_tipo5[[
                        'cnpj_raiz_formatado', 'nome_empresarial', 'gerfe', 'situacao_gep',
                        'numero_processo', 'saldo_total_dividas', 'debito_declarado', 'divida_ativa'
                    ]].sort_values('saldo_total_dividas', ascending=False),
                    column_config={
                        'cnpj_raiz_formatado': 'CNPJ Raiz',
                        'nome_empresarial': 'Empresa',
                        'gerfe': 'GERFE',
                        'situacao_gep': 'Situação',
                        'numero_processo': 'Processo',
                        'saldo_total_dividas': st.column_config.NumberColumn('Débito Total', format='R$ %.2f'),
                        'debito_declarado': st.column_config.NumberColumn('Declarado', format='R$ %.2f'),
                        'divida_ativa': st.column_config.NumberColumn('Dívida Ativa', format='R$ %.2f'),
                    },
                    use_container_width=True,
                    hide_index=True,
                    height=400
                )
            else:
                st.success("✅ Nenhum alerta deste tipo")
        
        with tabs[5]:
            # Todos os alertas
            st.markdown(f"**📊 Total: {len(df_alertas)} alertas**")
            
            # Filtros gerais
            col1, col2, col3 = st.columns(3)
            with col1:
                tipos = ['Todos'] + df_alertas['tipo_alerta'].unique().tolist()
                tipo_sel = st.selectbox("Tipo de Alerta:", tipos, key="tipo_todos")
            with col2:
                niveis = ['Todos'] + df_alertas['nivel_criticidade'].unique().tolist()
                nivel_sel = st.selectbox("Nível:", niveis, key="nivel_todos")
            with col3:
                gerfes = ['Todas'] + sorted(df_alertas['gerfe'].dropna().unique().tolist())
                gerfe_sel = st.selectbox("GERFE:", gerfes, key="gerfe_todos")
            
            df_filtrado = df_alertas.copy()
            if tipo_sel != 'Todos':
                df_filtrado = df_filtrado[df_filtrado['tipo_alerta'] == tipo_sel]
            if nivel_sel != 'Todos':
                df_filtrado = df_filtrado[df_filtrado['nivel_criticidade'] == nivel_sel]
            if gerfe_sel != 'Todas':
                df_filtrado = df_filtrado[df_filtrado['gerfe'] == gerfe_sel]
            
            st.dataframe(
                df_filtrado[[
                    'tipo_alerta', 'nivel_criticidade', 'cnpj_raiz_formatado', 
                    'nome_empresarial', 'gerfe', 'situacao_gep',
                    'saldo_total_dividas', 'descricao_alerta'
                ]],
                column_config={
                    'tipo_alerta': 'Tipo',
                    'nivel_criticidade': 'Nível',
                    'cnpj_raiz_formatado': 'CNPJ Raiz',
                    'nome_empresarial': 'Empresa',
                    'gerfe': 'GERFE',
                    'situacao_gep': 'Situação',
                    'saldo_total_dividas': st.column_config.NumberColumn('Débito', format='R$ %.2f'),
                    'descricao_alerta': 'Descrição'
                },
                use_container_width=True,
                hide_index=True,
                height=500
            )
    
    # Info box
    st.divider()
    st.markdown("""
    <div class='info-box'>
    <b>📌 Tipos de Alerta (baseados nos DÉBITOS enquadrados):</b><br>
    • <b style='color:#dc2626'>URGENTE - Suspenso c/ Parc. Cancelado:</b> Débito enquadrado de contribuinte SUSPENSO teve parcelamento cancelado. <b>AVALIAR REATIVAÇÃO!</b><br>
    • <b style='color:#ea580c'>CRÍTICO - Enquadrado c/ Parc. Cancelado:</b> Débito enquadrado de contribuinte ativo teve parcelamento cancelado. Débito exigível.<br>
    • <b style='color:#eab308'>ATENÇÃO - Parc. Pend. 1ª Parcela:</b> Parcelamento de débito enquadrado sem pagamento há +30 dias. Pode ser cancelado.<br>
    • <b style='color:#eab308'>ATENÇÃO - Intimação +60 dias:</b> Processo parado há mais de 60 dias na fase de intimação.<br>
    • <b style='color:#eab308'>ATENÇÃO - Débito Alto:</b> Contribuinte com débito ≥ R$ 5 milhões ainda em fase de intimação.<br>
    • <b style='color:#6b7280'>INFORMATIVO - Sem Parcelamento:</b> Débito enquadrado ≥ R$ 100k sem parcelamento ativo.
    </div>
    """, unsafe_allow_html=True)


# =============================================================================
# PÁGINA: COMUNICAÇÕES
# =============================================================================

def pagina_comunicacoes(dados, engine):
    st.markdown("<h1 class='main-header'>📨 Comunicações Enviadas</h1>", unsafe_allow_html=True)
    
    # Carregar sob demanda
    if st.button("📥 Carregar Comunicações", use_container_width=True):
        with st.spinner("Carregando comunicações..."):
            df_com = carregar_comunicacoes(engine)
        
        if df_com.empty:
            st.warning("Dados não disponíveis")
            return
        
        st.session_state['df_comunicacoes'] = df_com
    
    if 'df_comunicacoes' in st.session_state:
        df_com = st.session_state['df_comunicacoes']
        
        # Métricas
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("📧 Total Comunicações", formatar_numero(len(df_com)))
        with col2:
            enviados = len(df_com[df_com['resultado_envio'] == 'Enviado'])
            st.metric("✅ Enviados", formatar_numero(enviados))
        with col3:
            contribuintes = df_com['inscricao_estadual'].nunique()
            st.metric("🏢 Contribuintes", formatar_numero(contribuintes))
        with col4:
            st.metric("💰 Débito Total", formatar_valor_br(df_com['debito_total'].sum()))
        
        st.divider()
        
        # Distribuição por tipo
        col1, col2 = st.columns(2)
        
        with col1:
            resumo_tipo = df_com['tipo_meio_comunicacao'].value_counts().reset_index()
            resumo_tipo.columns = ['Tipo', 'Quantidade']
            
            fig = px.pie(resumo_tipo, values='Quantidade', names='Tipo', 
                        title='Por Tipo de Comunicação')
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            resumo_resultado = df_com['resultado_envio'].value_counts().reset_index()
            resumo_resultado.columns = ['Resultado', 'Quantidade']
            
            fig = px.pie(resumo_resultado, values='Quantidade', names='Resultado',
                        title='Por Resultado',
                        color='Resultado',
                        color_discrete_map={
                            'Enviado': '#22c55e',
                            'Bloqueado': '#ef4444',
                            'Redundante': '#f59e0b',
                            'Inválido': '#6b7280'
                        })
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)
        
        # Tabela
        st.markdown("<div class='sub-header'>📋 Últimas Comunicações</div>", unsafe_allow_html=True)
        
        st.dataframe(
            df_com[['inscricao_estadual', 'numero_processo', 'nome_empresarial', 
                   'tipo_meio_comunicacao', 'tipo_destinacao', 'resultado_envio',
                   'data_envio', 'debito_total']].head(100),
            column_config={
                'inscricao_estadual': 'IE',
                'numero_processo': 'Processo',
                'nome_empresarial': 'Empresa',
                'tipo_meio_comunicacao': 'Meio',
                'tipo_destinacao': 'Destinação',
                'resultado_envio': 'Resultado',
                'data_envio': st.column_config.DatetimeColumn('Data', format='DD/MM/YYYY HH:mm'),
                'debito_total': st.column_config.NumberColumn('Débito', format='R$ %.2f')
            },
            use_container_width=True,
            hide_index=True,
            height=400
        )


# =============================================================================
# PÁGINA: PROCESSOS ENCERRADOS
# =============================================================================

def pagina_processos_encerrados(dados, engine):
    st.markdown("<h1 class='main-header'>✅ Processos Encerrados</h1>", unsafe_allow_html=True)
    
    kpis = calcular_kpis_gerais(dados)
    
    st.metric("📊 Total Encerrados", formatar_numero(kpis['qtd_encerrados']))
    
    st.divider()
    
    # Carregar sob demanda
    if st.button("📥 Carregar Processos Encerrados", use_container_width=True):
        with st.spinner("Carregando..."):
            df_enc = carregar_processos_encerrados(engine)
        
        if df_enc.empty:
            st.warning("Dados não disponíveis")
            return
        
        st.session_state['df_encerrados'] = df_enc
    
    if 'df_encerrados' in st.session_state:
        df_enc = st.session_state['df_encerrados']
        
        # Resumo por situação
        resumo = df_enc.groupby(['situacao_atual', 'situacao_atual_no_gep']).size().reset_index(name='quantidade')
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("<div class='sub-header'>📊 Por Situação Atual</div>", unsafe_allow_html=True)
            fig = px.pie(
                df_enc['situacao_atual'].value_counts().reset_index(),
                values='count',
                names='situacao_atual',
                color_discrete_sequence=px.colors.qualitative.Set2
            )
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("<div class='sub-header'>📊 Por Motivo GEP</div>", unsafe_allow_html=True)
            fig = px.pie(
                df_enc['situacao_atual_no_gep'].value_counts().reset_index(),
                values='count',
                names='situacao_atual_no_gep',
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)
        
        # Tabela
        st.markdown("<div class='sub-header'>📋 Listagem</div>", unsafe_allow_html=True)
        
        st.dataframe(
            df_enc[['inscricao_estadual', 'cnpj', 'nome_empresarial', 'gerfe',
                   'situacao_atual', 'situacao_atual_no_gep', 'saldo_total_dividas',
                   'data_atualizacao_situacao']].head(200),
            column_config={
                'saldo_total_dividas': st.column_config.NumberColumn('Débito', format='R$ %.2f'),
                'data_atualizacao_situacao': st.column_config.DatetimeColumn('Data', format='DD/MM/YYYY')
            },
            use_container_width=True,
            hide_index=True,
            height=400
        )


# =============================================================================
# PÁGINA: EXTRATOS E PUBLICAÇÕES
# =============================================================================

def pagina_extratos(dados, engine):
    st.markdown("<h1 class='main-header'>📰 Extratos e Publicações Pe/SEF</h1>", unsafe_allow_html=True)
    
    tabs = st.tabs(["🔴 Enquadramentos", "🟢 Desenquadramentos"])
    
    with tabs[0]:
        st.markdown("<div class='sub-header'>📋 Extratos de Enquadramento</div>", unsafe_allow_html=True)
        
        if st.button("📥 Carregar Enquadramentos", key="btn_enq"):
            with st.spinner("Carregando..."):
                df = carregar_extratos_enquadramentos(engine)
            
            if not df.empty:
                st.session_state['df_extratos_enq'] = df
        
        if 'df_extratos_enq' in st.session_state:
            df = st.session_state['df_extratos_enq']
            
            st.metric("📊 Total Enquadramentos", len(df))
            
            st.dataframe(
                df[['razao_social', 'cnpj_raiz', 'inscricao_estadual', 'gerfe',
                   'processo_enquadramento', 'termo_enquadramento', 'ref',
                   'pesef', 'data_pesef', 'inicio_efeitos']],
                column_config={
                    'razao_social': 'Empresa',
                    'cnpj_raiz': 'CNPJ Raiz',
                    'inscricao_estadual': 'IE',
                    'gerfe': 'GERFE',
                    'processo_enquadramento': 'Processo',
                    'termo_enquadramento': 'Termo',
                    'ref': 'REF',
                    'pesef': 'Pe/SEF',
                    'data_pesef': st.column_config.DateColumn('Data Pe/SEF', format='DD/MM/YYYY'),
                    'inicio_efeitos': st.column_config.DateColumn('Início Efeitos', format='DD/MM/YYYY')
                },
                use_container_width=True,
                hide_index=True,
                height=500
            )
    
    with tabs[1]:
        st.markdown("<div class='sub-header'>📋 Extratos de Desenquadramento</div>", unsafe_allow_html=True)
        st.info("ℹ️ Nenhum desenquadramento ativo no momento. Os contribuintes desenquadrados já foram finalizados.")


# =============================================================================
# PÁGINA: DRILL DOWN - DETALHES DA EMPRESA
# =============================================================================

def pagina_drill_down_empresa(dados, engine):
    st.markdown("<h1 class='main-header'>🔍 Drill Down - Análise Detalhada por Empresa</h1>", unsafe_allow_html=True)
    
    # Inicializar session_state para navegação
    if 'empresa_selecionada_cnpj' not in st.session_state:
        st.session_state.empresa_selecionada_cnpj = None
    if 'drill_down_ativo' not in st.session_state:
        st.session_state.drill_down_ativo = False
    
    # Se não tem empresa selecionada, mostrar listagem
    if not st.session_state.drill_down_ativo or not st.session_state.empresa_selecionada_cnpj:
        exibir_listagem_empresas(dados, engine)
    else:
        exibir_detalhes_empresa(engine)


def exibir_listagem_empresas(dados, engine):
    """Exibe listagem de empresas com filtros para seleção"""
    
    st.markdown("<div class='sub-header'>📋 Selecione uma Empresa para Análise Detalhada</div>", unsafe_allow_html=True)
    
    # Carregar dados
    with st.spinner("Carregando lista de empresas..."):
        df = carregar_situacao_atual(engine)
    
    if df.empty:
        st.warning("Dados não disponíveis")
        return
    
    # Filtros em linha
    st.markdown("#### 🔍 Filtros")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        gerfes = ['Todas'] + sorted(df['gerfe'].dropna().unique().tolist())
        gerfe_sel = st.selectbox("GERFE:", gerfes, key="drill_gerfe")
    
    with col2:
        situacoes = ['Todas'] + sorted(df['situacao_atual_no_gep'].dropna().unique().tolist())
        sit_sel = st.selectbox("Situação:", situacoes, key="drill_sit")
    
    with col3:
        valor_opcoes = ['Todos', '> R$ 1 milhão', '> R$ 5 milhões', '> R$ 10 milhões', '> R$ 50 milhões']
        valor_sel = st.selectbox("Débito:", valor_opcoes, key="drill_valor")
    
    with col4:
        busca = st.text_input("🔎 Buscar (CNPJ/Nome):", key="drill_busca")
    
    # Aplicar filtros
    df_filtrado = df.copy()
    
    if gerfe_sel != 'Todas':
        df_filtrado = df_filtrado[df_filtrado['gerfe'] == gerfe_sel]
    
    if sit_sel != 'Todas':
        df_filtrado = df_filtrado[df_filtrado['situacao_atual_no_gep'] == sit_sel]
    
    if valor_sel == '> R$ 1 milhão':
        df_filtrado = df_filtrado[df_filtrado['saldo_total_dividas'] >= 1000000]
    elif valor_sel == '> R$ 5 milhões':
        df_filtrado = df_filtrado[df_filtrado['saldo_total_dividas'] >= 5000000]
    elif valor_sel == '> R$ 10 milhões':
        df_filtrado = df_filtrado[df_filtrado['saldo_total_dividas'] >= 10000000]
    elif valor_sel == '> R$ 50 milhões':
        df_filtrado = df_filtrado[df_filtrado['saldo_total_dividas'] >= 50000000]
    
    if busca:
        mask = (
            df_filtrado['cnpj'].astype(str).str.contains(busca, case=False, na=False) |
            df_filtrado['cnpj_raiz'].astype(str).str.contains(busca, case=False, na=False) |
            df_filtrado['nome_empresarial'].astype(str).str.contains(busca, case=False, na=False)
        )
        df_filtrado = df_filtrado[mask]
    
    # Ordenar por débito
    df_filtrado = df_filtrado.sort_values('saldo_total_dividas', ascending=False)
    
    # Métricas do filtro
    st.divider()
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("📊 Empresas", formatar_numero(len(df_filtrado)))
    with col2:
        st.metric("💰 Débito Total", formatar_valor_br(df_filtrado['saldo_total_dividas'].sum()))
    with col3:
        enquadrados = len(df_filtrado[df_filtrado['cd_situacao_gep'] == 30])
        st.metric("🔴 Enquadrados", formatar_numero(enquadrados))
    with col4:
        suspensos = len(df_filtrado[df_filtrado['cd_situacao_gep'].isin([31, 32])])
        st.metric("⏸️ Suspensos", formatar_numero(suspensos))
    
    st.divider()
    
    # Tabela interativa com botões
    st.markdown(f"#### 📋 Lista de Empresas ({len(df_filtrado)} resultados)")
    
    if df_filtrado.empty:
        st.info("Nenhuma empresa encontrada com os filtros selecionados.")
        return
    
    # Exibir empresas em cards/rows clicáveis
    for idx, (_, row) in enumerate(df_filtrado.head(50).iterrows()):
        with st.container():
            col1, col2, col3, col4, col5 = st.columns([3, 2, 1.5, 1.5, 1])
            
            with col1:
                st.markdown(f"**{row['nome_empresarial'][:45]}**")
                st.caption(f"CNPJ: {row['cnpj']} | IE: {row['inscricao_estadual']}")
            
            with col2:
                situacao = row['situacao_atual_no_gep']
                cor = '🔴' if row['cd_situacao_gep'] == 30 else ('🟡' if row['cd_situacao_gep'] in [31, 32] else '🔵')
                st.markdown(f"{cor} {situacao}")
                st.caption(f"GERFE: {row['gerfe']}")
            
            with col3:
                st.markdown(f"**{formatar_valor_br(row['saldo_total_dividas'])}**")
                st.caption("Débito Total")
            
            with col4:
                st.markdown(f"**{formatar_valor_br(row['imposto_declarado'])}**")
                st.caption("Declarado")
            
            with col5:
                if st.button("🔍 Detalhes", key=f"btn_drill_{idx}_{row['cnpj_raiz']}"):
                    st.session_state.empresa_selecionada_cnpj = str(row['cnpj_raiz'])
                    st.session_state.drill_down_ativo = True
                    st.rerun()
            
            st.divider()
    
    if len(df_filtrado) > 50:
        st.warning(f"⚠️ Exibindo 50 de {len(df_filtrado)} empresas. Use os filtros para refinar a busca.")


def exibir_detalhes_empresa(engine):
    """Exibe todos os detalhes de uma empresa específica"""
    
    cnpj_raiz = st.session_state.empresa_selecionada_cnpj
    
    # Botão para voltar
    col1, col2 = st.columns([1, 5])
    with col1:
        if st.button("⬅️ Voltar à Lista", use_container_width=True):
            st.session_state.drill_down_ativo = False
            st.session_state.empresa_selecionada_cnpj = None
            st.rerun()
    
    # Carregar todos os detalhes
    with st.spinner("Carregando detalhes completos da empresa..."):
        detalhes = carregar_detalhes_empresa(engine, cnpj_raiz)
    
    if not detalhes or detalhes.get('situacao', pd.DataFrame()).empty:
        st.error("❌ Não foi possível carregar os detalhes desta empresa.")
        return
    
    # Dados principais
    sit = detalhes['situacao'].iloc[0]
    
    # Header com informações principais
    st.markdown(f"""
    <div style='background: linear-gradient(135deg, #1565C0 0%, #0D47A1 100%); 
                padding: 25px; border-radius: 12px; color: white; margin-bottom: 20px;'>
        <h2 style='margin: 0; color: white;'>{sit.get('nome_empresarial', 'N/A')}</h2>
        <p style='margin: 5px 0; opacity: 0.9;'>
            CNPJ: <strong>{sit.get('cnpj', 'N/A')}</strong> | 
            IE: <strong>{sit.get('inscricao_estadual', 'N/A')}</strong> |
            GERFE: <strong>{sit.get('gerfe', 'N/A')}</strong>
        </p>
        <p style='margin: 5px 0;'>
            <span style='background: rgba(255,255,255,0.2); padding: 5px 15px; border-radius: 20px;'>
                {sit.get('situacao_atual_no_gep', 'N/A')}
            </span>
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # KPIs principais
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("💰 Débito Total", formatar_valor_br(sit.get('saldo_total_dividas', 0)))
    with col2:
        st.metric("📝 Declarado (DIME)", formatar_valor_br(sit.get('imposto_declarado', 0)))
    with col3:
        st.metric("📑 Dívida Ativa", formatar_valor_br(sit.get('divida_ativa', 0)))
    with col4:
        qtd_debitos = sit.get('qtd_total_debitos', 0)
        st.metric("📊 Qtd. Débitos", formatar_numero(qtd_debitos))
    with col5:
        # Exibir situação cadastral ao invés de regime (campo não atualizado)
        sit_cad = sit.get('situacao_cadastral', 'N/A')
        st.metric("📋 Sit. Cadastral", str(sit_cad)[:15] if sit_cad else 'N/A')
    
    st.divider()
    
    # Tabs com detalhes
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📋 Cadastro", 
        "💳 Débitos", 
        "📑 Parcelamentos", 
        "🚨 Alertas",
        "📨 Comunicações", 
        "📜 Histórico"
    ])
    
    # TAB 1: Dados Cadastrais
    with tab1:
        st.markdown("### 📋 Dados Cadastrais Completos")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**🏢 Identificação**")
            st.write(f"• **Razão Social:** {sit.get('nome_empresarial', 'N/A')}")
            st.write(f"• **CNPJ:** {sit.get('cnpj', 'N/A')}")
            st.write(f"• **CNPJ Raiz:** {sit.get('cnpj_raiz', 'N/A')}")
            st.write(f"• **Inscrição Estadual:** {sit.get('inscricao_estadual', 'N/A')}")
            st.write(f"• **CNAE:** {sit.get('cnae_principal', 'N/A')} - {sit.get('descricao_cnae', 'N/A')}")
            
            st.markdown("**📍 Localização**")
            st.write(f"• **GERFE:** {sit.get('gerfe', 'N/A')}")
            st.write(f"• **Situação Cadastral:** {sit.get('situacao_cadastral', 'N/A')}")
            st.write(f"• **DT-e:** {sit.get('credenciado_dtec', 'N/A')}")
        
        with col2:
            st.markdown("**⚖️ Situação no Processo**")
            st.write(f"• **Situação Atual:** {sit.get('situacao_atual_no_gep', 'N/A')}")
            st.write(f"• **Código GEP:** {sit.get('cd_situacao_gep', 'N/A')}")
            st.write(f"• **Nº Processo:** {sit.get('numero_processo', 'N/A')}")
            
            st.markdown("**📅 Datas Importantes**")
            st.write(f"• **Pré-enquadramento:** {formatar_data(sit.get('data_pre_enquadramento'))}")
            st.write(f"• **Atualização Situação:** {formatar_data(sit.get('data_atualizacao_situacao'))}")
            st.write(f"• **Última Atualização:** {formatar_data(sit.get('data_ultima_atualizacao'))}")
        
        # Observações
        if sit.get('observacao'):
            st.markdown("**📝 Observações**")
            st.info(sit.get('observacao'))
    
    # TAB 2: Débitos
    with tab2:
        st.markdown("### 💳 Débitos Enquadrados")
        
        df_debitos = detalhes.get('debitos', pd.DataFrame())
        
        if not df_debitos.empty:
            # Resumo
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("📊 Total Débitos", len(df_debitos))
            with col2:
                st.metric("💰 Valor Total", formatar_valor_br(df_debitos['saldo_debito'].sum()))
            with col3:
                dime = len(df_debitos[df_debitos['grupo_debito_desc'] == 'DIME'])
                st.metric("📝 DIME", dime)
            with col4:
                dva = len(df_debitos[df_debitos['grupo_debito_desc'] == 'Dívida Ativa'])
                st.metric("📑 Dívida Ativa", dva)
            
            st.divider()
            
            # Tabela de débitos
            cols_debitos = ['num_documento_debito', 'grupo_debito_desc', 'tipo_transferencia_desc', 
                           'saldo_debito', 'status_parcelamento_desc', 'num_parcelamento', 'valor_parcelado']
            cols_exist = [c for c in cols_debitos if c in df_debitos.columns]
            
            col_config_deb = {
                'num_documento_debito': st.column_config.TextColumn('Nº Documento'),
                'grupo_debito_desc': st.column_config.TextColumn('Tipo'),
                'tipo_transferencia_desc': st.column_config.TextColumn('Transferência'),
                'saldo_debito': st.column_config.NumberColumn('Saldo', format='R$ %.2f'),
                'status_parcelamento_desc': st.column_config.TextColumn('Status Parc.'),
                'num_parcelamento': st.column_config.TextColumn('Nº Parcelamento'),
                'valor_parcelado': st.column_config.NumberColumn('Valor Parcelado', format='R$ %.2f'),
            }
            
            st.dataframe(df_debitos[cols_exist], column_config=col_config_deb, 
                        use_container_width=True, hide_index=True, height=400)
            
            # Gráfico por tipo
            if 'grupo_debito_desc' in df_debitos.columns:
                fig = px.pie(df_debitos, names='grupo_debito_desc', values='saldo_debito',
                            title='Distribuição por Tipo de Débito', hole=0.4)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("ℹ️ Nenhum débito detalhado encontrado para esta empresa.")
    
    # TAB 3: Parcelamentos
    with tab3:
        st.markdown("### 📑 Parcelamentos")
        
        df_parc = detalhes.get('parcelamentos', pd.DataFrame())
        
        if not df_parc.empty:
            # Resumo por status
            if 'status_parcelamento' in df_parc.columns:
                resumo_status = df_parc.groupby('status_parcelamento').agg({
                    'num_parcelamento': 'count',
                    'valor_parcelado': 'sum'
                }).reset_index()
                resumo_status.columns = ['Status', 'Quantidade', 'Valor Total']
                
                col1, col2 = st.columns([1, 2])
                with col1:
                    st.dataframe(resumo_status, hide_index=True)
                with col2:
                    fig = px.bar(resumo_status, x='Status', y='Valor Total', 
                                title='Valor por Status de Parcelamento')
                    st.plotly_chart(fig, use_container_width=True)
            
            st.divider()
            
            # Tabela detalhada
            cols_parc = ['num_parcelamento', 'status_parcelamento', 'valor_parcelado', 
                        'saldo_divida', 'num_parcelas', 'dt_pedido', 'dt_cancelamento']
            cols_exist = [c for c in cols_parc if c in df_parc.columns]
            
            st.dataframe(df_parc[cols_exist], use_container_width=True, hide_index=True, height=300)
        else:
            st.info("ℹ️ Nenhum parcelamento encontrado para esta empresa.")
    
    # TAB 4: Alertas
    with tab4:
        st.markdown("### 🚨 Alertas da Empresa")
        
        df_alertas = detalhes.get('alertas', pd.DataFrame())
        
        if not df_alertas.empty:
            for _, alerta in df_alertas.iterrows():
                nivel = alerta.get('nivel_criticidade', 'INFO')
                tipo = alerta.get('tipo_alerta', 'N/A')
                desc = alerta.get('descricao_alerta', 'N/A')
                acao = alerta.get('acao_sugerida', 'N/A')
                
                if nivel == 'URGENTE':
                    st.error(f"🚨 **{tipo}**\n\n{desc}\n\n**Ação:** {acao}")
                elif nivel == 'CRÍTICO':
                    st.warning(f"⚠️ **{tipo}**\n\n{desc}\n\n**Ação:** {acao}")
                elif nivel == 'ATENÇÃO':
                    st.info(f"ℹ️ **{tipo}**\n\n{desc}\n\n**Ação:** {acao}")
                else:
                    st.success(f"📋 **{tipo}**\n\n{desc}")
        else:
            st.success("✅ Nenhum alerta pendente para esta empresa.")
    
    # TAB 5: Comunicações
    with tab5:
        st.markdown("### 📨 Comunicações Enviadas")
        
        df_com = detalhes.get('comunicacoes', pd.DataFrame())
        
        if not df_com.empty:
            # Resumo
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("📧 Total Enviadas", len(df_com))
            with col2:
                if 'resultado_envio' in df_com.columns:
                    enviadas = len(df_com[df_com['resultado_envio'] == 'Enviado'])
                    st.metric("✅ Com Sucesso", enviadas)
            with col3:
                if 'data_envio' in df_com.columns and not df_com['data_envio'].isna().all():
                    ultima = pd.to_datetime(df_com['data_envio']).max()
                    st.metric("📅 Última", ultima.strftime('%d/%m/%Y') if pd.notna(ultima) else 'N/A')
            
            st.divider()
            
            cols_com = ['data_envio', 'tipo_meio_comunicacao', 'tipo_destinacao', 
                       'resultado_envio', 'destinatario_contato']
            cols_exist = [c for c in cols_com if c in df_com.columns]
            
            st.dataframe(df_com[cols_exist], use_container_width=True, hide_index=True, height=300)
        else:
            st.info("ℹ️ Nenhuma comunicação registrada para esta empresa.")
    
    # TAB 6: Histórico
    with tab6:
        st.markdown("### 📜 Histórico do Processo")
        
        df_hist = detalhes.get('historico', pd.DataFrame())
        
        if not df_hist.empty:
            # Timeline visual
            st.markdown("#### 📅 Linha do Tempo")
            
            for _, evento in df_hist.head(20).iterrows():
                data = formatar_data(evento.get('data_atualizacao_situacao'))
                situacao = evento.get('situacao_atual_no_gep', 'N/A')
                sit_atual = evento.get('situacao_atual', 'N/A')
                
                st.markdown(f"""
                <div style='border-left: 3px solid #1565C0; padding-left: 15px; margin: 10px 0;'>
                    <p style='color: #1565C0; font-weight: bold; margin: 0;'>{data}</p>
                    <p style='margin: 5px 0;'><strong>{situacao}</strong> - {sit_atual}</p>
                </div>
                """, unsafe_allow_html=True)
            
            if len(df_hist) > 20:
                st.caption(f"Exibindo 20 de {len(df_hist)} eventos.")
        else:
            st.info("ℹ️ Nenhum histórico disponível para esta empresa.")
    
    # Extratos Pe/SEF
    st.divider()
    st.markdown("### 📰 Extratos Pe/SEF")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Enquadramento")
        df_enq = detalhes.get('extratos_enq', pd.DataFrame())
        if not df_enq.empty:
            for _, ext in df_enq.iterrows():
                st.write(f"• **Termo:** {ext.get('termo_enquadramento', 'N/A')}")
                st.write(f"• **Data:** {formatar_data(ext.get('data_enquadramento'))}")
                st.write(f"• **Pe/SEF:** {ext.get('pesef', 'N/A')}")
                st.write(f"• **Início Efeitos:** {formatar_data(ext.get('inicio_efeitos'))}")
        else:
            st.info("Sem extrato de enquadramento.")
    
    with col2:
        st.markdown("#### Desenquadramento")
        df_desenq = detalhes.get('extratos_desenq', pd.DataFrame())
        if not df_desenq.empty:
            for _, ext in df_desenq.iterrows():
                st.write(f"• **Termo:** {ext.get('termo_desenquadramento', 'N/A')}")
                st.write(f"• **Data:** {formatar_data(ext.get('data_desenquadramento'))}")
                st.write(f"• **Pe/SEF:** {ext.get('pesef_desenquadramento', 'N/A')}")
        else:
            st.info("Sem extrato de desenquadramento.")


def formatar_data(data):
    """Formata data para exibição"""
    if pd.isna(data) or data is None:
        return 'N/A'
    try:
        if isinstance(data, str):
            data = pd.to_datetime(data)
        return data.strftime('%d/%m/%Y')
    except:
        return str(data)[:10]


# =============================================================================
# PÁGINA: SOBRE O SISTEMA
# =============================================================================

def pagina_sobre(dados, engine):
    st.markdown("<h1 class='main-header'>ℹ️ Sobre o Sistema</h1>", unsafe_allow_html=True)
    
    st.markdown("""
    ## Sistema de Monitoramento de Devedores Contumaz - V1.0
    
    ### 📋 Base Legal
    O regime do Devedor Contumaz está previsto no **RICMS/SC, Anexo IV, Capítulo LXX** (Arts. 408 a 413-A).
    
    ### 📊 Critérios de Enquadramento (Art. 408)
    
    **Inciso I:** Deixar de recolher imposto declarado (DIME/ST):
    - Mínimo **8 períodos** de apuração (sucessivos ou não)
    - Nos últimos **12 meses**
    - Valor superior a **R$ 1.000.000,00**
    
    **Inciso II:** Créditos tributários inscritos em dívida ativa:
    - Valor superior a **R$ 20.000.000,00**
    - Considerando todos os estabelecimentos no Estado
    
    ### 📈 Fluxo do Processo no GEP (Gestor Eletrônico de Processos)
    
    ```
    PRÉ-ENQUADRAMENTO (mensal, 2ª quinzena)
            ↓
    INSTAURAÇÃO DO PROCESSO
            ↓
    INTIMAÇÃO (prazo 30 dias)
            ├── QUITOU → Termo de Regularidade → ENCERRADO
            ├── PARCELOU → Termo de Suspensão → SITUAÇÃO 31
            │              (Processo Suspenso)
            │                   ├── Quitou → ENCERRADO
            │                   └── Cancelou parc. → ENQUADRAR
            │
            └── NÃO FEZ NADA → Termo de Enquadramento → SITUAÇÃO 30
                                    ↓
                        ENQUADRADO (aplicar medidas)
                                    ├── Parcelou → SITUAÇÃO 32
                                    │   (Efeitos Suspensos)
                                    │        ├── Quitou → DESENQUADRADO
                                    │        └── Cancelou → RESTAURAR EFEITOS
                                    │
                                    └── Quitou → DESENQUADRADO (40)
    ```
    
    ### 🔴 Situações do Processo GEP
    
    | Código | Situação | Descrição |
    |--------|----------|-----------|
    | 10 | A intimar | Aguardando intimação |
    | 11 | Intimado | Intimação enviada |
    | 12 | Intimado +30 dias | Prazo de 30 dias expirado |
    | 30 | **Enquadrado** | Formalmente enquadrado |
    | 31 | Processo Suspenso | Suspenso ANTES do enquadramento |
    | 32 | Efeito Suspenso | Enquadrado com EFEITOS suspensos |
    | 40 | Desenquadrado | Por regularização |
    | 41 | Desenquadrado | Por ordem judicial |
    
    ### ⚖️ Medidas Aplicáveis (Art. 410)
    
    1. **Impedimento** de benefícios/incentivos fiscais
    2. **Apuração** do ICMS por operação/prestação
    3. **Regime Especial** de Fiscalização (REF)
    
    ---
    
    **Desenvolvido por:** SEF/SC - Secretaria de Estado da Fazenda  
    **Versão:** 1.0 | **Schema:** gecob.dvd_cont_*
    """)
    
    # KPIs resumidos
    st.divider()
    kpis = calcular_kpis_gerais(dados)
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("📋 Processos", formatar_numero(kpis['total_processos']))
    with col2:
        st.metric("🔴 Enquadrados", formatar_numero(kpis['qtd_enquadrados']))
    with col3:
        st.metric("⏸️ Suspensos", formatar_numero(kpis['qtd_suspensos']))
    with col4:
        st.metric("💰 Débito Total", formatar_valor_br(kpis['vl_total']))


# =============================================================================
# NAVEGAÇÃO E FUNÇÃO PRINCIPAL
# =============================================================================

def criar_filtros_sidebar(dados):
    """Cria filtros na sidebar"""
    filtros = {}
    with st.sidebar.expander("🔍 Filtros Globais", expanded=False):
        df_gerfe = dados.get('metricas_gerfe', pd.DataFrame())
        if not df_gerfe.empty:
            gerfes = ['Todas'] + sorted(df_gerfe['gerfe'].dropna().unique().tolist())
            filtros['gerfe'] = st.selectbox("GERFE:", gerfes, key="filtro_gerfe")
    return filtros


def main():
    # Sidebar
    st.sidebar.markdown("""
    <div style='text-align: center; padding: 15px; background: linear-gradient(135deg, #1565C0 0%, #0D47A1 100%); border-radius: 12px; margin-bottom: 15px;'>
        <h2 style='color: white; margin: 0;'>⚖️</h2>
        <p style='color: white; margin: 0; font-size: 0.9rem;'>Devedores Contumaz</p>
        <p style='color: #90CAF9; margin: 0; font-size: 0.7rem;'>SEF/SC - V1.0</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.sidebar.markdown("### 📋 Menu")
    
    # Definição das páginas
    paginas = {
        "📊 Dashboard Executivo": pagina_dashboard_executivo,
        "🚨 Alertas": pagina_alertas,
        "💰 Panorama de Valores": pagina_panorama_valores,
        "🏢 Análise por GERFE": pagina_analise_gerfe,
        "📋 Situação Atual": pagina_situacao_atual,
        "🔍 Drill Down Empresa": pagina_drill_down_empresa,
        "📑 Parcelamentos": pagina_parcelamentos,
        "📨 Comunicações": pagina_comunicacoes,
        "✅ Processos Encerrados": pagina_processos_encerrados,
        "📰 Extratos Pe/SEF": pagina_extratos,
        "ℹ️ Sobre": pagina_sobre,
    }
    
    pagina_sel = st.sidebar.radio("Navegação", list(paginas.keys()), label_visibility="collapsed")
    
    st.sidebar.markdown("---")
    
    # Conexão
    engine = get_impala_engine()
    if engine is None:
        st.error("❌ Falha na conexão com o banco de dados")
        st.stop()
    
    # Carregar dados resumidos
    with st.spinner('⏳ Carregando dados...'):
        dados = carregar_dados_resumo(engine)
    
    if not dados:
        st.error("❌ Falha no carregamento dos dados")
        st.stop()
    
    # Info na sidebar
    kpis = calcular_kpis_gerais(dados)
    st.sidebar.success(f"""
    📊 **Resumo Atual**
    
    📋 {kpis['total_processos']} processos
    🔴 {kpis['qtd_enquadrados']} enquadrados
    💰 {formatar_valor_br(kpis['vl_total'])}
    """)
    
    # Filtros
    filtros = criar_filtros_sidebar(dados)
    
    st.sidebar.markdown("---")
    st.sidebar.caption(f"Atualizado: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    
    # Renderizar página
    try:
        paginas[pagina_sel](dados, engine)
    except Exception as e:
        st.error(f"❌ Erro ao carregar página: {str(e)}")
        with st.expander("🔍 Detalhes do erro"):
            st.exception(e)
    
    # Rodapé
    st.markdown("---")
    st.markdown(
        f"<div style='text-align: center; color: #666; font-size: 0.75rem;'>"
        f"Sistema de Devedores Contumaz V1.0 | SEF/SC | {datetime.now().strftime('%d/%m/%Y %H:%M')}"
        f"</div>", 
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()