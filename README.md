# Sistema de Monitoramento de Devedores Contumaz - DVD CONT V1.0

Sistema web de monitoramento e gestão de devedores contumaz desenvolvido para a **Secretaria de Estado da Fazenda de Santa Catarina (SEF/SC)**.

## Sobre o Projeto

O DVD (Devedores Contumaz) é uma aplicação de dashboard executivo que permite o acompanhamento e monitoramento de empresas com débitos tributários em situação de inadimplência. O sistema oferece visualizações analíticas, alertas, drill-down por empresa e gestão de processos de cobrança.

## Tecnologias Utilizadas

| Tecnologia | Descrição |
|------------|-----------|
| **Python 3.x** | Linguagem de programação principal |
| **Streamlit** | Framework web para criação de dashboards interativos |
| **SQLAlchemy** | ORM para conexão e queries no banco de dados |
| **Impala** | Data warehouse para armazenamento e consulta de dados |
| **Pandas** | Manipulação e análise de dados |
| **NumPy** | Computação numérica |
| **Plotly** | Visualização de dados com gráficos interativos |

## Funcionalidades Principais

- **Dashboard Executivo**: Visão geral com KPIs e métricas principais
- **Sistema de Alertas**: Monitoramento de alertas com níveis de criticidade (URGENTE, CRÍTICO, ATENÇÃO)
- **Análise por GERFE**: Comparativos entre gerências regionais da fazenda
- **Drill Down por Empresa**: Detalhamento completo de cada contribuinte
- **Gestão de Parcelamentos**: Acompanhamento de acordos de parcelamento
- **Comunicações**: Histórico de intimações e notificações
- **Processos Encerrados**: Registro de casos finalizados
- **Extratos**: Consulta de extratos e enquadramentos

## Estrutura do Projeto

```
CONTUMAZ_NEW/
├── DVD.py              # Aplicação principal (arquivo único)
├── README.md           # Documentação do projeto
└── .streamlit/
    └── secrets.toml    # Configurações de credenciais (não versionado)
```

## Requisitos

### Dependências Python

```txt
streamlit>=1.28.0
pandas>=2.0.0
numpy>=1.24.0
plotly>=5.15.0
sqlalchemy>=2.0.0
impyla>=0.18.0
```

### Instalação das Dependências

```bash
pip install streamlit pandas numpy plotly sqlalchemy impyla
```

## Configuração

### 1. Configuração de Credenciais

Crie o arquivo `.streamlit/secrets.toml` com as credenciais de acesso ao banco de dados:

```toml
[impala]
user = "seu_usuario_ldap"
password = "sua_senha_ldap"
```

### 2. Conexão com Banco de Dados

O sistema conecta-se ao Impala através de autenticação LDAP com SSL:
- **Host**: `bdaworkernode02.sef.sc.gov.br`
- **Porta**: `21050`
- **Database**: `gecob`
- **Autenticação**: LDAP

## Execução

### Executar Localmente

```bash
streamlit run DVD.py
```

### Executar com Porta Específica

```bash
streamlit run DVD.py --server.port 8501
```

### Acessar a Aplicação

Após iniciar, acesse no navegador:
```
http://localhost:8501
```

## Páginas do Sistema

| Página | Descrição |
|--------|-----------|
| **Dashboard Executivo** | Visão consolidada com KPIs principais, gráficos de evolução e resumo executivo |
| **Alertas** | Painel de alertas categorizados por criticidade e tipo |
| **Panorama de Valores** | Análise da composição de valores e débitos |
| **Análise por GERFE** | Comparativo entre gerências regionais com rankings e métricas |
| **Situação Atual** | Lista detalhada de todos os processos ativos |
| **Drill Down Empresa** | Consulta detalhada por CNPJ/Razão Social com histórico completo |
| **Parcelamentos** | Gestão e acompanhamento de acordos de parcelamento |
| **Comunicações** | Histórico de intimações, notificações e comunicados |
| **Processos Encerrados** | Registro de casos finalizados e seus desfechos |
| **Extratos** | Consulta de extratos PE/SEF e enquadramentos |
| **Sobre** | Informações sobre o sistema e equipe |

## Indicadores (KPIs)

O sistema monitora os seguintes indicadores principais:

| KPI | Descrição | Cor |
|-----|-----------|-----|
| **Enquadrados** | Devedores ativos enquadrados | Vermelho |
| **Suspensos** | Processos com suspensão | Laranja |
| **Efeito Suspensivo** | Com efeito suspensivo ativo | Amarelo |
| **A Intimar** | Aguardando intimação | Azul |
| **Intimado** | Já intimados | Azul |

## Cache e Performance

O sistema utiliza cache do Streamlit para otimizar performance:

| Tipo de Dado | TTL (Time To Live) |
|--------------|-------------------|
| Dados de resumo | 1 hora |
| Dados detalhados | 30 minutos |

## Segurança

- Acesso protegido por senha
- Conexão com banco via SSL
- Autenticação LDAP integrada
- Credenciais gerenciadas via Streamlit Secrets

## Estrutura de Código

```
DVD.py
├── Autenticação
│   └── check_password()
├── Conexão com Banco
│   └── get_impala_engine()
├── Carregamento de Dados
│   ├── carregar_dados_resumo()
│   ├── carregar_situacao_atual()
│   ├── carregar_processos_encerrados()
│   ├── carregar_parcelamentos()
│   ├── carregar_alertas_detalhados()
│   ├── carregar_detalhes_empresa()
│   ├── carregar_comunicacoes()
│   └── carregar_extratos_enquadramentos()
├── Funções Utilitárias
│   ├── formatar_valor_br()
│   ├── formatar_numero()
│   ├── calcular_kpis_gerais()
│   └── criar_kpi_card()
├── Páginas
│   ├── pagina_dashboard_executivo()
│   ├── pagina_panorama_valores()
│   ├── pagina_analise_gerfe()
│   ├── pagina_alertas()
│   ├── pagina_situacao_atual()
│   ├── pagina_parcelamentos()
│   ├── pagina_comunicacoes()
│   ├── pagina_processos_encerrados()
│   ├── pagina_extratos()
│   ├── pagina_drill_down_empresa()
│   └── pagina_sobre()
└── main()
```

## Formatação de Valores

O sistema utiliza formatação brasileira para valores monetários:
- Separador de milhar: ponto (.)
- Separador decimal: vírgula (,)
- Símbolo monetário: R$

Exemplo: `R$ 1.234.567,89`

## Desenvolvimento

### Autor
Secretaria de Estado da Fazenda de Santa Catarina - SEF/SC

### Versão
1.0 (DVD CONT V1.0)

## Suporte

Para suporte técnico ou dúvidas sobre o sistema, entre em contato com a equipe de TI da SEF/SC.

---

**Secretaria de Estado da Fazenda de Santa Catarina**
*Sistema de Monitoramento de Devedores Contumaz*
