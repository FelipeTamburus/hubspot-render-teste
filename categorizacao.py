import os
import json
import time
from contexto_ai_client import chamar_contexto_ai
from hubspot_client import (
    buscar_ticket,
    buscar_company_id,
    buscar_plano_empresa,
    buscar_plano_do_ticket,
    buscar_thread_conversa,
    chat_esta_encerrado,
    buscar_mensagens_chat,
    buscar_emails_ticket,
    REMETENTES_BOT
)

# --- MAPEAMENTO DE COLUNAS ---
COLUNAS_PIPELINE = {
    "URGENT": "1338631792",
    "HIGH":   "1338631792",
    "MEDIUM": "1338631793",
    "LOW":    "1338631794"
}

TIMEOUT_CHAT_SEGUNDOS = 86400
INTERVALO_VERIFICACAO = 30

# --- ESCALA DE PRIORIDADE (para ajuste por tipo) ---
ESCALA = ["LOW", "MEDIUM", "HIGH", "URGENT"]


def descer_prioridade(prioridade, niveis):
    """Desce a prioridade N níveis na escala."""
    idx = ESCALA.index(prioridade) if prioridade in ESCALA else 0
    novo_idx = max(0, idx - niveis)
    return ESCALA[novo_idx]


# --- AJUSTE POR TIPO DE PROBLEMA ---
AJUSTE_TIPO = {
    "falha_critica": 0,   # mantém prioridade
    "bug":           1,   # desce 1 nível
    "duvida":        2,   # desce 2 níveis
    "configuracao":  2,   # desce 2 níveis
    "sugestao":      3,   # desce 3 níveis (quase sempre LOW)
}

TIPO_DESCRICAO = {
    "falha_critica": "Falha crítica — sistema não funciona ou dados comprometidos",
    "bug":           "Bug — comportamento incorreto, sistema ainda funciona",
    "duvida":        "Dúvida — pergunta sobre uso de funcionalidade",
    "configuracao":  "Configuração — ajuste de preferência ou setup",
    "sugestao":      "Sugestão — pedido de melhoria ou nova feature",
}


# --- MATRIZ DE DECISÃO (módulo × plano) ---
MATRIZ_PRIORIDADE = {
    "juridico": {
        "Growth+": "URGENT", "Growth": "URGENT",
        "Standard": "HIGH",  "Premium": "HIGH", "Starter": "MEDIUM",
        "Trial": "HIGH", "Convênio": "MEDIUM", "EasyCall": "MEDIUM",
        "Enterprise": "HIGH", "legal_crm": "HIGH"
    },
    "financeiro": {
        "Growth+": "URGENT", "Growth": "HIGH",
        "Standard": "MEDIUM", "Premium": "MEDIUM", "Starter": "MEDIUM",
        "Trial": "MEDIUM", "Convênio": "MEDIUM", "EasyCall": "LOW",
        "Enterprise": "HIGH", "legal_crm": "MEDIUM"
    },
    "documentos": {
        "Growth+": "HIGH",   "Growth": "MEDIUM",
        "Standard": "MEDIUM", "Premium": "LOW", "Starter": "LOW",
        "Trial": "LOW", "Convênio": "LOW", "EasyCall": "LOW",
        "Enterprise": "MEDIUM", "legal_crm": "LOW"
    },
    "gestao": {
        "Growth+": "MEDIUM", "Growth": "MEDIUM",
        "Standard": "LOW",   "Premium": "LOW", "Starter": "LOW",
        "Trial": "LOW", "Convênio": "LOW", "EasyCall": "LOW",
        "Enterprise": "MEDIUM", "legal_crm": "LOW"
    },
    "configuracoes": {
        "Growth+": "MEDIUM", "Growth": "LOW",
        "Standard": "LOW",   "Premium": "LOW", "Starter": "LOW",
        "Trial": "LOW", "Convênio": "LOW", "EasyCall": "LOW",
        "Enterprise": "LOW", "legal_crm": "LOW"
    },
    "produtos_ia": {
        "Growth+": "LOW", "Growth": "LOW",
        "Standard": "LOW", "Premium": "LOW", "Starter": "LOW",
        "Trial": "LOW", "Convênio": "LOW", "EasyCall": "LOW",
        "Enterprise": "LOW", "legal_crm": "LOW"
    }
}

MODULOS_KEYWORDS = {
    "juridico": [
        "processo", "processual", "intimação", "intimações", "publicação", "publicações",
        "prazo", "prazos", "agenda", "movimentação", "movimentações", "consultivo",
        "jurídico", "juridico", "pessoa", "projeto", "andamento", "distribuição",
        "captura", "monitoramento", "diário oficial"
    ],
    "financeiro": [
        "financeiro", "receita", "despesa", "fluxo de caixa", "conta bancária",
        "dre", "provisionamento", "asaas", "honorário", "honorários", "fatura",
        "cobrança", "pagamento", "financeira", "lançamento", "centro de custo"
    ],
    "documentos": [
        "documento", "documentos", "modelo", "modelos", "tag", "jurisprudência",
        "biblioteca", "ged", "digital", "arquivo", "arquivos", "contrato", "minuta"
    ],
    "gestao": [
        "dashboard", "analytics", "relatório", "relatórios", "automação",
        "automações", "negócio", "negócios", "legal analytics", "indicador", "meta"
    ],
    "configuracoes": [
        "configuração", "configurações", "acesso", "usuário", "usuários",
        "cargo", "cargos", "equipe", "equipes", "centro de custo", "grupo",
        "grupos", "campo personalizado", "senioridade", "segurança", "cadastro",
        "senha", "permissão", "perfil"
    ],
    "produtos_ia": [
        "jurisai", "smartdocs", "controladoria inteligente", "análise de publicações",
        "copilot", "ia", "inteligência artificial", "smart", "juris ai"
    ]
}


def detectar_modulo_por_keywords(conteudo):
    """Pré-detecção do módulo por palavras-chave."""
    conteudo_lower = conteudo.lower()
    scores = {}
    for modulo, keywords in MODULOS_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in conteudo_lower)
        if score > 0:
            scores[modulo] = score
    return max(scores, key=scores.get) if scores else None


def ticket_e_chat(ticket):
    """Verifica se o ticket veio por canal de chat."""
    source = ticket.get("properties", {}).get("hs_object_source", "")
    subject = ticket.get("properties", {}).get("subject", "")
    return "CHAT" in source.upper() or "bot" in subject.lower()


def aguardar_chat_encerrado(thread_id):
    """
    Aguarda o chat ser encerrado com timeout de 24h.
    Verifica status + inatividade de 5h (mesma regra do worker_chat).
    """
    tempo_aguardado = 0
    print(f"[categ] Aguardando chat {thread_id} ser encerrado...")
    while tempo_aguardado < TIMEOUT_CHAT_SEGUNDOS:
        if chat_esta_encerrado(thread_id):
            print(f"[categ] Chat encerrado após {tempo_aguardado}s.")
            return True
        time.sleep(INTERVALO_VERIFICACAO)
        tempo_aguardado += INTERVALO_VERIFICACAO
        if tempo_aguardado % 300 == 0:
            print(f"[categ] Chat ainda aberto ({tempo_aguardado/60:.0f}min aguardados)...")
    print(f"[categ] Timeout de 24h. Categorizando com o que há.")
    return False


def extrair_conteudo_ticket(ticket_id, ticket):
    """Extrai o conteúdo completo do ticket de qualquer canal."""
    props = ticket.get("properties", {})
    textos = []

    for campo in ["subject", "content", "demanda_apresentada_pelo_cliente"]:
        valor = props.get(campo, "")
        if valor:
            textos.append(valor)

    thread_id = buscar_thread_conversa(ticket_id)
    if thread_id:
        mensagens = buscar_mensagens_chat(thread_id)
        for msg in mensagens:
            if msg.get("type") not in ["MESSAGE", "WELCOME_MESSAGE"]:
                continue
            remetente = msg.get("senders", [{}])[0]
            if remetente.get("name", "") in REMETENTES_BOT:
                continue
            if msg.get("createdBy", "").startswith("V-"):
                texto = msg.get("text", "").strip()
                if texto:
                    textos.append(texto)

    emails = buscar_emails_ticket(ticket_id)
    for email in emails:
        if email.get("properties", {}).get("hs_email_direction") == "INCOMING_EMAIL":
            texto = email.get("properties", {}).get("hs_email_text", "").strip()
            if texto:
                textos.append(texto[:500])

    return "\n".join(filter(None, textos))


def categorizar_com_contexto_ai(conteudo, plano, modulo_sugerido=None):
    """
    Chama o Contexto.AI para identificar módulo, tipo de problema e prioridade.
    O ajuste final de prioridade é feito em Python pela lógica de tipo.
    """
    plano_desc = plano or "Não identificado"
    sugestao = f"\nSugestão de módulo por palavras-chave: {modulo_sugerido}" if modulo_sugerido else ""

    prompt = f"""Você é um especialista em triagem de tickets de suporte da EasyJur. Analise o conteúdo abaixo e retorne APENAS um JSON válido. Não inclua explicações fora do JSON.

Plano do cliente: {plano_desc}{sugestao}

MÓDULOS disponíveis:
- juridico: Processos, Intimações, Prazos, Publicações, Movimentações, Andamentos
- financeiro: Receitas, Despesas, Fluxo de Caixa, DRE, Asaas, Lançamentos
- documentos: GED, Modelos, Jurisprudência, Biblioteca Digital, Contratos
- gestao: Dashboards, Analytics, Relatórios, Automações
- configuracoes: Usuários, Cargos, Permissões, Campos, Grupos
- produtos_ia: JurisAI, SmartDocs, Controladoria Inteligente

TIPOS DE PROBLEMA:
- falha_critica: sistema não funciona, dados perdidos, processo não capturado, erro que impede trabalho
- bug: comportamento incorreto mas sistema ainda funciona parcialmente
- duvida: pergunta sobre como usar uma funcionalidade, como funciona algo
- configuracao: ajuste de configuração, preferência, setup inicial
- sugestao: pedido de melhoria, nova feature, sugestão

Conteúdo do ticket:
{conteudo[:1500]}

Retorne APENAS este JSON:
{{
  "modulo": "juridico|financeiro|documentos|gestao|configuracoes|produtos_ia",
  "tipo_problema": "falha_critica|bug|duvida|configuracao|sugestao",
  "prioridade_base": "LOW|MEDIUM|HIGH|URGENT",
  "justificativa": "Uma frase explicando módulo, tipo e decisão."
}}"""

    resposta = chamar_contexto_ai(prompt, task_name="categorizar_ticket")
    if not resposta:
        return None

    try:
        texto = resposta.replace("```json", "").replace("```", "").strip()
        return json.loads(texto)
    except Exception as e:
        print(f"[categ] Erro ao processar JSON: {e}")
        return None


def calcular_prioridade_final(prioridade_base, tipo_problema):
    """
    Aplica o ajuste de tipo sobre a prioridade base.
    Uma dúvida sempre desce na escala, uma falha crítica mantém.
    """
    niveis = AJUSTE_TIPO.get(tipo_problema, 0)
    prioridade_final = descer_prioridade(prioridade_base, niveis)

    if niveis > 0:
        print(f"[categ] Ajuste por tipo '{tipo_problema}': {prioridade_base} → {prioridade_final} (-{niveis} nível{'s' if niveis > 1 else ''})")
    else:
        print(f"[categ] Tipo '{tipo_problema}': prioridade mantida em {prioridade_final}")

    return prioridade_final


def aplicar_matriz_fallback(modulo, plano):
    """Fallback: usa a matriz de decisão se o Contexto.AI falhar."""
    modulo_key = modulo.lower() if modulo else "configuracoes"
    plano_key = plano if plano else "Premium"
    matriz_modulo = MATRIZ_PRIORIDADE.get(modulo_key, MATRIZ_PRIORIDADE["configuracoes"])
    return matriz_modulo.get(plano_key, "LOW")


def atualizar_ticket_hubspot(ticket_id, prioridade, stage_id):
    """Atualiza prioridade e move o ticket para a coluna correta."""
    import requests as req
    token = os.environ.get("ACCESS_TOKEN_HUBSPOT")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    url = f"https://api.hubapi.com/crm/v3/objects/tickets/{ticket_id}"
    payload = {
        "properties": {
            "hs_ticket_priority": prioridade,
            "hs_pipeline_stage": stage_id
        }
    }
    try:
        response = req.patch(url, headers=headers, json=payload, timeout=15)
        response.raise_for_status()
        print(f"[categ] ✅ Ticket {ticket_id} → prioridade={prioridade}, coluna={stage_id}")
        return True
    except Exception as e:
        print(f"[categ] ❌ Erro ao atualizar ticket {ticket_id}: {e}")
        return False


def processar_categorizacao(ticket_id, forcar=False):
    """
    Função principal de categorização.
    1. Aguarda 30s para o HubSpot propagar propriedades (ex: plano_contratado_easyjur)
    2. Aguarda chat fechar se necessário (pulado se forcar=True)
    3. Extrai conteúdo completo
    4. Identifica módulo + tipo + prioridade base via Contexto.AI
    5. Aplica ajuste por tipo de problema
    6. Atualiza HubSpot e move para coluna correta
    """
    print(f"[categ] Iniciando categorização do ticket {ticket_id}...")
    print(f"[categ] Aguardando 30s para propagação das propriedades no HubSpot...")
    time.sleep(30)

    ticket = buscar_ticket(ticket_id)
    if not ticket:
        print(f"[categ] Ticket {ticket_id} não encontrado. Abortando.")
        return False

    # Aguarda chat fechar se necessário (pula se forcar=True)
    e_chat = ticket_e_chat(ticket)
    if e_chat and not forcar:
        thread_id = buscar_thread_conversa(ticket_id)
        if thread_id and not chat_esta_encerrado(thread_id):
            aguardar_chat_encerrado(thread_id)
    elif e_chat and forcar:
        print(f"[categ] Forçando categorização sem aguardar chat fechar.")

    # Extrai conteúdo completo
    conteudo = extrair_conteudo_ticket(ticket_id, ticket)
    if not conteudo:
        print(f"[categ] Conteúdo vazio. Usando LOW como padrão.")
        atualizar_ticket_hubspot(ticket_id, "LOW", COLUNAS_PIPELINE["LOW"])
        return True

    # Busca plano da empresa
    company_id = buscar_company_id(ticket_id)
    plano = buscar_plano_do_ticket(ticket_id)
    print(f"[categ] Plano: {plano or 'Não identificado'}")

    # Pré-detecção por palavras-chave
    modulo_sugerido = detectar_modulo_por_keywords(conteudo)
    if modulo_sugerido:
        print(f"[categ] Módulo sugerido por keywords: {modulo_sugerido}")

    # Categoriza com Contexto.AI
    resultado = categorizar_com_contexto_ai(conteudo, plano, modulo_sugerido)

    if resultado:
        modulo = resultado.get("modulo", modulo_sugerido or "configuracoes")
        tipo_problema = resultado.get("tipo_problema", "duvida")
        prioridade_base = resultado.get("prioridade_base", "LOW")
        justificativa = resultado.get("justificativa", "")

        print(f"[categ] Módulo: {modulo} | Tipo: {tipo_problema} | Prioridade base: {prioridade_base}")
        print(f"[categ] Justificativa: {justificativa}")

        # Aplica ajuste por tipo de problema
        prioridade_final = calcular_prioridade_final(prioridade_base, tipo_problema)

    else:
        # Fallback completo via matriz + assume dúvida para ser conservador
        print(f"[categ] Contexto.AI falhou. Usando matriz de decisão + tipo=duvida.")
        modulo = modulo_sugerido or "configuracoes"
        prioridade_base = aplicar_matriz_fallback(modulo, plano)
        prioridade_final = calcular_prioridade_final(prioridade_base, "duvida")

    # Garante prioridade válida
    if prioridade_final not in COLUNAS_PIPELINE:
        prioridade_final = "LOW"

    stage_id = COLUNAS_PIPELINE[prioridade_final]

    # Para tickets de chat: verifica se ainda está no estágio Novo antes de mover
    # Se o analista já moveu o ticket, não altera coluna nem prioridade
    if e_chat:
        ticket_atual = buscar_ticket(ticket_id)
        estagio_atual = ticket_atual.get("properties", {}).get("hs_pipeline_stage", "") if ticket_atual else ""
        if estagio_atual != "1":
            print(f"[categ] Ticket {ticket_id} de chat já foi movido pelo analista (estágio atual: {estagio_atual}). Não alterando coluna.")
            return True

    # Atualiza no HubSpot
    sucesso = atualizar_ticket_hubspot(ticket_id, prioridade_final, stage_id)

    if sucesso:
        print(f"[categ] ✅ Ticket {ticket_id} categorizado: {prioridade_final} → coluna {stage_id}")
    else:
        print(f"[categ] ❌ Falha ao categorizar ticket {ticket_id}.")

    return sucesso
