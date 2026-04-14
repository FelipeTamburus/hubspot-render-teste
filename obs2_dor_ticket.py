import os
import time
import json
from contexto_ai_client import chamar_contexto_ai
from hubspot_client import (
    buscar_ticket,
    buscar_id_empresa_ej,
    buscar_thread_conversa,
    chat_esta_encerrado,
    buscar_mensagens_chat,
    buscar_emails_ticket,
    buscar_todos_tickets_empresa_30_dias,
    adicionar_observacao,
    obs_ja_criada,
    marcar_obs_criada,
    REMETENTES_BOT
)

TIMEOUT_CHAT_SEGUNDOS = 7200
INTERVALO_VERIFICACAO = 30


def analisar_com_contexto_ai(conteudo_bruto, canal):
    """Usa o Contexto.AI para analisar a dor e contexto do ticket."""
    if not conteudo_bruto or not conteudo_bruto.strip():
        return None

    prompt = f"""Você é um analista sênior de suporte da EasyJur. Analise o conteúdo abaixo de um atendimento recebido via {canal} e retorne APENAS um JSON válido com os campos "dor" e "contexto". Não inclua explicações fora do JSON.

Conteúdo do atendimento:
{conteudo_bruto}

Instruções:
- "dor": Descreva em 2 a 4 frases, na terceira pessoa, o que o cliente está enfrentando. Inclua detalhes específicos (módulo, funcionalidade, erro, impacto). Ignore e-mails e nomes de empresa.
- "contexto": Descreva em 2 a 3 frases o contexto geral: módulo envolvido, urgência percebida, padrão do problema (bug, dúvida, solicitação) e qualquer detalhe que ajude o analista a agir rapidamente.

Formato obrigatório:
{{"dor": "Texto aqui.", "contexto": "Texto aqui."}}"""

    resposta = chamar_contexto_ai(prompt, task_name="analisar_dor_obs2")
    if not resposta:
        return None
    try:
        texto = resposta.replace("```json", "").replace("```", "").strip()
        return json.loads(texto)
    except Exception as e:
        print(f"[obs2] Erro ao processar JSON do Contexto.AI: {e}")
        return None


def ticket_veio_por_chat(ticket):
    source = ticket.get("properties", {}).get("hs_object_source", "")
    subject = ticket.get("properties", {}).get("subject", "")
    return "CHAT" in source.upper() or "bot" in subject.lower()


def ticket_veio_por_bot(ticket):
    subject = ticket.get("properties", {}).get("subject", "")
    return "ticket criado por bot" in subject.lower()


def aguardar_chat_encerrado(thread_id):
    tempo_aguardado = 0
    print(f"[obs2] Aguardando chat da thread {thread_id} ser encerrado...")
    while tempo_aguardado < TIMEOUT_CHAT_SEGUNDOS:
        if chat_esta_encerrado(thread_id):
            print(f"[obs2] Chat encerrado após {tempo_aguardado}s.")
            return True
        time.sleep(INTERVALO_VERIFICACAO)
        tempo_aguardado += INTERVALO_VERIFICACAO
        print(f"[obs2] Chat ainda aberto... ({tempo_aguardado}s aguardados)")
    print(f"[obs2] Timeout de {TIMEOUT_CHAT_SEGUNDOS}s. Processando com o que há.")
    return False


def extrair_conteudo_chat(thread_id):
    mensagens = buscar_mensagens_chat(thread_id)
    mensagens_cliente = []
    for msg in mensagens:
        if msg.get("type") not in ["MESSAGE", "WELCOME_MESSAGE"]:
            continue
        remetente = msg.get("senders", [{}])[0]
        nome = remetente.get("name", "")
        if nome in REMETENTES_BOT:
            continue
        creator = msg.get("createdBy", "")
        if creator.startswith("V-"):
            texto = msg.get("text", "").strip()
            if texto:
                mensagens_cliente.append(texto)
    return "\n".join(mensagens_cliente) if mensagens_cliente else None


def extrair_conteudo_email(ticket_id):
    emails = buscar_emails_ticket(ticket_id)
    emails_cliente = [
        e for e in emails
        if e.get("properties", {}).get("hs_email_direction") == "INCOMING_EMAIL"
    ]
    if not emails_cliente:
        return None
    emails_cliente.sort(key=lambda e: e.get("properties", {}).get("hs_createdate", ""))
    return emails_cliente[0].get("properties", {}).get("hs_email_text", "").strip()


TITULOS_GENERICOS = [
    "ticket criado por bot",
    "ticket criado pelo bot",
    "sem título",
    "sem assunto",
    "atendimento",
    "suporte",
    "ajuda",
    "dúvida",
    "problema",
]


def titulo_e_generico(subject):
    """Verifica se o título do ticket é genérico."""
    if not subject:
        return True
    return subject.strip().lower() in TITULOS_GENERICOS


def gerar_titulo_personalizado(conteudo_bruto, analise):
    """Usa o Contexto.AI para gerar um título curto, objetivo e focado no problema."""
    contexto = ""
    if analise:
        contexto = analise.get("dor", "") or analise.get("contexto", "")

    texto_base = contexto or conteudo_bruto or ""
    if not texto_base:
        return None

    prompt = f"""Você é um especialista em suporte técnico. Com base na descrição abaixo de um ticket de suporte, crie um título com no máximo 60 caracteres.

Regras obrigatórias:
- Foco total no PROBLEMA do cliente (o que está errado ou o que ele precisa)
- Inclua o módulo ou funcionalidade afetada quando identificável
- Linguagem direta e objetiva, sem rodeios
- Sem palavras genéricas como "dúvida", "problema", "ajuda", "suporte"
- Sem aspas, sem pontuação no final, sem explicações adicionais
- Responda APENAS com o título

Exemplos de títulos bons:
- Erro na emissão de nota fiscal no módulo financeiro
- Captura de intimações não está localizando processos
- CPF não aceito no cadastro de termos monitorados
- Relatório de honorários com valores incorretos

Descrição do ticket: {texto_base[:600]}"""

    try:
        titulo = chamar_contexto_ai(prompt, task_name="gerar_titulo_ticket")
        if titulo:
            titulo = titulo.strip().strip('"').strip("'")[:60]
            return titulo
    except Exception as e:
        print(f"[obs2] Erro ao gerar título: {e}")
    return None


def atualizar_titulo_ticket(ticket_id, novo_titulo):
    """Atualiza o subject do ticket no HubSpot."""
    import requests as req
    import os
    ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN_HUBSPOT")
    url = f"https://api.hubapi.com/crm/v3/objects/tickets/{ticket_id}"
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"}
    try:
        response = req.patch(url, headers=headers, json={"properties": {"subject": novo_titulo}}, timeout=10)
        response.raise_for_status()
        print(f"[obs2] ✅ Título atualizado para: '{novo_titulo}'")
        return True
    except Exception as e:
        print(f"[obs2] Erro ao atualizar título do ticket {ticket_id}: {e}")
        return False


def gerar_html_obs2(company_ej_id, total_tickets, canal, chat_encerrado, analise):
    html = "<p>🤖 <strong>[IA] ANÁLISE DO TICKET</strong></p><hr>"

    if company_ej_id and total_tickets is not None:
        html += f"<p>🏢 <strong>Tickets abertos pela empresa (últimos 30 dias):</strong> {total_tickets}</p>"
    else:
        html += "<p>🏢 <strong>Empresa:</strong> Não identificada neste ticket.</p>"

    canal_emoji = "💬" if "Chat" in canal else "📧"
    html += f"<p>{canal_emoji} <strong>Canal de entrada:</strong> {canal}</p>"

    if chat_encerrado is not None:
        status = "Chat encerrado pelo cliente ✅" if chat_encerrado else "Chat encerrado por timeout (2h) ⏱️"
        html += f"<p>🔔 <strong>Status do chat:</strong> {status}</p>"

    html += "<hr>"

    if analise:
        dor = analise.get("dor", "")
        contexto = analise.get("contexto", "")
        if dor:
            html += f"<p>😣 <strong>Dor relatada pelo cliente:</strong></p><p>{dor}</p>"
        if contexto:
            html += f"<p>📋 <strong>Resumo e contexto:</strong></p><p>{contexto}</p>"
    else:
        html += "<p>⚠️ <strong>Análise:</strong> Não foi possível analisar o conteúdo deste ticket.</p>"

    return html


def processar_obs2(ticket_id, forcar=False):
    print(f"[obs2] Iniciando para ticket {ticket_id}...")
    print(f"[obs2] Aguardando 120 segundos...")
    time.sleep(120)

    # Verifica se já foi criada para evitar duplicatas
    if obs_ja_criada(ticket_id, 2):
        print(f"[obs2] Obs 2 já criada para ticket {ticket_id}. Pulando.")
        return True

    ticket = buscar_ticket(ticket_id)
    if not ticket:
        print(f"[obs2] Ticket {ticket_id} não encontrado. Abortando.")
        return False

    props = ticket.get("properties", {})

    # Usa id_empresa_ej para buscar tickets da empresa (mesma fonte que Obs 1)
    company_ej_id = buscar_id_empresa_ej(ticket_id)

    total_tickets = None
    if company_ej_id:
        tickets_30_dias = buscar_todos_tickets_empresa_30_dias(company_ej_id)
        total_tickets = len(tickets_30_dias)
        print(f"[obs2] {total_tickets} tickets encontrados para empresa EJ {company_ej_id}.")

    veio_por_bot = ticket_veio_por_bot(ticket)
    veio_por_chat = ticket_veio_por_chat(ticket)
    canal = "Chat (via Bot)" if veio_por_bot else "Chat" if veio_por_chat else "Formulário/E-mail"

    conteudo_bruto = None
    chat_encerrado = None

    if veio_por_chat or veio_por_bot:
        thread_id = buscar_thread_conversa(ticket_id)
        if thread_id:
            if forcar:
                print(f"[obs2] Forçando processamento sem aguardar chat fechar.")
                chat_encerrado = True
            elif not chat_esta_encerrado(thread_id):
                chat_encerrado = aguardar_chat_encerrado(thread_id)
            else:
                chat_encerrado = True
                print(f"[obs2] Chat já estava encerrado.")
            conteudo_bruto = extrair_conteudo_chat(thread_id)
        if not conteudo_bruto:
            conteudo_bruto = props.get("content", "").strip() or None
    else:
        conteudo_bruto = extrair_conteudo_email(ticket_id)
        if not conteudo_bruto:
            conteudo_bruto = props.get("content", "").strip() or None

    analise = None
    if conteudo_bruto:
        print(f"[obs2] Analisando conteúdo com Contexto.AI...")
        analise = analisar_com_contexto_ai(conteudo_bruto, canal)

    conteudo_html = gerar_html_obs2(company_ej_id, total_tickets, canal, chat_encerrado, analise)
    sucesso = adicionar_observacao(ticket_id, "Observação 2 — Contexto e Dor do Ticket", conteudo_html)

    if sucesso:
        marcar_obs_criada(ticket_id, 2)
        print(f"[obs2] ✅ Observação 2 adicionada ao ticket {ticket_id}.")

        # Sempre gera e atualiza o título com base na análise da dor
        print(f"[obs2] Gerando título personalizado para ticket {ticket_id}...")
        novo_titulo = gerar_titulo_personalizado(conteudo_bruto, analise)
        if novo_titulo:
            atualizar_titulo_ticket(ticket_id, novo_titulo)
        else:
            print(f"[obs2] Não foi possível gerar título para ticket {ticket_id}.")
    else:
        print(f"[obs2] ❌ Falha ao adicionar Observação 2 ao ticket {ticket_id}.")
    return sucesso
