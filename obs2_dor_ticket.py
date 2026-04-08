import os
import time
import json
from google import genai
from google.genai import types
from hubspot_client import (
    buscar_ticket,
    buscar_company_id,
    buscar_thread_conversa,
    chat_esta_encerrado,
    buscar_mensagens_chat,
    buscar_emails_ticket,
    buscar_todos_tickets_empresa_30_dias,
    adicionar_observacao,
    REMETENTES_BOT
)

TIMEOUT_CHAT_SEGUNDOS = 7200
INTERVALO_VERIFICACAO = 30
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")


# --- GEMINI ---

def configurar_gemini():
    keys_str = os.environ.get("GEMINI_API_KEYS", "")
    keys = [k.strip() for k in keys_str.split(",") if k.strip()]
    if not keys:
        print("[obs2] ERRO: Nenhuma chave Gemini encontrada.")
        return None
    return genai.Client(api_key=keys[0])


def analisar_com_gemini(client, conteudo_bruto, canal):
    """
    Usa o Gemini para analisar o conteúdo do ticket e gerar
    uma análise rica com dor do cliente e contexto completo.
    """
    if not conteudo_bruto or not conteudo_bruto.strip():
        return None

    prompt = f"""
Você é um analista sênior de suporte especialista em entender profundamente a dor e o contexto de clientes.

Canal de entrada: {canal}

Conteúdo bruto do atendimento:
\"\"\"{conteudo_bruto}\"\"\"

Analise o conteúdo acima com profundidade e gere:

1. "dor": Descreva de forma clara, completa e enriquecida o que o cliente está enfrentando ou precisando. 
   - Escreva na terceira pessoa (ex: "O cliente relata...", "O cliente apresenta dificuldade com...")
   - Inclua detalhes específicos mencionados pelo cliente (módulo, funcionalidade, erro, impacto no trabalho)
   - Se o cliente mencionou urgência ou impacto no negócio, destaque isso
   - Ignore apenas informações de contato como e-mail e nome da empresa
   - Seja completo: o analista deve entender o problema sem precisar ler o ticket original

2. "contexto": Forneça um resumo do contexto geral da situação com informações enriquecedoras:
   - Qual módulo ou funcionalidade está envolvida
   - Se há indícios de urgência ou impacto crítico no negócio do cliente
   - Padrão de comportamento identificado (ex: erro recorrente, dúvida de uso, solicitação de feature)
   - Qualquer detalhe adicional que ajude o analista a agir com mais assertividade e rapidez

Responda APENAS com um JSON no formato:
{{
  "dor": "Texto completo e enriquecido da dor aqui.",
  "contexto": "Texto completo do contexto enriquecido aqui."
}}
"""
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json"
            )
        )
        texto = response.text.strip().replace("```json", "").replace("```", "")
        return json.loads(texto)
    except Exception as e:
        print(f"[obs2] Erro ao chamar Gemini: {e}")
        return None


# --- CANAL E CONTEÚDO ---

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
    emails_cliente.sort(
        key=lambda e: e.get("properties", {}).get("hs_createdate", ""),
        reverse=False
    )
    return emails_cliente[0].get("properties", {}).get("hs_email_text", "").strip()


# --- HTML ---

def gerar_html_obs2(company_id, total_tickets, canal, chat_encerrado, analise):
    """Gera o HTML da Observação 2 com emojis e análise enriquecida do Gemini."""

    # Cabeçalho
    html = "<p>🤖 <strong>[IA] ANÁLISE DO TICKET</strong></p><hr>"

    # Informações da empresa
    if company_id and total_tickets is not None:
        html += f"<p>🏢 <strong>Tickets abertos pela empresa (últimos 30 dias):</strong> {total_tickets}</p>"
    else:
        html += "<p>🏢 <strong>Empresa:</strong> Não identificada neste ticket.</p>"

    # Canal
    canal_emoji = "💬" if "Chat" in canal else "📧"
    html += f"<p>{canal_emoji} <strong>Canal de entrada:</strong> {canal}</p>"

    # Status do chat
    if chat_encerrado is not None:
        status = "Chat encerrado pelo cliente ✅" if chat_encerrado else "Chat encerrado por timeout (2h) ⏱️"
        html += f"<p>🔔 <strong>Status do chat:</strong> {status}</p>"

    html += "<hr>"

    # Dor e contexto analisados pelo Gemini
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


# --- PRINCIPAL ---

def processar_obs2(ticket_id):
    print(f"[obs2] Iniciando para ticket {ticket_id}...")
    print(f"[obs2] Aguardando 120 segundos...")
    time.sleep(120)

    client = configurar_gemini()

    ticket = buscar_ticket(ticket_id)
    if not ticket:
        print(f"[obs2] Ticket {ticket_id} não encontrado. Abortando.")
        return False

    props = ticket.get("properties", {})
    company_id = buscar_company_id(ticket_id)

    total_tickets = None
    if company_id:
        tickets_30_dias = buscar_todos_tickets_empresa_30_dias(company_id)
        total_tickets = len(tickets_30_dias)

    veio_por_chat = ticket_veio_por_chat(ticket)
    veio_por_bot = ticket_veio_por_bot(ticket)
    canal = "Chat (via Bot)" if veio_por_bot else "Chat" if veio_por_chat else "Formulário/E-mail"

    conteudo_bruto = None
    chat_encerrado = None

    if veio_por_chat or veio_por_bot:
        thread_id = buscar_thread_conversa(ticket_id)
        if thread_id:
            if not chat_esta_encerrado(thread_id):
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
    if client and conteudo_bruto:
        print(f"[obs2] Analisando conteúdo com Gemini...")
        analise = analisar_com_gemini(client, conteudo_bruto, canal)
    elif not client:
        print(f"[obs2] Gemini não configurado. Pulando análise.")

    conteudo_html = gerar_html_obs2(company_id, total_tickets, canal, chat_encerrado, analise)
    sucesso = adicionar_observacao(
        ticket_id,
        "Observação 2 — Contexto e Dor do Ticket",
        conteudo_html
    )

    if sucesso:
        print(f"[obs2] ✅ Observação 2 adicionada ao ticket {ticket_id}.")
    else:
        print(f"[obs2] ❌ Falha ao adicionar Observação 2 ao ticket {ticket_id}.")

    return sucesso
