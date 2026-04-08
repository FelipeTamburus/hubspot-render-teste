import os
import time
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

TIMEOUT_CHAT_SEGUNDOS = 7200   # 2 horas máximo esperando chat fechar
INTERVALO_VERIFICACAO = 30     # verifica status do chat a cada 30 segundos


def ticket_veio_por_chat(ticket):
    """Verifica se o ticket veio por canal de chat."""
    source = ticket.get("properties", {}).get("hs_object_source", "")
    subject = ticket.get("properties", {}).get("subject", "")
    return "CHAT" in source.upper() or "bot" in subject.lower()


def ticket_veio_por_bot(ticket):
    """Verifica se o ticket foi criado por bot com base no título."""
    subject = ticket.get("properties", {}).get("subject", "")
    return "ticket criado por bot" in subject.lower()


def aguardar_chat_encerrado(thread_id):
    """
    Aguarda até o chat ser encerrado ou timeout de 2 horas.
    Retorna True se encerrou, False se deu timeout.
    """
    tempo_aguardado = 0
    print(f"[obs2] Aguardando chat da thread {thread_id} ser encerrado...")

    while tempo_aguardado < TIMEOUT_CHAT_SEGUNDOS:
        if chat_esta_encerrado(thread_id):
            print(f"[obs2] Chat encerrado após {tempo_aguardado}s.")
            return True
        time.sleep(INTERVALO_VERIFICACAO)
        tempo_aguardado += INTERVALO_VERIFICACAO
        print(f"[obs2] Chat ainda aberto... ({tempo_aguardado}s aguardados)")

    print(f"[obs2] Timeout de {TIMEOUT_CHAT_SEGUNDOS}s atingido. Processando com o que há.")
    return False


def extrair_dor_do_chat(thread_id):
    """Extrai a dor principal do cliente a partir das mensagens do chat."""
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
        if creator.startswith("V-"):  # V- = cliente
            texto = msg.get("text", "").strip()
            if texto:
                mensagens_cliente.append(texto)

    if not mensagens_cliente:
        return None
    # Retorna as últimas 3 mensagens do cliente como contexto da dor
    return "\n".join(mensagens_cliente[-3:])


def extrair_dor_do_email(ticket_id):
    """Extrai a dor principal do cliente a partir dos e-mails recebidos."""
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
    # Pega o primeiro e-mail do cliente (abertura do ticket)
    return emails_cliente[0].get("properties", {}).get("hs_email_text", "").strip()


def gerar_html_obs2(company_id, total_tickets, dor_texto, canal, chat_encerrado=None):
    """Gera o HTML da Observação 2."""
    contexto_html = ""
    if company_id and total_tickets is not None:
        contexto_html = f"<p><strong>Tickets abertos pela empresa (últimos 30 dias):</strong> {total_tickets}</p>"
    elif not company_id:
        contexto_html = "<p><strong>Contexto da empresa:</strong> Empresa não identificada neste ticket.</p>"

    dor_html = ""
    if dor_texto:
        dor_formatada = dor_texto.replace("\n", "<br>")
        dor_html = f"<p><strong>Dor relatada pelo cliente:</strong></p><blockquote>{dor_formatada}</blockquote>"
    else:
        dor_html = "<p><strong>Dor relatada pelo cliente:</strong> Não foi possível identificar a dor neste ticket.</p>"

    canal_html = f"<p><strong>Canal de entrada:</strong> {canal}</p>"

    chat_status_html = ""
    if chat_encerrado is not None:
        status = "Chat encerrado pelo cliente" if chat_encerrado else "Chat encerrado por timeout (2h)"
        chat_status_html = f"<p><strong>Status do chat:</strong> {status}</p>"

    return f"{contexto_html}{canal_html}{chat_status_html}{dor_html}"


def processar_obs2(ticket_id):
    """
    Função principal da Observação 2.
    Aguarda 2 minutos, identifica canal, aguarda chat se necessário
    e extrai a dor do cliente.
    """
    print(f"[obs2] Iniciando para ticket {ticket_id}...")

    # Aguarda 2 minutos antes de processar
    print(f"[obs2] Aguardando 120 segundos...")
    time.sleep(120)

    # Busca dados do ticket
    ticket = buscar_ticket(ticket_id)
    if not ticket:
        print(f"[obs2] Ticket {ticket_id} não encontrado. Abortando.")
        return False

    props = ticket.get("properties", {})
    company_id = buscar_company_id(ticket_id)

    # Busca total de tickets da empresa
    total_tickets = None
    if company_id:
        tickets_30_dias = buscar_todos_tickets_empresa_30_dias(company_id)
        total_tickets = len(tickets_30_dias)

    # Identifica canal
    veio_por_chat = ticket_veio_por_chat(ticket)
    veio_por_bot = ticket_veio_por_bot(ticket)
    canal = "Chat" if veio_por_chat else "Formulário/E-mail"
    if veio_por_bot:
        canal = "Chat (via Bot)"

    dor_texto = None
    chat_encerrado = None

    if veio_por_chat or veio_por_bot:
        # Busca thread do chat
        thread_id = buscar_thread_conversa(ticket_id)
        if thread_id:
            # Verifica se já está encerrado
            if not chat_esta_encerrado(thread_id):
                chat_encerrado = aguardar_chat_encerrado(thread_id)
            else:
                chat_encerrado = True
                print(f"[obs2] Chat já estava encerrado.")
            dor_texto = extrair_dor_do_chat(thread_id)
        else:
            print(f"[obs2] Nenhuma thread de chat encontrada para ticket {ticket_id}.")
            dor_texto = props.get("content", "").strip() or None
    else:
        # Ticket por e-mail/formulário
        dor_texto = extrair_dor_do_email(ticket_id)
        if not dor_texto:
            dor_texto = props.get("content", "").strip() or None

    # Gera e adiciona a observação
    conteudo_html = gerar_html_obs2(company_id, total_tickets, dor_texto, canal, chat_encerrado)
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