import requests
import datetime
import os
from datetime import timezone

ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN_HUBSPOT")
BASE_URL = "https://api.hubapi.com"

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

ANALISTAS_IDS = {"79245101", "77002308"}
REMETENTES_BOT = {"Easybot", "Agente do Suporte"}
PIPELINE_SUPORTE = "0"
STAGE_RESOLVIDO = "164386119"


# --- TICKETS ---

def buscar_ticket(ticket_id, properties=None):
    """Busca um ticket pelo ID com as propriedades solicitadas."""
    props = properties or [
        "subject", "content", "hs_pipeline", "hs_pipeline_stage",
        "hubspot_owner_id", "hs_object_source", "createdate",
        "analisado_pela_ia", "associatedcompanyid", "id_empresa_ej",
        "demanda_apresentada_pelo_cliente", "tipo_de_servico",
        "hs_ticket_priority"
    ]
    url = f"{BASE_URL}/crm/v3/objects/tickets/{ticket_id}"
    try:
        response = requests.get(url, headers=HEADERS, params={"properties": ",".join(props)}, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"[hubspot] Erro ao buscar ticket {ticket_id}: {e}")
        return None


def buscar_tickets_empresa(company_id, stage=STAGE_RESOLVIDO, dias=30, limit=50):
    """Busca tickets resolvidos de uma empresa nos últimos N dias."""

    url = f"{BASE_URL}/crm/v3/objects/tickets/search"
    payload = {
        "filterGroups": [{
            "filters": [
                {"propertyName": "id_empresa_ej", "operator": "EQ", "value": str(company_id)},
                {"propertyName": "hs_pipeline", "operator": "EQ", "value": PIPELINE_SUPORTE},
                {"propertyName": "hs_pipeline_stage", "operator": "EQ", "value": stage},
            ]
        }],
        "properties": [
            "subject", "demanda_apresentada_pelo_cliente",
            "tipo_de_servico", "hs_ticket_priority", "createdate", "id_empresa_ej"
        ],
        "sorts": [{"propertyName": "createdate", "direction": "DESCENDING"}],
        "limit": limit
    }
    try:
        response = requests.post(url, headers=HEADERS, json=payload, timeout=15)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"[hubspot] Erro ao buscar tickets da empresa {company_id}: {e}")
        return []


def buscar_tickets_resolvidos_globais(tipo_de_servico=None, limit=50):
    """Busca tickets resolvidos globalmente, opcionalmente filtrando por tipo_de_servico."""
    url = f"{BASE_URL}/crm/v3/objects/tickets/search"
    filters = [
        {"propertyName": "hs_pipeline", "operator": "EQ", "value": PIPELINE_SUPORTE},
        {"propertyName": "hs_pipeline_stage", "operator": "EQ", "value": STAGE_RESOLVIDO}
    ]
    if tipo_de_servico:
        filters.append({"propertyName": "tipo_de_servico", "operator": "EQ", "value": tipo_de_servico})

    payload = {
        "filterGroups": [{"filters": filters}],
        "properties": [
            "subject", "demanda_apresentada_pelo_cliente",
            "tipo_de_servico", "createdate", "associatedcompanyid"
        ],
        "sorts": [{"propertyName": "createdate", "direction": "DESCENDING"}],
        "limit": limit
    }
    try:
        response = requests.post(url, headers=HEADERS, json=payload, timeout=15)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"[hubspot] Erro ao buscar tickets globais: {e}")
        return []


def buscar_todos_tickets_empresa_30_dias(company_id):
    """Busca todos os tickets (qualquer status) de uma empresa nos últimos 30 dias."""

    url = f"{BASE_URL}/crm/v3/objects/tickets/search"
    payload = {
        "filterGroups": [{
            "filters": [
                {"propertyName": "id_empresa_ej", "operator": "EQ", "value": str(company_id)},
                {"propertyName": "hs_pipeline", "operator": "EQ", "value": PIPELINE_SUPORTE},
            ]
        }],
        "properties": ["subject", "hs_ticket_priority", "createdate", "tipo_de_servico"],
        "sorts": [{"propertyName": "createdate", "direction": "DESCENDING"}],
        "limit": 100
    }
    try:
        response = requests.post(url, headers=HEADERS, json=payload, timeout=15)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"[hubspot] Erro ao buscar todos tickets empresa {company_id}: {e}")
        return []


# --- CONVERSAS E MENSAGENS ---

def buscar_thread_conversa(ticket_id):
    """Busca o ID da thread de conversa associada ao ticket."""
    url = f"{BASE_URL}/crm/v3/objects/tickets/{ticket_id}?associations=conversation"
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        resultados = response.json().get("associations", {}).get("conversations", {}).get("results", [])
        return resultados[0]["id"] if resultados else None
    except requests.exceptions.RequestException as e:
        print(f"[hubspot] Erro ao buscar thread do ticket {ticket_id}: {e}")
        return None


def buscar_mensagens_chat(thread_id):
    """Busca todas as mensagens de uma thread de conversa."""
    url = f"{BASE_URL}/conversations/v3/conversations/threads/{thread_id}/messages"
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"[hubspot] Erro ao buscar mensagens da thread {thread_id}: {e}")
        return []


def chat_esta_encerrado(thread_id):
    """Verifica se o chat de uma thread está encerrado."""
    url = f"{BASE_URL}/conversations/v3/conversations/threads/{thread_id}"
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        status = response.json().get("status", "")
        return status in ["ENDED", "CLOSED", "ARCHIVED"]
    except requests.exceptions.RequestException as e:
        print(f"[hubspot] Erro ao verificar status do chat {thread_id}: {e}")
        return False


def buscar_ultima_mensagem_analista(thread_id):
    """Busca a última mensagem enviada por um analista humano (não bot)."""
    mensagens = buscar_mensagens_chat(thread_id)
    mensagens_analista = []

    for msg in mensagens:
        if msg.get("type") not in ["MESSAGE", "WELCOME_MESSAGE"]:
            continue
        remetente = msg.get("senders", [{}])[0]
        nome = remetente.get("name", "")
        actor_id = str(remetente.get("actorId", ""))

        # Ignora bots
        if nome in REMETENTES_BOT:
            continue

        # Verifica se é analista pelo ID ou se começa com V- (cliente)
        creator = msg.get("createdBy", "")
        if creator.startswith("V-"):
            continue

        texto = msg.get("text", "").strip()
        if texto:
            mensagens_analista.append({
                "texto": texto,
                "timestamp": msg.get("createdAt", ""),
                "autor": nome or "Analista"
            })

    return mensagens_analista[-1] if mensagens_analista else None


# --- EMAILS ---

def buscar_emails_ticket(ticket_id):
    """Busca todos os e-mails associados a um ticket."""
    url = f"{BASE_URL}/crm/v4/objects/tickets/{ticket_id}/associations/email"
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        if response.status_code == 404:
            return []
        response.raise_for_status()
        email_ids = [a["toObjectId"] for a in response.json().get("results", [])]
        if not email_ids:
            return []

        batch_url = f"{BASE_URL}/crm/v3/objects/email/batch/read"
        payload = {
            "properties": ["hs_email_text", "hs_createdate", "hs_email_direction", "hs_email_from_email"],
            "inputs": [{"id": i} for i in email_ids]
        }
        batch_response = requests.post(batch_url, headers=HEADERS, json=payload, timeout=15)
        batch_response.raise_for_status()
        return batch_response.json().get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"[hubspot] Erro ao buscar e-mails do ticket {ticket_id}: {e}")
        return []


def buscar_ultimo_email_analista(ticket_id):
    """Busca o último e-mail enviado pelo analista (não pelo cliente)."""
    emails = buscar_emails_ticket(ticket_id)
    emails_analista = [
        e for e in emails
        if e.get("properties", {}).get("hs_email_direction") == "FORWARDED_EMAIL"
        or e.get("properties", {}).get("hs_email_direction") == "SENT"
    ]
    if not emails_analista:
        return None

    emails_analista.sort(
        key=lambda e: e.get("properties", {}).get("hs_createdate", ""),
        reverse=True
    )
    ultimo = emails_analista[0]["properties"]
    return {
        "texto": ultimo.get("hs_email_text", "").strip(),
        "timestamp": ultimo.get("hs_createdate", ""),
        "de": ultimo.get("hs_email_from_email", "")
    }


# --- NOTAS ---

def adicionar_observacao(ticket_id, titulo, conteudo_html):
    """Adiciona uma observação (nota) a um ticket no HubSpot."""
    url = f"{BASE_URL}/crm/v3/objects/notes"
    ts_ms = str(int(datetime.datetime.now(timezone.utc).timestamp() * 1000))
    corpo = f"<h3>{titulo}</h3><hr>{conteudo_html}"
    payload = {
        "properties": {
            "hs_note_body": corpo,
            "hs_timestamp": ts_ms
        },
        "associations": [{
            "to": {"id": str(ticket_id)},
            "types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 228}]
        }]
    }
    try:
        response = requests.post(url, headers=HEADERS, json=payload, timeout=15)
        response.raise_for_status()
        print(f"[hubspot] Observação '{titulo}' adicionada ao ticket {ticket_id}.")
        return True
    except requests.exceptions.RequestException as e:
        print(f"[hubspot] Erro ao adicionar observação ao ticket {ticket_id}: {e}")
        if hasattr(e, "response") and e.response is not None:
            print(f"  Detalhe: {e.response.text[:300]}")
        return False


# --- EMPRESA ---

def buscar_company_id(ticket_id):
    """
    Busca o ID da empresa pelo campo customizado id_empresa_ej do ticket.
    Esse campo contém o ID da empresa do cliente solicitante no HubSpot.
    """
    url = f"{BASE_URL}/crm/v3/objects/tickets/{ticket_id}?properties=id_empresa_ej"
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        company_id = response.json().get("properties", {}).get("id_empresa_ej")
        if company_id and str(company_id).strip():
            print(f"[hubspot] Empresa identificada via id_empresa_ej: {company_id}")
            return str(company_id).strip()
        print(f"[hubspot] Campo id_empresa_ej vazio para ticket {ticket_id}.")
        return None
    except requests.exceptions.RequestException as e:
        print(f"[hubspot] Erro ao buscar empresa do ticket {ticket_id}: {e}")
        return None
