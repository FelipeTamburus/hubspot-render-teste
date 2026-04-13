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
        "hs_ticket_priority", "plano_contratado_easyjur"
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
    """
    Busca todos os tickets da empresa na pipeline de suporte (id=0)
    criados nos últimos 30 dias a partir de hoje.
    """
    data_limite = datetime.datetime.now(timezone.utc) - datetime.timedelta(days=30)
    data_limite_ms = str(int(data_limite.timestamp() * 1000))

    url = f"{BASE_URL}/crm/v3/objects/tickets/search"
    payload = {
        "filterGroups": [{
            "filters": [
                {"propertyName": "id_empresa_ej", "operator": "EQ", "value": str(company_id)},
                {"propertyName": "hs_pipeline", "operator": "EQ", "value": PIPELINE_SUPORTE},
                {"propertyName": "createdate", "operator": "GTE", "value": data_limite_ms}
            ]
        }],
        "properties": ["subject", "hs_ticket_priority", "createdate", "tipo_de_servico",
                       "demanda_apresentada_pelo_cliente", "content"],
        "sorts": [{"propertyName": "createdate", "direction": "DESCENDING"}],
        "limit": 100
    }
    try:
        response = requests.post(url, headers=HEADERS, json=payload, timeout=15)
        response.raise_for_status()
        resultados = response.json().get("results", [])
        print(f"[hubspot] {len(resultados)} tickets encontrados para empresa {company_id} nos últimos 30 dias (pipeline 0).")
        return resultados
    except requests.exceptions.RequestException as e:
        print(f"[hubspot] Erro ao buscar tickets empresa {company_id}: {e}")
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

def buscar_id_empresa_ej(ticket_id):
    """
    Busca o id_empresa_ej do ticket — ID interno do EasyJur.
    Usado para filtrar tickets similares da mesma empresa.
    """
    url = f"{BASE_URL}/crm/v3/objects/tickets/{ticket_id}?properties=id_empresa_ej"
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        company_ej_id = response.json().get("properties", {}).get("id_empresa_ej")
        if company_ej_id and str(company_ej_id).strip():
            return str(company_ej_id).strip()
        return None
    except requests.exceptions.RequestException as e:
        print(f"[hubspot] Erro ao buscar id_empresa_ej do ticket {ticket_id}: {e}")
        return None


def buscar_company_id(ticket_id):
    """
    Busca o ID do objeto empresa no HubSpot via relacionamento nativo.
    Usado para buscar propriedades da empresa como plano_contratado_ej.
    """
    url = f"{BASE_URL}/crm/v4/objects/tickets/{ticket_id}/associations/company"
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        resultados = response.json().get("results", [])
        if resultados:
            company_id = str(resultados[0]["toObjectId"])
            print(f"[hubspot] Empresa HubSpot identificada via associação: {company_id}")
            return company_id
        print(f"[hubspot] Nenhuma empresa associada ao ticket {ticket_id}.")
        return None
    except requests.exceptions.RequestException as e:
        print(f"[hubspot] Erro ao buscar empresa do ticket {ticket_id}: {e}")
        return None


def buscar_plano_empresa(company_id):
    """
    Busca o plano contratado da empresa pelo campo plano_contratado_ej.
    Retorna o nome interno do plano ou None se não encontrado.
    """
    url = f"{BASE_URL}/crm/v3/objects/companies/{company_id}?properties=plano_contratado_ej"
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        plano = response.json().get("properties", {}).get("plano_contratado_ej")
        if plano and str(plano).strip():
            print(f"[hubspot] Plano da empresa {company_id}: {plano}")
            return str(plano).strip()
        print(f"[hubspot] Campo plano_contratado_ej vazio para empresa {company_id}.")
        return None
    except requests.exceptions.RequestException as e:
        print(f"[hubspot] Erro ao buscar plano da empresa {company_id}: {e}")
        return None


def buscar_plano_do_ticket(ticket_id):
    """
    Busca o plano contratado diretamente do ticket via propriedade plano_contratado_easyjur.
    Mais simples e direto — não precisa buscar na empresa.
    """
    url = f"{BASE_URL}/crm/v3/objects/tickets/{ticket_id}?properties=plano_contratado_easyjur"
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        plano = response.json().get("properties", {}).get("plano_contratado_easyjur")
        if plano and str(plano).strip():
            print(f"[hubspot] Plano do ticket {ticket_id}: {plano}")
            return str(plano).strip()
        print(f"[hubspot] Campo plano_contratado_easyjur vazio para ticket {ticket_id}.")
        return None
    except requests.exceptions.RequestException as e:
        print(f"[hubspot] Erro ao buscar plano do ticket {ticket_id}: {e}")
        return None


# --- CONTROLE DE OBSERVAÇÕES ---

def obs_ja_criada(ticket_id, numero):
    """
    Verifica se uma observação já foi criada consultando a propriedade do ticket.
    numero: 1, 2 ou 3
    Retorna True se a propriedade obs{numero} == '1'
    """
    prop = f"obs{numero}"
    url = f"{BASE_URL}/crm/v3/objects/tickets/{ticket_id}?properties={prop}"
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        valor = response.json().get("properties", {}).get(prop, "")
        return str(valor).strip() == "1"
    except Exception as e:
        print(f"[hubspot] Erro ao verificar obs{numero} do ticket {ticket_id}: {e}")
        return False


def marcar_obs_criada(ticket_id, numero):
    """
    Marca a propriedade obs{numero} = '1' no ticket para evitar duplicatas.
    numero: 1, 2 ou 3
    """
    prop = f"obs{numero}"
    url = f"{BASE_URL}/crm/v3/objects/tickets/{ticket_id}"
    try:
        response = requests.patch(url, headers=HEADERS, json={"properties": {prop: "1"}}, timeout=10)
        response.raise_for_status()
    except Exception as e:
        print(f"[hubspot] Erro ao marcar obs{numero} do ticket {ticket_id}: {e}")
