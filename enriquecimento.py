import requests
import json
import datetime
import os

ACCESS_TOKEN_HUBSPOT = os.environ.get("ACCESS_TOKEN_HUBSPOT")

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN_HUBSPOT}",
    "Content-Type": "application/json"
}

PIPELINE_SUPORTE_ID = "0"  # ID da sua pipeline de suporte

def buscar_ticket(ticket_id):
    """Busca os dados do ticket no HubSpot incluindo a pipeline."""
    url = f"https://api.hubapi.com/crm/v3/objects/tickets/{ticket_id}?properties=hs_pipeline,subject"
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao buscar ticket {ticket_id}: {e}")
        return None

def adicionar_observacao(ticket_id, observacao_html, titulo_nota):
    """Adiciona uma nota associada a um ticket no HubSpot."""
    if not ACCESS_TOKEN_HUBSPOT:
        print("❌ Token HubSpot ausente.")
        return False

    if not observacao_html or not observacao_html.strip():
        print(f"⚠️ Conteúdo da observação '{titulo_nota}' vazio. Pulando.")
        return True

    api_url_notes = "https://api.hubapi.com/crm/v3/objects/notes"

    ts_ms = str(int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000))
    corpo_nota_completo = f"<h3>{titulo_nota}</h3><hr>{observacao_html.strip()}"

    payload = {
        "properties": {
            "hs_note_body": corpo_nota_completo,
            "hs_timestamp": ts_ms
        },
        "associations": [
            {
                "to": {"id": str(ticket_id).strip()},
                "types": [
                    {
                        "associationCategory": "HUBSPOT_DEFINED",
                        "associationTypeId": 228
                    }
                ]
            }
        ]
    }

    try:
        response = requests.post(api_url_notes, headers=HEADERS, json=payload, timeout=30)
        response.raise_for_status()
        print(f"✅ Nota '{titulo_nota}' adicionada ao ticket {ticket_id}.")
        return True
    except requests.exceptions.Timeout:
        print(f"❌ Timeout ao adicionar nota no ticket {ticket_id}.")
        return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao adicionar nota no ticket {ticket_id}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   Status: {e.response.status_code}")
            print(f"   Detalhe: {e.response.text[:300]}")
        return False


def processar_ticket(ticket_id):
    """Busca o ticket, verifica a pipeline e adiciona a observação."""
    print(f"[processando] Buscando ticket {ticket_id}...")

    ticket = buscar_ticket(ticket_id)
    if not ticket:
        return

    pipeline_id = ticket.get("properties", {}).get("hs_pipeline", "")
    print(f"[info] Ticket {ticket_id} pertence à pipeline: {pipeline_id}")

    # Só processa se for da pipeline de suporte
    if str(pipeline_id) != PIPELINE_SUPORTE_ID:
        print(f"[ignorado] Ticket {ticket_id} não é da pipeline de suporte (pipeline: {pipeline_id}). Ignorando.")
        return

    print(f"[ok] Ticket {ticket_id} é da pipeline de suporte. Adicionando observação...")

    conteudo_nota = "<p>teste render - Felipe Tamburus</p>"

    resultado = adicionar_observacao(
        ticket_id=ticket_id,
        observacao_html=conteudo_nota,
        titulo_nota="Teste Render"
    )

    if resultado:
        print(f"✅ Ticket {ticket_id} enriquecido com sucesso!")
    else:
        print(f"❌ Falha ao enriquecer o ticket {ticket_id}.")
