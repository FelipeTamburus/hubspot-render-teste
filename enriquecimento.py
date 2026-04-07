import requests
import json
import datetime
import os

# Pega o token do HubSpot via variável de ambiente (configurada no Render)
ACCESS_TOKEN_HUBSPOT = os.environ.get("ACCESS_TOKEN_HUBSPOT")

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN_HUBSPOT}",
    "Content-Type": "application/json"
}

def adicionar_observacao(ticket_id, observacao_html, titulo_nota):
    """
    Adiciona uma nota (observação) associada a um ticket no HubSpot.
    Reaproveitado diretamente do seu script original.
    """
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
    """
    Função principal chamada pelo webhook.
    Recebe o ID do ticket recém-criado e adiciona a observação de teste.
    """
    print(f"[processando] Iniciando enriquecimento do ticket {ticket_id}...")

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