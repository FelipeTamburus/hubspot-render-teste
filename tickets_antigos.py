import os
import time
import datetime
import threading
import requests
from categorizacao import processar_categorizacao
from obs2_dor_ticket import processar_obs2

PIPELINE_SUPORTE_ID = "0"
STAGE_NOVO = "1"
HORAS_LIMITE = 8

ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN_HUBSPOT")
HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}


def buscar_tickets_antigos():
    """
    Busca tickets na coluna Novo criados há mais de 8 horas.
    Retorna lista com detalhes de cada ticket.
    """
    agora = datetime.datetime.now(datetime.timezone.utc)
    url = "https://api.hubapi.com/crm/v3/objects/tickets/search"
    todos = []
    after = None

    while True:
        payload = {
            "filterGroups": [{"filters": [
                {"propertyName": "hs_pipeline", "operator": "EQ", "value": PIPELINE_SUPORTE_ID},
                {"propertyName": "hs_pipeline_stage", "operator": "EQ", "value": STAGE_NOVO}
            ]}],
            "properties": ["subject", "hs_object_source", "createdate"],
            "sorts": [{"propertyName": "createdate", "direction": "ASCENDING"}],
            "limit": 100
        }
        if after:
            payload["after"] = after
        try:
            response = requests.post(url, headers=HEADERS, json=payload, timeout=15)
            response.raise_for_status()
            data = response.json()
            todos.extend(data.get("results", []))
            paging = data.get("paging", {}).get("next", {}).get("after")
            if paging:
                after = paging
            else:
                break
        except Exception as e:
            print(f"[antigos] Erro ao buscar tickets: {e}")
            break

    antigos = []
    for ticket in todos:
        ticket_id = str(ticket.get("id", ""))
        props = ticket.get("properties", {})
        createdate = props.get("createdate", "")
        if not createdate or not ticket_id:
            continue
        try:
            criado_em = datetime.datetime.fromisoformat(createdate.replace("Z", "+00:00"))
            horas_aberto = (agora - criado_em).total_seconds() / 3600
        except Exception:
            continue
        if horas_aberto >= HORAS_LIMITE:
            source = props.get("hs_object_source", "")
            subject = props.get("subject", "Sem título")
            canal = "chat" if ("CHAT" in source.upper() or "bot" in subject.lower()) else "e-mail"
            antigos.append({
                "ticket_id": ticket_id,
                "subject": subject,
                "horas_aberto": round(horas_aberto, 1),
                "canal": canal,
                "criado_em": criado_em.strftime("%d/%m/%Y %H:%M")
            })

    return antigos


def categorizar_antigos(r=None):
    """
    Categoriza todos os tickets em Novo criados há mais de 8 horas.
    Também dispara a Obs 2 para tickets de chat que ainda não foram processados.
    Roda em background — pode ser chamada pelo endpoint ou pela rotina automática.
    """
    print("[antigos] Iniciando categorização de tickets antigos...")
    antigos = buscar_tickets_antigos()
    total = len(antigos)
    print(f"[antigos] {total} ticket(s) encontrado(s) com mais de {HORAS_LIMITE}h em Novo.")

    if not total:
        print("[antigos] Nenhum ticket para categorizar.")
        return

    for t in antigos:
        ticket_id = t["ticket_id"]
        canal = t["canal"]
        print(f"[antigos] Processando ticket {ticket_id} ({t['horas_aberto']}h em Novo · {canal})...")

        # Sempre categoriza — independente do canal ou status do chat
        threading.Thread(target=processar_categorizacao, args=(ticket_id,), daemon=True).start()

        # Para chat: dispara Obs 2 se ainda não foi processada
        if canal == "chat" and r is not None:
            obs2_concluida = r.exists(f"obs2_concluida:{ticket_id}")
            if not obs2_concluida:
                print(f"[antigos] Disparando Obs 2 para ticket de chat {ticket_id}...")
                threading.Thread(target=processar_obs2, args=(ticket_id,), daemon=True).start()
            else:
                print(f"[antigos] Obs 2 já processada para ticket {ticket_id}.")

    print(f"[antigos] ✅ {total} tickets enviados para categorização.")


def worker_tickets_antigos(r):
    """
    Rotina automática que roda a cada 1 hora verificando tickets antigos em Novo.
    """
    print("[worker_antigos] Iniciado, verificando tickets antigos a cada 1 hora...")
    while True:
        try:
            time.sleep(3600)
            print("[worker_antigos] Iniciando varredura de tickets antigos...")
            categorizar_antigos(r)
        except Exception as e:
            print(f"[worker_antigos] Erro: {e}")
