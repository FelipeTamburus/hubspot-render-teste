import os
import time
import datetime
import requests

ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN_HUBSPOT")
HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

PIPELINE_SUPORTE_ID = "0"
COLUNAS_ATENDIMENTO = ["1338631792", "1338631793", "1338631794", "3"]

HORARIO_INICIO = 8    # 08:00
HORARIO_FIM = 18      # 18:00
FUSO_BRASILIA = datetime.timezone(datetime.timedelta(hours=-3))

# Faixas de SLA em horas úteis (08h-18h = 10h úteis/dia)
SLA_NORMAL   = 24   # até 24h úteis (2,4 dias)
SLA_URGENTE  = 30   # até 30h úteis (3 dias)
# mais de 70h úteis (7 dias) = sla_estourado


def calcular_horas_comerciais(criado_em_utc, agora_utc):
    """
    Calcula quantas horas comerciais (09h-17h, dias úteis seg-sex)
    se passaram entre criado_em_utc e agora_utc.
    """
    # Converte para horário de Brasília
    criado = criado_em_utc.astimezone(FUSO_BRASILIA)
    agora = agora_utc.astimezone(FUSO_BRASILIA)

    total_minutos = 0
    cursor = criado

    while cursor < agora:
        # Ignora fins de semana
        if cursor.weekday() >= 5:
            cursor += datetime.timedelta(days=1)
            cursor = cursor.replace(hour=HORARIO_INICIO, minute=0, second=0, microsecond=0)
            continue

        # Define início e fim do expediente do dia atual
        inicio_dia = cursor.replace(hour=HORARIO_INICIO, minute=0, second=0, microsecond=0)
        fim_dia = cursor.replace(hour=HORARIO_FIM, minute=0, second=0, microsecond=0)

        # Se criado após o expediente, avança para o próximo dia útil
        if cursor >= fim_dia:
            cursor += datetime.timedelta(days=1)
            cursor = cursor.replace(hour=HORARIO_INICIO, minute=0, second=0, microsecond=0)
            continue

        # Se criado antes do expediente, começa no início
        if cursor < inicio_dia:
            cursor = inicio_dia

        # Fim do período a contar (menor entre agora e fim do expediente)
        fim_periodo = min(agora, fim_dia)

        if fim_periodo > cursor:
            total_minutos += (fim_periodo - cursor).total_seconds() / 60

        # Avança para o próximo dia útil
        cursor += datetime.timedelta(days=1)
        cursor = cursor.replace(hour=HORARIO_INICIO, minute=0, second=0, microsecond=0)

    return total_minutos / 60


def calcular_sla(horas_uteis):
    """Retorna o valor da propriedade sla_atendimento com base nas horas úteis."""
    if horas_uteis <= SLA_NORMAL:
        return "sla_normal"
    elif horas_uteis <= SLA_URGENTE:
        return "sla_urgente"
    else:
        return "sla_estourado"


def buscar_tickets_em_atendimento():
    """Busca todos os tickets nas colunas de atendimento (Alta, Média, Baixa)."""
    url = "https://api.hubapi.com/crm/v3/objects/tickets/search"
    todos = []
    after = None

    while True:
        payload = {
            "filterGroups": [{
                "filters": [
                    {"propertyName": "hs_pipeline", "operator": "EQ", "value": PIPELINE_SUPORTE_ID},
                    {"propertyName": "hs_pipeline_stage", "operator": "IN", "values": COLUNAS_ATENDIMENTO}
                ]
            }],
            "properties": ["subject", "createdate", "hs_pipeline_stage", "sla_atendimento"],
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
            print(f"[sla] Erro ao buscar tickets: {e}")
            break

    return todos


def atualizar_sla_ticket(ticket_id, sla_valor):
    """Atualiza a propriedade sla_atendimento do ticket."""
    url = f"https://api.hubapi.com/crm/v3/objects/tickets/{ticket_id}"
    try:
        response = requests.patch(
            url,
            headers=HEADERS,
            json={"properties": {"sla_atendimento": sla_valor}},
            timeout=10
        )
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"[sla] Erro ao atualizar SLA do ticket {ticket_id}: {e}")
        return False


def rodar_analise_sla():
    """
    Busca todos os tickets em atendimento e atualiza o SLA de cada um
    com base nas horas comerciais desde a criação.
    """
    agora = datetime.datetime.now(datetime.timezone.utc)
    print(f"[sla] Iniciando análise de SLA — {agora.astimezone(FUSO_BRASILIA).strftime('%d/%m/%Y %H:%M')} (Brasília)")

    tickets = buscar_tickets_em_atendimento()
    total = len(tickets)
    print(f"[sla] {total} ticket(s) em atendimento encontrados.")

    if not total:
        print("[sla] Nenhum ticket para analisar.")
        return

    atualizados = 0
    for ticket in tickets:
        ticket_id = str(ticket.get("id", ""))
        props = ticket.get("properties", {})
        createdate = props.get("createdate", "")
        subject = props.get("subject", "Sem título")
        sla_atual = props.get("sla_atendimento", "")

        if not createdate:
            print(f"[sla] Ticket {ticket_id} sem createdate. Pulando.")
            continue

        try:
            criado_em = datetime.datetime.fromisoformat(createdate.replace("Z", "+00:00"))
        except Exception:
            continue

        horas = calcular_horas_comerciais(criado_em, agora)
        novo_sla = calcular_sla(horas)

        if novo_sla == sla_atual:
            print(f"[sla] Ticket {ticket_id} — {horas:.1f}h úteis — SLA já atualizado: {novo_sla}")
            continue

        sucesso = atualizar_sla_ticket(ticket_id, novo_sla)
        if sucesso:
            print(f"[sla] ✅ Ticket {ticket_id} '{subject[:40]}' — {horas:.1f}h úteis → {novo_sla}")
            atualizados += 1
        
    print(f"[sla] ✅ Análise concluída — {atualizados}/{total} tickets atualizados.")


def worker_sla():
    """Rotina automática que roda a cada 30 minutos."""
    print("[worker_sla] Iniciado, analisando SLA a cada 30 minutos...")
    while True:
        try:
            time.sleep(1800)
            rodar_analise_sla()
        except Exception as e:
            print(f"[worker_sla] Erro: {e}")
