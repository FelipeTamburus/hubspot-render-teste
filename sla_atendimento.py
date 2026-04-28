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
SLA_URGENTE  = 70   # até 70h úteis (7 dias)
# mais de 70h úteis = sla_estourado


def calcular_horas_comerciais(criado_em_utc, agora_utc):
    """
    Calcula quantas horas úteis (08h-18h, seg-sex) se passaram
    entre criado_em_utc e agora_utc, preservando o horário exato de abertura.

    Exemplo:
      Aberto: quinta 09/04 às 10:27
      Agora:  segunda 20/04 às 10:27
      Resultado: 70h úteis (7 dias úteis × 10h)
    """
    criado = criado_em_utc.astimezone(FUSO_BRASILIA)
    agora = agora_utc.astimezone(FUSO_BRASILIA)

    if agora <= criado:
        return 0.0

    total_minutos = 0.0
    cursor = criado

    while cursor < agora:
        # Pula fins de semana
        if cursor.weekday() >= 5:
            # Avança para segunda-feira às 08:00
            dias_ate_segunda = 7 - cursor.weekday()
            cursor = (cursor + datetime.timedelta(days=dias_ate_segunda)).replace(
                hour=HORARIO_INICIO, minute=0, second=0, microsecond=0
            )
            continue

        inicio_dia = cursor.replace(hour=HORARIO_INICIO, minute=0, second=0, microsecond=0)
        fim_dia = cursor.replace(hour=HORARIO_FIM, minute=0, second=0, microsecond=0)

        # Se cursor está antes do expediente, avança para o início
        if cursor < inicio_dia:
            cursor = inicio_dia
            continue

        # Se cursor está após o expediente, avança para o próximo dia útil às 08:00
        if cursor >= fim_dia:
            proximo = cursor + datetime.timedelta(days=1)
            cursor = proximo.replace(hour=HORARIO_INICIO, minute=0, second=0, microsecond=0)
            continue

        # Cursor está dentro do expediente
        # Calcula até quando contar neste dia (mínimo entre agora e fim do expediente)
        fim_periodo = min(agora, fim_dia)
        minutos = (fim_periodo - cursor).total_seconds() / 60
        total_minutos += minutos

        # Se agora está dentro deste dia, terminou
        if agora <= fim_dia:
            break

        # Avança para o próximo dia útil às 08:00
        proximo = cursor + datetime.timedelta(days=1)
        cursor = proximo.replace(hour=HORARIO_INICIO, minute=0, second=0, microsecond=0)

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


DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1493299930566230077/5_TWodg7wdyxl_4TuWacCqxpSFsagBNcDUuI29VMhzYF8DTRvxUqlCj2lEZlmJLpP1nb"
DISCORD_MENTIONS = "<@1352245725995728906> <@1328680380748009513>"
HUBSPOT_PORTAL_ID = "44225969"


def buscar_nome_analista(ticket_id):
    """Busca o nome do analista responsável pelo ticket."""
    url = f"https://api.hubapi.com/crm/v3/objects/tickets/{ticket_id}?properties=hubspot_owner_id"
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        owner_id = response.json().get("properties", {}).get("hubspot_owner_id", "")
        if not owner_id:
            return "Não atribuído"

        owner_url = f"https://api.hubapi.com/crm/v3/owners/{owner_id}"
        owner_resp = requests.get(owner_url, headers=HEADERS, timeout=10)
        owner_resp.raise_for_status()
        dados = owner_resp.json()
        nome = f"{dados.get('firstName', '')} {dados.get('lastName', '')}".strip()
        return nome or "Não atribuído"
    except Exception:
        return "Não atribuído"


def gerar_resumo_discord(subject, ticket_id):
    """Usa o Contexto.AI para gerar um resumo fiel e curto da demanda."""
    from contexto_ai_client import chamar_contexto_ai
    prompt = f"""Com base no título do ticket de suporte abaixo, gere um resumo muito curto e direto da demanda do cliente em no máximo 1 frase de até 15 palavras. Foque no problema principal. Responda APENAS com o resumo, sem aspas, sem pontuação no final.

Título: {subject}"""
    try:
        resumo = chamar_contexto_ai(prompt, task_name="resumo_discord_sla")
        return resumo.strip().strip('"').strip("'") if resumo else subject
    except Exception:
        return subject


def enviar_alerta_discord(ticket_id, subject, horas):
    """Envia alerta no Discord quando um ticket estoura o SLA."""
    link = f"https://app.hubspot.com/contacts/{HUBSPOT_PORTAL_ID}/ticket/{ticket_id}"
    analista = buscar_nome_analista(ticket_id)
    resumo = gerar_resumo_discord(subject, ticket_id)

    mensagem = (
        f"🆘 **TICKET - SLA ESTOURADO** 🆘\n"
        f"{DISCORD_MENTIONS}\n"
        f"_Um ticket está com SLA Estourado, precisamos de atenção especial!_\n\n"
        f"🆔 **ID:** [#{ticket_id}]({link})\n"
        f"🎙️ **Resumo:** {resumo}\n"
        f"⏱️ **Tempo aberto:** {horas:.1f}h úteis\n"
        f"🚹 **Analista:** {analista}"
    )

    try:
        response = requests.post(
            DISCORD_WEBHOOK,
            json={"content": mensagem},
            timeout=10
        )
        if response.status_code in [200, 204]:
            print(f"[sla] ✅ Alerta Discord enviado para ticket {ticket_id}.")
            return True
        else:
            print(f"[sla] ❌ Discord retornou {response.status_code}: {response.text}")
            return False
    except Exception as e:
        print(f"[sla] ❌ Erro ao enviar alerta Discord: {e}")
        return False


def enviar_alerta_discord_teste():
    """Envia uma mensagem fictícia no Discord para teste do webhook."""
    link = f"https://app.hubspot.com/contacts/{HUBSPOT_PORTAL_ID}/ticket/00000000"
    mensagem = (
        f"🆘 **TICKET - SLA ESTOURADO** 🆘\n"
        f"{DISCORD_MENTIONS}\n"
        f"_Um ticket está com SLA Estourado, precisamos de atenção especial!_\n\n"
        f"🆔 **ID:** [#00000000]({link})\n"
        f"🎙️ **Resumo:** Erro na emissão de nota fiscal no módulo financeiro\n"
        f"⏱️ **Tempo aberto:** 72.5h úteis\n"
        f"🚹 **Analista:** Agente do Suporte\n\n"
        f"⚠️ _Esta é uma mensagem de teste — nenhuma ação necessária._"
    )
    try:
        response = requests.post(
            DISCORD_WEBHOOK,
            json={"content": mensagem},
            timeout=10
        )
        if response.status_code in [200, 204]:
            print(f"[sla] ✅ Mensagem de teste enviada no Discord.")
            return True
        else:
            print(f"[sla] ❌ Discord retornou {response.status_code}: {response.text}")
            return False
    except Exception as e:
        print(f"[sla] ❌ Erro ao enviar teste Discord: {e}")
        return False


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
    Busca todos os tickets em atendimento e atualiza o SLA de cada um.
    Envia alerta no Discord quando um ticket muda para sla_estourado.
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

            # Dispara alerta no Discord apenas quando muda PARA sla_estourado
            if novo_sla == "sla_estourado" and sla_atual != "sla_estourado":
                print(f"[sla] 🆘 Ticket {ticket_id} estourou o SLA! Enviando alerta no Discord...")
                enviar_alerta_discord(ticket_id, subject, horas)

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
