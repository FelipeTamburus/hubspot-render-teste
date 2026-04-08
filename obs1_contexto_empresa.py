import os
import time
import json
from collections import Counter
from datetime import datetime, timezone
from contexto_ai_client import chamar_contexto_ai
from hubspot_client import (
    buscar_ticket,
    buscar_company_id,
    buscar_plano_empresa,
    buscar_todos_tickets_empresa_30_dias,
    adicionar_observacao
)

HUBSPOT_PORTAL_ID = "44225969"


def url_ticket(ticket_id):
    return f"https://app.hubspot.com/contacts/{HUBSPOT_PORTAL_ID}/ticket/{ticket_id}"


def resumir_ticket(subject, tipo_de_servico, conteudo):
    """Usa o Contexto.AI para gerar um resumo enxuto do ticket."""
    if not subject and not conteudo:
        return "Sem conteúdo disponível para resumo."

    prompt = f"""Analise o ticket de suporte abaixo e retorne APENAS um JSON válido com o campo "resumo" contendo 1 a 2 frases objetivas descrevendo o problema ou solicitação do cliente. Não inclua explicações fora do JSON.

Assunto: {subject}
Tipo de serviço: {tipo_de_servico or 'Não informado'}
Conteúdo: {conteudo or 'Não disponível'}

Formato obrigatório:
{{"resumo": "Texto do resumo aqui."}}"""

    resposta = chamar_contexto_ai(prompt, task_name="resumir_ticket_obs1")
    if not resposta:
        return subject or "Resumo não disponível."
    try:
        texto = resposta.replace("```json", "").replace("```", "").strip()
        return json.loads(texto).get("resumo", subject)
    except Exception:
        return subject or "Resumo não disponível."


PESOS_PLANO = {
    "Trial":      {"pontos": 40, "descricao": "Trial — cliente ainda não converteu"},
    "Starter":    {"pontos": 30, "descricao": "Starter — plano básico, maior risco"},
    "Premium":    {"pontos": 30, "descricao": "Premium — plano básico, maior risco"},
    "Standard":   {"pontos": 15, "descricao": "Standard — plano médio"},
    "Convênio":   {"pontos": 15, "descricao": "Convênio — depende do acordo"},
    "EasyCall":   {"pontos": 10, "descricao": "EasyCall — risco médio-baixo"},
    "Growth":     {"pontos":  5, "descricao": "Growth — baixo risco, requer atenção"},
    "Growth+":    {"pontos":  5, "descricao": "Growth+ — baixo risco, atenção especial"},
    "Enterprise": {"pontos":  0, "descricao": "Enterprise — contrato robusto, baixíssimo risco"},
    "legal_crm":  {"pontos":  0, "descricao": "Legal CRM — produto específico, baixíssimo risco"},
}


def calcular_churn(tickets_30_dias, plano=None):
    pontos = 0
    fatores = []
    total = len(tickets_30_dias)

    # Fator: volume de tickets
    if total > 10:
        pontos += 50
        fatores.append(f"Mais de 10 tickets nos últimos 30 dias ({total} tickets)")
    elif total > 5:
        pontos += 30
        fatores.append(f"Mais de 5 tickets nos últimos 30 dias ({total} tickets)")
    elif total > 2:
        pontos += 10
        fatores.append(f"{total} tickets nos últimos 30 dias")

    # Fator: prioridade alta
    tem_high = any(
        t.get("properties", {}).get("hs_ticket_priority") == "HIGH"
        for t in tickets_30_dias
    )
    if tem_high:
        pontos += 20
        fatores.append("Possui ticket(s) com prioridade ALTA")

    # Fator: problema recorrente
    tipos = [t.get("properties", {}).get("tipo_de_servico", "") for t in tickets_30_dias]
    tipos = [t for t in tipos if t]
    if tipos:
        mais_comum = Counter(tipos).most_common(1)[0]
        if mais_comum[1] >= 3:
            pontos += 25
            fatores.append(f"Problema recorrente: '{mais_comum[0]}' aparece {mais_comum[1]}x")

    # Fator: cliente novo
    if total == 1:
        pontos -= 10
        fatores.append("Primeiro ticket da empresa (cliente novo)")

    # Fator: plano contratado
    if plano and plano in PESOS_PLANO:
        peso_plano = PESOS_PLANO[plano]["pontos"]
        desc_plano = PESOS_PLANO[plano]["descricao"]
        pontos += peso_plano
        if peso_plano > 0:
            fatores.append(f"Plano {plano}: {desc_plano}")
    elif plano:
        fatores.append(f"Plano '{plano}' não mapeado — sem peso adicional")

    if pontos >= 85:
        classificacao = "🔴 Crítico"
    elif pontos >= 60:
        classificacao = "🟠 Alto"
    elif pontos >= 30:
        classificacao = "🟡 Médio"
    else:
        classificacao = "🟢 Baixo"

    return pontos, classificacao, fatores


def problema_mais_recorrente(tickets):
    if not tickets:
        return None
    tipos = [t.get("properties", {}).get("tipo_de_servico", "") for t in tickets]
    tipos = [t for t in tipos if t]
    if not tipos:
        return None
    return Counter(tipos).most_common(1)[0]


def gerar_html_obs1(company_id, total, pontos, classificacao, fatores, recorrente, tickets_recentes, plano):
    hoje = datetime.now(timezone.utc).strftime("%d/%m/%Y")

    html = "<p>🤖 <strong>[IA] CONTEXTO DA EMPRESA</strong></p><hr>"
    html += f"<p>📊 <strong>Total de tickets (últimos 30 dias):</strong> {total}</p>"

    # Plano contratado
    if plano:
        peso = PESOS_PLANO.get(plano, {}).get("pontos", 0)
        risco_plano = "🔴 Alto risco" if peso >= 30 else "🟡 Médio risco" if peso >= 10 else "🟢 Baixo risco"
        html += f"<p>💼 <strong>Plano contratado:</strong> {plano} — {risco_plano}</p>"
    else:
        html += "<p>💼 <strong>Plano contratado:</strong> Não identificado</p>"

    if recorrente:
        html += f"<p>🔁 <strong>Problema mais recorrente:</strong> {recorrente[0]} ({recorrente[1]}x nos últimos 30 dias)</p>"
    else:
        html += "<p>🔁 <strong>Problema mais recorrente:</strong> Nenhum padrão identificado</p>"

    fatores_html = "".join([f"<li>{f}</li>" for f in fatores]) if fatores else "<li>Nenhum fator crítico identificado</li>"
    html += f"<p>⚠️ <strong>Risco de Churn:</strong> {classificacao} ({pontos} pts)</p>"
    html += f"<p><strong>Fatores considerados:</strong></p><ul>{fatores_html}</ul><hr>"
    html += "<p>🕐 <strong>Últimos tickets desta empresa:</strong></p>"

    if tickets_recentes:
        for i, t in enumerate(tickets_recentes[:3]):
            props = t.get("properties", {})
            ticket_id = t.get("id", "")
            subject = props.get("subject", "Sem título")
            tipo = props.get("tipo_de_servico", "")
            conteudo = props.get("demanda_apresentada_pelo_cliente", "") or props.get("content", "")
            data = props.get("createdate", "")[:10] if props.get("createdate") else ""
            link = url_ticket(ticket_id)
            resumo = resumir_ticket(subject, tipo, conteudo)
            html += f"<p><strong>{i+1}º Ticket:</strong> {subject} <em>({data})</em><br>{resumo}<br><a href=\"{link}\">🔗 Clique aqui para visualizar o ticket</a></p>"
    else:
        html += "<p><em>Nenhum ticket recente encontrado.</em></p>"

    html += f"<hr><p><small>Análise automática realizada em {hoje}</small></p>"
    return html


def processar_obs1(ticket_id):
    print(f"[obs1] Iniciando para ticket {ticket_id}...")
    print(f"[obs1] Aguardando 60 segundos...")
    time.sleep(60)

    ticket = buscar_ticket(ticket_id)
    if not ticket:
        print(f"[obs1] Ticket {ticket_id} não encontrado. Abortando.")
        return False

    company_id = buscar_company_id(ticket_id)
    if not company_id:
        print(f"[obs1] Ticket {ticket_id} sem empresa associada.")
        html = "<p>🤖 <strong>[IA] CONTEXTO DA EMPRESA</strong></p><hr><p>⚠️ Empresa não identificada neste ticket. Nenhum dado de contexto disponível.</p>"
        adicionar_observacao(ticket_id, "Observação 1 — Contexto da Empresa", html)
        return True

    print(f"[obs1] Empresa identificada: {company_id}")
    tickets_30_dias = buscar_todos_tickets_empresa_30_dias(company_id)
    tickets_30_dias = [t for t in tickets_30_dias if t.get("id") != str(ticket_id)]
    total = len(tickets_30_dias)
    print(f"[obs1] {total} tickets encontrados nos últimos 30 dias.")

    # Busca plano contratado da empresa
    plano = buscar_plano_empresa(company_id)

    pontos, classificacao, fatores = calcular_churn(tickets_30_dias, plano=plano)
    recorrente = problema_mais_recorrente(tickets_30_dias)
    tickets_recentes = tickets_30_dias[:3]

    conteudo_html = gerar_html_obs1(company_id, total, pontos, classificacao, fatores, recorrente, tickets_recentes, plano)
    sucesso = adicionar_observacao(ticket_id, "Observação 1 — Contexto da Empresa", conteudo_html)

    if sucesso:
        print(f"[obs1] ✅ Observação 1 adicionada ao ticket {ticket_id}.")
    else:
        print(f"[obs1] ❌ Falha ao adicionar Observação 1 ao ticket {ticket_id}.")
    return sucesso
