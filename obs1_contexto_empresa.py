import os
import time
import datetime
from collections import Counter
from hubspot_client import (
    buscar_ticket,
    buscar_company_id,
    buscar_todos_tickets_empresa_30_dias,
    adicionar_observacao
)

# --- LÓGICA DE CHURN ---

def calcular_churn(tickets_30_dias, ticket_atual):
    """
    Calcula o risco de churn com base em pesos definidos.
    Retorna: (pontuacao, classificacao, fatores)
    """
    pontos = 0
    fatores = []
    total = len(tickets_30_dias)

    # Volume de tickets
    if total > 10:
        pontos += 50
        fatores.append(f"Mais de 10 tickets nos últimos 30 dias ({total} tickets)")
    elif total > 5:
        pontos += 30
        fatores.append(f"Mais de 5 tickets nos últimos 30 dias ({total} tickets)")
    elif total > 2:
        pontos += 10
        fatores.append(f"{total} tickets nos últimos 30 dias")

    # Ticket com prioridade HIGH
    tem_high = any(
        t.get("properties", {}).get("hs_ticket_priority") == "HIGH"
        for t in tickets_30_dias
    )
    if tem_high:
        pontos += 20
        fatores.append("Possui ticket(s) com prioridade ALTA")

    # Problema recorrente (mesmo tipo_de_servico 3+ vezes)
    tipos = [t.get("properties", {}).get("tipo_de_servico", "") for t in tickets_30_dias]
    tipos = [t for t in tipos if t]
    if tipos:
        mais_comum = Counter(tipos).most_common(1)[0]
        if mais_comum[1] >= 3:
            pontos += 25
            fatores.append(f"Problema recorrente: '{mais_comum[0]}' aparece {mais_comum[1]}x")

    # Cliente novo (primeiro ticket)
    if total == 1:
        pontos -= 10
        fatores.append("Primeiro ticket da empresa (cliente novo)")

    # Classificação
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
    """Identifica o assunto mais recorrente nos tickets."""
    if not tickets:
        return None
    tipos = [t.get("properties", {}).get("tipo_de_servico", "") for t in tickets]
    tipos = [t for t in tipos if t]
    if not tipos:
        return None
    return Counter(tipos).most_common(1)[0]


def gerar_html_obs1(total_tickets, churn_pontos, churn_class, churn_fatores, problema_recorrente):
    """Gera o HTML da Observação 1."""
    fatores_html = "".join([f"<li>{f}</li>" for f in churn_fatores]) if churn_fatores else "<li>Nenhum fator crítico identificado</li>"

    recorrente_html = ""
    if problema_recorrente:
        recorrente_html = f"<p><strong>Problema mais recorrente:</strong> {problema_recorrente[0]} ({problema_recorrente[1]}x nos últimos 30 dias)</p>"
    else:
        recorrente_html = "<p><strong>Problema mais recorrente:</strong> Nenhum padrão identificado</p>"

    return f"""
<p><strong>Total de tickets (últimos 30 dias):</strong> {total_tickets}</p>
{recorrente_html}
<p><strong>Risco de Churn:</strong> {churn_class} ({churn_pontos} pts)</p>
<p><strong>Fatores considerados:</strong></p>
<ul>{fatores_html}</ul>
"""


def processar_obs1(ticket_id):
    """
    Função principal da Observação 1.
    Busca contexto da empresa e calcula risco de churn.
    """
    print(f"[obs1] Iniciando para ticket {ticket_id}...")

    # Aguarda 1 minuto antes de processar
    print(f"[obs1] Aguardando 60 segundos...")
    time.sleep(60)

    # Busca dados do ticket
    ticket = buscar_ticket(ticket_id)
    if not ticket:
        print(f"[obs1] Ticket {ticket_id} não encontrado. Abortando.")
        return False

    props = ticket.get("properties", {})

    # Verifica se tem empresa associada
    company_id = buscar_company_id(ticket_id)
    if not company_id:
        print(f"[obs1] Ticket {ticket_id} sem empresa associada.")
        adicionar_observacao(
            ticket_id,
            "Observação 1 — Contexto da Empresa",
            "<p>⚠️ Empresa não identificada neste ticket. Nenhum dado de contexto disponível.</p>"
        )
        return True

    print(f"[obs1] Empresa identificada: {company_id}")

    # Busca tickets dos últimos 30 dias
    tickets_30_dias = buscar_todos_tickets_empresa_30_dias(company_id)
    total = len(tickets_30_dias)
    print(f"[obs1] {total} tickets encontrados nos últimos 30 dias.")

    # Calcula churn
    pontos, classificacao, fatores = calcular_churn(tickets_30_dias, ticket)

    # Identifica problema recorrente
    recorrente = problema_mais_recorrente(tickets_30_dias)

    # Gera e adiciona a observação
    conteudo_html = gerar_html_obs1(total, pontos, classificacao, fatores, recorrente)
    sucesso = adicionar_observacao(
        ticket_id,
        "Observação 1 — Contexto da Empresa",
        conteudo_html
    )

    if sucesso:
        print(f"[obs1] ✅ Observação 1 adicionada ao ticket {ticket_id}.")
    else:
        print(f"[obs1] ❌ Falha ao adicionar Observação 1 ao ticket {ticket_id}.")

    return sucesso