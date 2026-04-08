import os
import time
import json
from collections import Counter
from datetime import datetime, timezone
from google import genai
from google.genai import types
from hubspot_client import (
    buscar_ticket,
    buscar_company_id,
    buscar_todos_tickets_empresa_30_dias,
    adicionar_observacao
)

GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
HUBSPOT_PORTAL_ID = "44225969"


# --- GEMINI ---

def configurar_gemini():
    keys_str = os.environ.get("GEMINI_API_KEYS", "")
    keys = [k.strip() for k in keys_str.split(",") if k.strip()]
    if not keys:
        print("[obs1] ERRO: Nenhuma chave Gemini encontrada.")
        return None
    return genai.Client(api_key=keys[0])


def resumir_ticket_com_gemini(client, subject, tipo_de_servico, conteudo):
    """Gera um resumo enxuto de 1-2 frases sobre o ticket."""
    if not conteudo and not subject:
        return "Sem conteúdo disponível para resumo."

    prompt = f"""
Você é um analista de suporte. Gere um resumo ENXUTO de 1 a 2 frases sobre o ticket abaixo.
Descreva o problema ou solicitação do cliente de forma objetiva e clara.

Assunto: {subject}
Tipo de serviço: {tipo_de_servico}
Conteúdo: {conteudo or 'Não disponível'}

Responda APENAS com um JSON no formato:
{{
  "resumo": "Texto do resumo aqui."
}}
"""
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json"
            )
        )
        texto = response.text.strip().replace("```json", "").replace("```", "")
        resultado = json.loads(texto)
        return resultado.get("resumo", "Resumo não disponível.")
    except Exception as e:
        print(f"[obs1] Erro ao resumir ticket com Gemini: {e}")
        return subject or "Resumo não disponível."


def url_ticket(ticket_id):
    return f"https://app.hubspot.com/contacts/{HUBSPOT_PORTAL_ID}/ticket/{ticket_id}"


# --- CHURN ---

def calcular_churn(tickets_30_dias):
    pontos = 0
    fatores = []
    total = len(tickets_30_dias)

    if total > 10:
        pontos += 50
        fatores.append(f"Mais de 10 tickets nos últimos 30 dias ({total} tickets)")
    elif total > 5:
        pontos += 30
        fatores.append(f"Mais de 5 tickets nos últimos 30 dias ({total} tickets)")
    elif total > 2:
        pontos += 10
        fatores.append(f"{total} tickets nos últimos 30 dias")

    tem_high = any(
        t.get("properties", {}).get("hs_ticket_priority") == "HIGH"
        for t in tickets_30_dias
    )
    if tem_high:
        pontos += 20
        fatores.append("Possui ticket(s) com prioridade ALTA")

    tipos = [t.get("properties", {}).get("tipo_de_servico", "") for t in tickets_30_dias]
    tipos = [t for t in tipos if t]
    if tipos:
        mais_comum = Counter(tipos).most_common(1)[0]
        if mais_comum[1] >= 3:
            pontos += 25
            fatores.append(f"Problema recorrente: '{mais_comum[0]}' aparece {mais_comum[1]}x")

    if total == 1:
        pontos -= 10
        fatores.append("Primeiro ticket da empresa (cliente novo)")

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


# --- HTML ---

def gerar_html_obs1(company_id, total_tickets, churn_pontos, churn_class,
                    churn_fatores, problema_recorrente, tickets_recentes, client):
    """Gera o HTML da Observação 1 com emojis, churn e tickets recentes."""

    hoje = datetime.now(timezone.utc).strftime("%d/%m/%Y")

    # Cabeçalho
    html = "<p>🤖 <strong>[IA] CONTEXTO DA EMPRESA</strong></p><hr>"

    # Volume
    html += f"<p>📊 <strong>Total de tickets (últimos 30 dias):</strong> {total_tickets}</p>"

    # Problema recorrente
    if problema_recorrente:
        html += f"<p>🔁 <strong>Problema mais recorrente:</strong> {problema_recorrente[0]} ({problema_recorrente[1]}x nos últimos 30 dias)</p>"
    else:
        html += "<p>🔁 <strong>Problema mais recorrente:</strong> Nenhum padrão identificado</p>"

    # Risco de churn
    fatores_html = "".join([f"<li>{f}</li>" for f in churn_fatores]) if churn_fatores else "<li>Nenhum fator crítico identificado</li>"
    html += f"""
<p>⚠️ <strong>Risco de Churn:</strong> {churn_class} ({churn_pontos} pts)</p>
<p><strong>Fatores considerados:</strong></p>
<ul>{fatores_html}</ul>
<hr>
"""

    # 3 tickets mais recentes
    html += "<p>🕐 <strong>Últimos tickets desta empresa:</strong></p>"

    if tickets_recentes:
        for i, t in enumerate(tickets_recentes[:3]):
            props = t.get("properties", {})
            ticket_id = t.get("id", "")
            subject = props.get("subject", "Sem título")
            tipo = props.get("tipo_de_servico", "")
            conteudo = props.get("demanda_apresentada_pelo_cliente", "") or props.get("content", "")
            data_raw = props.get("createdate", "")
            data = data_raw[:10] if data_raw else ""
            link = url_ticket(ticket_id)

            resumo = resumir_ticket_com_gemini(client, subject, tipo, conteudo)

            ordinal = f"{i+1}º"
            html += f"""
<p><strong>{ordinal} Ticket:</strong> {subject} <em>({data})</em><br>
{resumo}<br>
<a href="{link}">🔗 Clique aqui para visualizar o ticket</a></p>
"""
    else:
        html += "<p><em>Nenhum ticket recente encontrado.</em></p>"

    html += f"<hr><p><small>Análise automática realizada em {hoje}</small></p>"

    return html


# --- PRINCIPAL ---

def processar_obs1(ticket_id):
    print(f"[obs1] Iniciando para ticket {ticket_id}...")
    print(f"[obs1] Aguardando 60 segundos...")
    time.sleep(60)

    client = configurar_gemini()

    ticket = buscar_ticket(ticket_id)
    if not ticket:
        print(f"[obs1] Ticket {ticket_id} não encontrado. Abortando.")
        return False

    company_id = buscar_company_id(ticket_id)
    if not company_id:
        print(f"[obs1] Ticket {ticket_id} sem empresa associada.")
        html = """
<p>🤖 <strong>[IA] CONTEXTO DA EMPRESA</strong></p>
<hr>
<p>⚠️ Empresa não identificada neste ticket. Nenhum dado de contexto disponível.</p>
"""
        adicionar_observacao(ticket_id, "Observação 1 — Contexto da Empresa", html)
        return True

    print(f"[obs1] Empresa identificada: {company_id}")

    tickets_30_dias = buscar_todos_tickets_empresa_30_dias(company_id)
    # Remove o ticket atual da lista
    tickets_30_dias = [t for t in tickets_30_dias if t.get("id") != str(ticket_id)]
    total = len(tickets_30_dias)
    print(f"[obs1] {total} tickets encontrados nos últimos 30 dias.")

    pontos, classificacao, fatores = calcular_churn(tickets_30_dias)
    recorrente = problema_mais_recorrente(tickets_30_dias)

    # Pega os 3 mais recentes (já vêm ordenados por data DESC da API)
    tickets_recentes = tickets_30_dias[:3]

    conteudo_html = gerar_html_obs1(
        company_id, total, pontos, classificacao,
        fatores, recorrente, tickets_recentes, client
    )

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
