import os
import json
from datetime import datetime, timezone
from contexto_ai_client import chamar_contexto_ai
from hubspot_client import (
    buscar_ticket,
    buscar_company_id,
    buscar_tickets_empresa,
    buscar_tickets_resolvidos_globais,
    buscar_ultimo_email_analista,
    buscar_thread_conversa,
    buscar_ultima_mensagem_analista,
    adicionar_observacao,
    STAGE_RESOLVIDO
)

HUBSPOT_PORTAL_ID = "44225969"


def url_ticket(ticket_id):
    return f"https://app.hubspot.com/contacts/{HUBSPOT_PORTAL_ID}/ticket/{ticket_id}"


def selecionar_similares(demanda_atual, candidatos, max_resultados=3):
    """Usa o Contexto.AI para selecionar os tickets mais similares."""
    if not candidatos:
        return []

    candidatos_formatados = []
    for i, t in enumerate(candidatos):
        props = t.get("properties", {})
        candidatos_formatados.append({
            "indice": i,
            "subject": props.get("subject", ""),
            "tipo_de_servico": props.get("tipo_de_servico", ""),
            "demanda": props.get("demanda_apresentada_pelo_cliente", "")
        })

    prompt = f"""Você é um analista de suporte da EasyJur especialista em identificar similaridade entre tickets. Analise o ticket atual e os candidatos abaixo e retorne APENAS um JSON válido. Não inclua explicações fora do JSON.

Demanda do ticket atual:
{demanda_atual}

Candidatos:
{json.dumps(candidatos_formatados, ensure_ascii=False, indent=2)}

Selecione os {max_resultados} candidatos mais similares ao ticket atual com base na demanda apresentada. Ordene do mais similar para o menos similar.

Formato obrigatório:
{{"indices_selecionados": [0, 2, 5]}}"""

    resposta = chamar_contexto_ai(prompt, task_name="selecionar_similares_obs3")
    if not resposta:
        return candidatos[:max_resultados]
    try:
        texto = resposta.replace("```json", "").replace("```", "").strip()
        indices = json.loads(texto).get("indices_selecionados", [])
        return [candidatos[i] for i in indices if i < len(candidatos)]
    except Exception as e:
        print(f"[obs3] Erro ao processar similaridade: {e}")
        return candidatos[:max_resultados]


def buscar_resolucao_ticket(ticket_id):
    """Busca a última mensagem ou e-mail do analista como resolução."""
    ultimo_email = buscar_ultimo_email_analista(ticket_id)
    if ultimo_email and ultimo_email.get("texto"):
        return ultimo_email["texto"]
    thread_id = buscar_thread_conversa(ticket_id)
    if thread_id:
        ultima_msg = buscar_ultima_mensagem_analista(thread_id)
        if ultima_msg and ultima_msg.get("texto"):
            return ultima_msg["texto"]
    return None


def gerar_resumo_resolucao(demanda_atual, resolucao_texto):
    """Usa o Contexto.AI para gerar um resumo enxuto da resolução."""
    if not resolucao_texto:
        return "Resolução não documentada."

    prompt = f"""Você é um analista de suporte da EasyJur. Gere um resumo ENXUTO de 1 a 2 frases descrevendo como o problema foi resolvido, com base na última resposta do analista. Retorne APENAS um JSON válido. Não inclua explicações fora do JSON.

Demanda do cliente:
{demanda_atual}

Última resposta do analista:
{resolucao_texto}

Formato obrigatório:
{{"resumo": "Texto do resumo aqui."}}"""

    resposta = chamar_contexto_ai(prompt, task_name="gerar_resumo_obs3")
    if not resposta:
        return "Não foi possível gerar o resumo."
    try:
        texto = resposta.replace("```json", "").replace("```", "").strip()
        return json.loads(texto).get("resumo", "Resumo não disponível.")
    except Exception as e:
        print(f"[obs3] Erro ao processar resumo: {e}")
        return "Erro ao processar resumo."


def gerar_html_obs3(similares_empresa, similares_globais, demanda_atual, company_id):
    hoje = datetime.now(timezone.utc).strftime("%d/%m/%Y")

    html = "<p>🤖 <strong>[IA] SUGESTÕES DE RESOLUÇÃO</strong></p><hr>"

    # Bloco empresa
    if company_id and similares_empresa:
        html += f"<p>📌 <strong>HISTÓRICO DESTA EMPRESA (ID: {company_id})</strong></p>"
        for i, t in enumerate(similares_empresa):
            props = t.get("properties", {})
            ticket_id = t.get("id", "")
            subject = props.get("subject", "Sem título")
            resolucao_raw = buscar_resolucao_ticket(ticket_id)
            resumo = gerar_resumo_resolucao(demanda_atual, resolucao_raw)
            link = url_ticket(ticket_id)
            html += f"<p><strong>{i+1}º Caso:</strong> {subject}<br><em>{resumo}</em><br><a href=\"{link}\">🔗 Clique aqui para visualizar o ticket</a></p>"
    elif company_id:
        html += f"<p>📌 <strong>HISTÓRICO DESTA EMPRESA (ID: {company_id})</strong></p>"
        html += "<p><em>Nenhum ticket similar resolvido encontrado para esta empresa.</em></p>"
    else:
        html += "<p>📌 <strong>HISTÓRICO DESTA EMPRESA</strong></p>"
        html += "<p><em>Empresa não identificada neste ticket.</em></p>"

    html += "<hr>"

    # Bloco global
    html += "<p>📌 <strong>SUGESTÕES DA BASE GERAL</strong></p>"
    if similares_globais:
        for i, t in enumerate(similares_globais):
            props = t.get("properties", {})
            ticket_id = t.get("id", "")
            subject = props.get("subject", "Sem título")
            resolucao_raw = buscar_resolucao_ticket(ticket_id)
            resumo = gerar_resumo_resolucao(demanda_atual, resolucao_raw)
            link = url_ticket(ticket_id)
            html += f"<p><strong>{i+1}º Caso:</strong> {subject}<br><em>{resumo}</em><br><a href=\"{link}\">🔗 Clique aqui para visualizar o ticket</a></p>"
    else:
        html += "<p><em>Nenhum ticket similar resolvido encontrado na base geral.</em></p>"

    html += f"<hr><p><small>Busca automática realizada em {hoje}</small></p>"
    return html


def processar_obs3(ticket_id):
    print(f"[obs3] Iniciando para ticket {ticket_id}...")

    ticket = buscar_ticket(ticket_id)
    if not ticket:
        print(f"[obs3] Ticket {ticket_id} não encontrado. Abortando.")
        return False

    props = ticket.get("properties", {})
    demanda_atual = (
        props.get("demanda_apresentada_pelo_cliente", "")
        or props.get("content", "")
        or props.get("subject", "")
    )
    tipo_de_servico = props.get("tipo_de_servico", "")
    company_id = buscar_company_id(ticket_id)

    # Tickets similares da empresa
    similares_empresa = []
    if company_id:
        print(f"[obs3] Buscando tickets similares da empresa {company_id}...")
        candidatos_empresa = buscar_tickets_empresa(company_id, stage=STAGE_RESOLVIDO)
        candidatos_empresa = [t for t in candidatos_empresa if t.get("id") != str(ticket_id)]
        if candidatos_empresa and demanda_atual:
            similares_empresa = selecionar_similares(demanda_atual, candidatos_empresa, max_resultados=3)
        elif candidatos_empresa:
            similares_empresa = candidatos_empresa[:3]

    # Tickets similares globais
    print(f"[obs3] Buscando tickets similares globais...")
    candidatos_globais = buscar_tickets_resolvidos_globais(tipo_de_servico=tipo_de_servico)
    ids_ja_usados = {t.get("id") for t in similares_empresa} | {str(ticket_id)}
    candidatos_globais = [t for t in candidatos_globais if t.get("id") not in ids_ja_usados]

    similares_globais = []
    if candidatos_globais and demanda_atual:
        similares_globais = selecionar_similares(demanda_atual, candidatos_globais, max_resultados=3)
    elif candidatos_globais:
        similares_globais = candidatos_globais[:3]

    conteudo_html = gerar_html_obs3(similares_empresa, similares_globais, demanda_atual, company_id)
    sucesso = adicionar_observacao(ticket_id, "Observação 3 — Tickets Similares e Resolução", conteudo_html)

    if sucesso:
        print(f"[obs3] ✅ Observação 3 adicionada ao ticket {ticket_id}.")
    else:
        print(f"[obs3] ❌ Falha ao adicionar Observação 3 ao ticket {ticket_id}.")
    return sucesso
