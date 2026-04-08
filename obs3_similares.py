import os
import json
from datetime import datetime, timezone
from google import genai
from google.genai import types
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

GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
HUBSPOT_PORTAL_ID = "44225969"


# --- GEMINI ---

def configurar_gemini():
    keys_str = os.environ.get("GEMINI_API_KEYS", "")
    keys = [k.strip() for k in keys_str.split(",") if k.strip()]
    if not keys:
        print("[obs3] ERRO: Nenhuma chave Gemini encontrada.")
        return None
    return genai.Client(api_key=keys[0])


def chamar_gemini(client, prompt):
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json"
            )
        )
        return response.text.strip().replace("```json", "").replace("```", "")
    except Exception as e:
        print(f"[obs3] Erro na chamada ao Gemini: {e}")
        return None


def url_ticket(ticket_id):
    return f"https://app.hubspot.com/contacts/{HUBSPOT_PORTAL_ID}/ticket/{ticket_id}"


def selecionar_similares_com_gemini(client, demanda_atual, candidatos, max_resultados=3):
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

    prompt = f"""
Você é um analisador de similaridade de tickets de suporte.

Ticket atual — demanda do cliente:
\"\"\"{demanda_atual}\"\"\"

Abaixo estão {len(candidatos_formatados)} tickets candidatos. Selecione os {max_resultados} mais similares ao ticket atual com base na demanda apresentada.

Candidatos:
{json.dumps(candidatos_formatados, ensure_ascii=False, indent=2)}

Responda APENAS com um JSON no formato:
{{
  "indices_selecionados": [0, 2, 5]
}}

Os índices devem corresponder ao campo "indice" dos candidatos selecionados, ordenados do mais similar para o menos similar.
"""
    texto = chamar_gemini(client, prompt)
    if not texto:
        return candidatos[:max_resultados]
    try:
        resultado = json.loads(texto)
        indices = resultado.get("indices_selecionados", [])
        return [candidatos[i] for i in indices if i < len(candidatos)]
    except Exception as e:
        print(f"[obs3] Erro ao processar similaridade: {e}")
        return candidatos[:max_resultados]


def buscar_resolucao_ticket(ticket_id):
    ultimo_email = buscar_ultimo_email_analista(ticket_id)
    if ultimo_email and ultimo_email.get("texto"):
        return ultimo_email["texto"]

    thread_id = buscar_thread_conversa(ticket_id)
    if thread_id:
        ultima_msg = buscar_ultima_mensagem_analista(thread_id)
        if ultima_msg and ultima_msg.get("texto"):
            return ultima_msg["texto"]

    return None


def gerar_resumo_com_gemini(client, demanda_atual, resolucao_texto):
    """Gera um resumo enxuto de como o problema foi resolvido."""
    if not resolucao_texto:
        return "Resolução não documentada."

    prompt = f"""
Você é um analista de suporte especialista em sintetizar resoluções de tickets.

Demanda do cliente:
\"\"\"{demanda_atual}\"\"\"

Última resposta do analista para o cliente:
\"\"\"{resolucao_texto}\"\"\"

Gere um resumo ENXUTO e OBJETIVO (máximo 2-3 frases) de como o problema foi resolvido, focando na ação tomada pelo analista. Escreva de forma direta, sem floreios.

Responda APENAS com um JSON no formato:
{{
  "resumo": "Texto do resumo aqui."
}}
"""
    texto = chamar_gemini(client, prompt)
    if not texto:
        return "Não foi possível gerar o resumo."
    try:
        resultado = json.loads(texto)
        return resultado.get("resumo", "Não foi possível gerar o resumo.")
    except Exception as e:
        print(f"[obs3] Erro ao processar resumo: {e}")
        return "Erro ao processar resumo."


def ordinal(n):
    """Retorna 1º, 2º, 3º etc."""
    return f"{n}º"


def gerar_html_obs3(similares_empresa, similares_globais, demanda_atual, client, company_id):
    """Gera o HTML da Observação 3 no formato visual definido."""
    hoje = datetime.now(timezone.utc).strftime("%d/%m/%Y")

    # --- BLOCO EMPRESA ---
    empresa_html = ""
    if company_id and similares_empresa:
        itens_empresa = ""
        for i, t in enumerate(similares_empresa):
            props = t.get("properties", {})
            ticket_id = t.get("id", "")
            subject = props.get("subject", "Sem título")
            resolucao_raw = buscar_resolucao_ticket(ticket_id)
            resumo = gerar_resumo_com_gemini(client, demanda_atual, resolucao_raw)
            link = url_ticket(ticket_id)
            itens_empresa += f"""
<p><strong>{ordinal(i+1)} Caso:</strong> {subject}<br>
<em>{resumo}</em><br>
<a href="{link}">🔗 Clique aqui para visualizar o ticket</a></p>
"""
        empresa_html = f"""
<p>📌 <strong>HISTÓRICO DESTA EMPRESA (ID: {company_id})</strong></p>
{itens_empresa}
<hr>
"""
    elif company_id:
        empresa_html = f"""
<p>📌 <strong>HISTÓRICO DESTA EMPRESA (ID: {company_id})</strong></p>
<p><em>Nenhum ticket similar resolvido encontrado para esta empresa.</em></p>
<hr>
"""
    else:
        empresa_html = """
<p>📌 <strong>HISTÓRICO DESTA EMPRESA</strong></p>
<p><em>Empresa não identificada neste ticket.</em></p>
<hr>
"""

    # --- BLOCO GLOBAL ---
    global_html = ""
    if similares_globais:
        itens_global = ""
        for i, t in enumerate(similares_globais):
            props = t.get("properties", {})
            ticket_id = t.get("id", "")
            subject = props.get("subject", "Sem título")
            resolucao_raw = buscar_resolucao_ticket(ticket_id)
            resumo = gerar_resumo_com_gemini(client, demanda_atual, resolucao_raw)
            link = url_ticket(ticket_id)
            itens_global += f"""
<p><strong>{ordinal(i+1)} Caso:</strong> {subject}<br>
<em>{resumo}</em><br>
<a href="{link}">🔗 Clique aqui para visualizar o ticket</a></p>
"""
        global_html = f"""
<p>📌 <strong>SUGESTÕES DA BASE GERAL</strong></p>
{itens_global}
"""
    else:
        global_html = """
<p>📌 <strong>SUGESTÕES DA BASE GERAL</strong></p>
<p><em>Nenhum ticket similar resolvido encontrado na base geral.</em></p>
"""

    return f"""
<p>🤖 <strong>[IA] SUGESTÕES DE RESOLUÇÃO</strong></p>
<hr>
{empresa_html}
{global_html}
<p><small>Busca automática realizada em {hoje}</small></p>
"""


def processar_obs3(ticket_id):
    print(f"[obs3] Iniciando para ticket {ticket_id}...")

    client = configurar_gemini()
    if not client:
        return False

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

    # --- TICKETS SIMILARES DA EMPRESA ---
    similares_empresa = []
    if company_id:
        print(f"[obs3] Buscando tickets similares da empresa {company_id}...")
        candidatos_empresa = buscar_tickets_empresa(company_id, stage=STAGE_RESOLVIDO)
        candidatos_empresa = [t for t in candidatos_empresa if t.get("id") != str(ticket_id)]

        if candidatos_empresa and demanda_atual:
            similares_empresa = selecionar_similares_com_gemini(client, demanda_atual, candidatos_empresa, max_resultados=3)
        elif candidatos_empresa:
            similares_empresa = candidatos_empresa[:3]
    else:
        print(f"[obs3] Empresa não identificada para ticket {ticket_id}.")

    # --- TICKETS SIMILARES GLOBAIS ---
    print(f"[obs3] Buscando tickets similares globais...")
    candidatos_globais = buscar_tickets_resolvidos_globais(tipo_de_servico=tipo_de_servico)
    ids_ja_usados = {t.get("id") for t in similares_empresa} | {str(ticket_id)}
    candidatos_globais = [t for t in candidatos_globais if t.get("id") not in ids_ja_usados]

    similares_globais = []
    if candidatos_globais and demanda_atual:
        similares_globais = selecionar_similares_com_gemini(client, demanda_atual, candidatos_globais, max_resultados=3)
    elif candidatos_globais:
        similares_globais = candidatos_globais[:3]

    # Gera e adiciona a observação
    conteudo_html = gerar_html_obs3(similares_empresa, similares_globais, demanda_atual, client, company_id)
    sucesso = adicionar_observacao(
        ticket_id,
        "Observação 3 — Tickets Similares e Resolução",
        conteudo_html
    )

    if sucesso:
        print(f"[obs3] ✅ Observação 3 adicionada ao ticket {ticket_id}.")
    else:
        print(f"[obs3] ❌ Falha ao adicionar Observação 3 ao ticket {ticket_id}.")

    return sucesso
