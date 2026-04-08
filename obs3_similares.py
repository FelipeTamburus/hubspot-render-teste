import os
import json
import google.generativeai as genai
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

# --- GEMINI ---

def configurar_gemini():
    """Configura o Gemini com as chaves disponíveis."""
    keys_str = os.environ.get("GEMINI_API_KEYS", "")
    keys = [k.strip() for k in keys_str.split(",") if k.strip()]
    if not keys:
        print("[obs3] ERRO: Nenhuma chave Gemini encontrada.")
        return None
    genai.configure(api_key=keys[0])
    return genai.GenerativeModel(
        model_name=os.environ.get("GEMINI_MODEL", "gemini-1.5-flash-latest"),
        generation_config={"response_mime_type": "application/json"}
    )


def selecionar_similares_com_gemini(model, demanda_atual, candidatos, max_resultados=3):
    """
    Usa o Gemini para selecionar os tickets mais similares
    com base na demanda apresentada pelo cliente.
    """
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
    try:
        response = model.generate_content(prompt)
        texto = response.text.strip().replace("```json", "").replace("```", "")
        resultado = json.loads(texto)
        indices = resultado.get("indices_selecionados", [])
        return [candidatos[i] for i in indices if i < len(candidatos)]
    except Exception as e:
        print(f"[obs3] Erro ao usar Gemini para similaridade: {e}")
        return candidatos[:max_resultados]


def buscar_resolucao_ticket(ticket_id):
    """
    Busca como o ticket foi resolvido: último e-mail ou última mensagem do analista.
    """
    # Tenta e-mail primeiro
    ultimo_email = buscar_ultimo_email_analista(ticket_id)
    if ultimo_email and ultimo_email.get("texto"):
        return ultimo_email["texto"]

    # Tenta mensagem do chat
    thread_id = buscar_thread_conversa(ticket_id)
    if thread_id:
        ultima_msg = buscar_ultima_mensagem_analista(thread_id)
        if ultima_msg and ultima_msg.get("texto"):
            return ultima_msg["texto"]

    return None


def gerar_resumo_com_gemini(model, demanda_atual, tickets_similares, contexto="empresa"):
    """
    Gera um resumo consolidado de como tickets similares foram resolvidos.
    """
    if not tickets_similares:
        return "Nenhum ticket similar encontrado para gerar resumo."

    tickets_formatados = []
    for t in tickets_similares:
        props = t.get("properties", {})
        ticket_id = t.get("id", "")
        resolucao = buscar_resolucao_ticket(ticket_id)
        tickets_formatados.append({
            "subject": props.get("subject", ""),
            "demanda": props.get("demanda_apresentada_pelo_cliente", ""),
            "resolucao": resolucao or "Resolução não documentada"
        })

    prompt = f"""
Você é um analista de suporte especialista em sintetizar resoluções de tickets.

Ticket atual — demanda do cliente:
\"\"\"{demanda_atual}\"\"\"

Abaixo estão tickets similares resolvidos ({contexto}). Gere um resumo ENXUTO e OBJETIVO de como esse tipo de problema foi resolvido, destacando apenas as informações mais relevantes para o analista agir rapidamente.

Tickets similares:
{json.dumps(tickets_formatados, ensure_ascii=False, indent=2)}

Responda APENAS com um JSON no formato:
{{
  "resumo": "Texto do resumo aqui, em HTML simples com <p>, <strong> e <br> se necessário."
}}
"""
    try:
        response = model.generate_content(prompt)
        texto = response.text.strip().replace("```json", "").replace("```", "")
        resultado = json.loads(texto)
        return resultado.get("resumo", "Não foi possível gerar o resumo.")
    except Exception as e:
        print(f"[obs3] Erro ao gerar resumo com Gemini: {e}")
        return "Erro ao gerar resumo automático."


def gerar_html_obs3(similares_empresa, resumo_empresa, similares_globais, resumo_global):
    """Gera o HTML da Observação 3."""

    def formatar_lista(tickets):
        if not tickets:
            return "<li>Nenhum ticket similar encontrado.</li>"
        itens = ""
        for t in tickets:
            props = t.get("properties", {})
            subject = props.get("subject", "Sem título")
            data = props.get("createdate", "")[:10] if props.get("createdate") else ""
            itens += f"<li><strong>{subject}</strong> ({data})</li>"
        return itens

    empresa_html = f"""
<p><strong>Tickets similares da mesma empresa:</strong></p>
<ul>{formatar_lista(similares_empresa)}</ul>
<p><strong>Como foi resolvido (empresa):</strong></p>
{resumo_empresa}
"""

    global_html = f"""
<p><strong>Tickets similares globais:</strong></p>
<ul>{formatar_lista(similares_globais)}</ul>
<p><strong>Como foi resolvido (global):</strong></p>
{resumo_global}
"""

    return empresa_html + "<hr>" + global_html


def processar_obs3(ticket_id):
    """
    Função principal da Observação 3.
    Roda apenas após Obs 1 e Obs 2 sinalizarem conclusão no Redis.
    Busca tickets similares e gera resumo de resolução.
    """
    print(f"[obs3] Iniciando para ticket {ticket_id}...")

    model = configurar_gemini()
    if not model:
        return False

    # Busca dados do ticket atual
    ticket = buscar_ticket(ticket_id)
    if not ticket:
        print(f"[obs3] Ticket {ticket_id} não encontrado. Abortando.")
        return False

    props = ticket.get("properties", {})
    demanda_atual = props.get("demanda_apresentada_pelo_cliente", "") or props.get("content", "") or props.get("subject", "")
    tipo_de_servico = props.get("tipo_de_servico", "")
    company_id = buscar_company_id(ticket_id)

    # --- TICKETS SIMILARES DA EMPRESA ---
    similares_empresa = []
    resumo_empresa = "<p>Empresa não identificada neste ticket.</p>"

    if company_id:
        print(f"[obs3] Buscando tickets similares da empresa {company_id}...")
        candidatos_empresa = buscar_tickets_empresa(company_id, stage=STAGE_RESOLVIDO)
        # Remove o próprio ticket da lista
        candidatos_empresa = [t for t in candidatos_empresa if t.get("id") != str(ticket_id)]

        if candidatos_empresa and demanda_atual:
            similares_empresa = selecionar_similares_com_gemini(model, demanda_atual, candidatos_empresa, max_resultados=3)
        elif candidatos_empresa:
            similares_empresa = candidatos_empresa[:3]

        if similares_empresa:
            resumo_empresa = gerar_resumo_com_gemini(model, demanda_atual, similares_empresa, "mesma empresa")
        else:
            resumo_empresa = "<p>Nenhum ticket similar resolvido encontrado para esta empresa.</p>"
    else:
        print(f"[obs3] Empresa não identificada para ticket {ticket_id}.")

    # --- TICKETS SIMILARES GLOBAIS ---
    print(f"[obs3] Buscando tickets similares globais...")
    candidatos_globais = buscar_tickets_resolvidos_globais(tipo_de_servico=tipo_de_servico)
    # Remove o próprio ticket e os já selecionados da empresa
    ids_ja_usados = {t.get("id") for t in similares_empresa} | {str(ticket_id)}
    candidatos_globais = [t for t in candidatos_globais if t.get("id") not in ids_ja_usados]

    similares_globais = []
    resumo_global = "<p>Nenhum ticket similar resolvido encontrado globalmente.</p>"

    if candidatos_globais and demanda_atual:
        similares_globais = selecionar_similares_com_gemini(model, demanda_atual, candidatos_globais, max_resultados=3)
    elif candidatos_globais:
        similares_globais = candidatos_globais[:3]

    if similares_globais:
        resumo_global = gerar_resumo_com_gemini(model, demanda_atual, similares_globais, "global")

    # Gera e adiciona a observação
    conteudo_html = gerar_html_obs3(similares_empresa, resumo_empresa, similares_globais, resumo_global)
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