import os
import re
import json
from datetime import datetime, timezone
from contexto_ai_client import chamar_contexto_ai
from hubspot_client import (
    buscar_id_empresa_ej,
    buscar_ticket,
    buscar_company_id,
    buscar_tickets_empresa,
    buscar_tickets_resolvidos_globais,
    buscar_ultimo_email_analista,
    buscar_thread_conversa,
    buscar_ultima_mensagem_analista,
    buscar_mensagens_chat,
    adicionar_observacao,
    obs_ja_criada,
    marcar_obs_criada,
    STAGE_RESOLVIDO,
    REMETENTES_BOT
)

HUBSPOT_PORTAL_ID = "44225969"

STOPWORDS = {
    "de", "da", "do", "em", "no", "na", "para", "com", "por", "um", "uma",
    "os", "as", "que", "se", "ao", "dos", "das", "nos", "nas", "e", "o", "a",
    "é", "ou", "mas", "como", "mais", "foi", "ser", "tem", "não", "sobre",
    "isso", "este", "esta", "esse", "essa", "pelo", "pela", "está", "quando"
}


def url_ticket(ticket_id):
    return f"https://app.hubspot.com/contacts/{HUBSPOT_PORTAL_ID}/ticket/{ticket_id}"


def extrair_palavras_chave(texto):
    """Extrai palavras-chave relevantes de um texto removendo stopwords."""
    if not texto:
        return set()
    palavras = re.findall(r'\b\w{3,}\b', texto.lower())
    return {p for p in palavras if p not in STOPWORDS}


def calcular_score_similaridade(chaves_atual, candidato):
    """
    Calcula score de similaridade entre o ticket atual e um candidato
    verificando palavras-chave nos campos subject, tipo_de_servico e demanda.
    """
    props = candidato.get("properties", {})
    texto_candidato = " ".join(filter(None, [
        props.get("subject", ""),
        props.get("tipo_de_servico", ""),
        props.get("demanda_apresentada_pelo_cliente", ""),
        props.get("content", "")
    ]))
    chaves_candidato = extrair_palavras_chave(texto_candidato)
    if not chaves_atual or not chaves_candidato:
        return 0
    intersecao = chaves_atual & chaves_candidato
    uniao = chaves_atual | chaves_candidato
    return len(intersecao) / len(uniao) if uniao else 0


def pre_filtrar_candidatos(texto_atual, candidatos, top_n=5):
    """
    Pré-filtra candidatos por similaridade de palavras-chave.
    Retorna os top_n mais similares para enviar ao Contexto.AI.
    """
    chaves_atual = extrair_palavras_chave(texto_atual)
    scored = []
    for c in candidatos:
        score = calcular_score_similaridade(chaves_atual, c)
        scored.append((score, c))
    scored.sort(key=lambda x: x[0], reverse=True)
    filtrados = [c for score, c in scored if score > 0]
    if not filtrados:
        return candidatos[:top_n]
    return filtrados[:top_n]


def buscar_conteudo_ticket_atual(ticket_id, props):
    """
    Busca o conteúdo completo do ticket atual para extrair palavras-chave:
    subject + demanda + tipo_de_servico + mensagens do chat/formulário.
    """
    textos = [
        props.get("subject", ""),
        props.get("demanda_apresentada_pelo_cliente", ""),
        props.get("tipo_de_servico", ""),
        props.get("content", "")
    ]
    # Tenta buscar mensagens do chat
    thread_id = buscar_thread_conversa(ticket_id)
    if thread_id:
        mensagens = buscar_mensagens_chat(thread_id)
        for msg in mensagens:
            if msg.get("type") not in ["MESSAGE", "WELCOME_MESSAGE"]:
                continue
            remetente = msg.get("senders", [{}])[0]
            if remetente.get("name", "") in REMETENTES_BOT:
                continue
            creator = msg.get("createdBy", "")
            if creator.startswith("V-"):
                texto = msg.get("text", "").strip()
                if texto:
                    textos.append(texto)
    return " ".join(filter(None, textos))


def selecionar_similares(conteudo_atual, candidatos, max_resultados=3):
    """
    1. Pré-filtra por palavras-chave (Python) → top 5
    2. Manda os top 5 para o Contexto.AI selecionar os 3 melhores
    """
    if not candidatos:
        return []

    # Pré-filtro por palavras-chave
    pre_filtrados = pre_filtrar_candidatos(conteudo_atual, candidatos, top_n=5)
    print(f"[obs3] Pré-filtro: {len(candidatos)} candidatos → {len(pre_filtrados)} para o Contexto.AI.")

    candidatos_formatados = []
    for i, t in enumerate(pre_filtrados):
        props = t.get("properties", {})
        candidatos_formatados.append({
            "indice": i,
            "subject": props.get("subject", ""),
            "tipo_de_servico": props.get("tipo_de_servico", ""),
            "demanda": props.get("demanda_apresentada_pelo_cliente", ""),
            "conteudo": props.get("content", "")
        })

    prompt = f"""Você é um analista de suporte da EasyJur especialista em identificar similaridade entre tickets. Analise com ALTO GRAU DE RIGOR a similaridade entre o ticket atual e os candidatos abaixo. Retorne APENAS um JSON válido. Não inclua explicações fora do JSON.

Conteúdo do ticket atual:
{conteudo_atual[:1000]}

Candidatos:
{json.dumps(candidatos_formatados, ensure_ascii=False, indent=2)}

Selecione ATÉ {max_resultados} candidatos com ALTA similaridade ao ticket atual. Se nenhum candidato tiver alta similaridade, retorne uma lista vazia. Ordene do mais similar para o menos similar.

Critérios de alta similaridade:
- Mesmo tipo de problema ou módulo do sistema
- Demanda ou conteúdo semanticamente próximo
- Palavras-chave relevantes em comum

Formato obrigatório:
{{"indices_selecionados": [0, 2]}}"""

    resposta = chamar_contexto_ai(prompt, task_name="selecionar_similares_obs3")
    if not resposta:
        print("[obs3] Contexto.AI sem resposta. Usando pré-filtro como fallback.")
        return pre_filtrados[:max_resultados]
    try:
        texto = resposta.replace("```json", "").replace("```", "").strip()
        indices = json.loads(texto).get("indices_selecionados", [])
        selecionados = [pre_filtrados[i] for i in indices if i < len(pre_filtrados)]
        print(f"[obs3] Contexto.AI selecionou {len(selecionados)} similares.")
        return selecionados
    except Exception as e:
        print(f"[obs3] Erro ao processar similaridade: {e}")
        return pre_filtrados[:max_resultados]


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
{demanda_atual[:500]}

Última resposta do analista:
{resolucao_texto[:500]}

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
            html += (
                f"<p><strong>{i+1}º Caso:</strong> {subject}<br>"
                f"<em>{resumo}</em><br>"
                f"<a href=\"{link}\">🔗 Clique aqui para visualizar o ticket</a></p><br>"
            )
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
            html += (
                f"<p><strong>{i+1}º Caso:</strong> {subject}<br>"
                f"<em>{resumo}</em><br>"
                f"<a href=\"{link}\">🔗 Clique aqui para visualizar o ticket</a></p><br>"
            )
    else:
        html += "<p><em>Nenhum ticket similar resolvido encontrado na base geral.</em></p>"

    html += f"<hr><p><small>Busca automática realizada em {hoje}</small></p>"
    return html


def processar_obs3(ticket_id):
    print(f"[obs3] Iniciando para ticket {ticket_id}...")

    # Verifica se já foi criada para evitar duplicatas
    if obs_ja_criada(ticket_id, 3):
        print(f"[obs3] Obs 3 já criada para ticket {ticket_id}. Pulando.")
        return True

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
    company_ej_id = buscar_id_empresa_ej(ticket_id)
    company_id = buscar_company_id(ticket_id)

    # Busca conteúdo completo do ticket atual para palavras-chave
    conteudo_atual = buscar_conteudo_ticket_atual(ticket_id, props)

    # Tickets similares da empresa — usa id_empresa_ej para filtrar
    similares_empresa = []
    if company_ej_id:
        print(f"[obs3] Buscando tickets similares da empresa EJ {company_ej_id}...")
        candidatos_empresa = buscar_tickets_empresa(company_ej_id, stage=STAGE_RESOLVIDO)
        candidatos_empresa = [t for t in candidatos_empresa if t.get("id") != str(ticket_id)]
        if candidatos_empresa and conteudo_atual:
            similares_empresa = selecionar_similares(conteudo_atual, candidatos_empresa, max_resultados=3)
        elif candidatos_empresa:
            similares_empresa = candidatos_empresa[:3]

    # Tickets similares globais
    print(f"[obs3] Buscando tickets similares globais...")
    candidatos_globais = buscar_tickets_resolvidos_globais(tipo_de_servico=tipo_de_servico)
    ids_ja_usados = {t.get("id") for t in similares_empresa} | {str(ticket_id)}
    candidatos_globais = [t for t in candidatos_globais if t.get("id") not in ids_ja_usados]

    similares_globais = []
    if candidatos_globais and conteudo_atual:
        similares_globais = selecionar_similares(conteudo_atual, candidatos_globais, max_resultados=3)
    elif candidatos_globais:
        similares_globais = candidatos_globais[:3]

    conteudo_html = gerar_html_obs3(similares_empresa, similares_globais, demanda_atual, company_ej_id)
    sucesso = adicionar_observacao(ticket_id, "Observação 3 — Tickets Similares e Resolução", conteudo_html)

    if sucesso:
        marcar_obs_criada(ticket_id, 3)
        print(f"[obs3] ✅ Observação 3 adicionada ao ticket {ticket_id}.")
    else:
        print(f"[obs3] ❌ Falha ao adicionar Observação 3 ao ticket {ticket_id}.")
    return sucesso
