import os
import json
import datetime
import requests
from contexto_ai_client import chamar_contexto_ai

# --- CONFIG ---
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN_HUBSPOT")
HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

EMAIL_HOST = os.environ.get("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = int(os.environ.get("EMAIL_PORT", 587))
EMAIL_USER = os.environ.get("EMAIL_USER")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD")

DESTINATARIOS = [
    "jvitoriano@easyjur.com",
    "pgalvao@easyjur.com",
    "lamaral@easyjur.com"
]

PIPELINE_SUPORTE_ID = "0"


# --- BUSCA DE DADOS ---

def buscar_tickets_semana():
    """Busca todos os tickets criados nos últimos 7 dias na pipeline de suporte."""
    agora = datetime.datetime.now(datetime.timezone.utc)
    sete_dias_atras = agora - datetime.timedelta(days=7)
    timestamp_ms = int(sete_dias_atras.timestamp() * 1000)

    url = "https://api.hubapi.com/crm/v3/objects/tickets/search"
    todos = []
    after = None

    while True:
        payload = {
            "filterGroups": [{
                "filters": [
                    {"propertyName": "hs_pipeline", "operator": "EQ", "value": PIPELINE_SUPORTE_ID},
                    {"propertyName": "createdate", "operator": "GTE", "value": str(timestamp_ms)}
                ]
            }],
            "properties": [
                "subject", "hs_pipeline_stage", "hs_object_source",
                "createdate", "hs_lastmodifieddate",
                "modulo_assunto", "classificacao_do_atendimento",
                "plano_contratado_easyjur", "id_empresa_ej",
                "servico"
            ],
            "sorts": [{"propertyName": "createdate", "direction": "DESCENDING"}],
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
            print(f"[relatorio] Erro ao buscar tickets: {e}")
            break

    return todos


def buscar_tickets_semana_anterior():
    """Busca total de tickets da semana anterior para comparativo."""
    agora = datetime.datetime.now(datetime.timezone.utc)
    inicio = agora - datetime.timedelta(days=14)
    fim = agora - datetime.timedelta(days=7)

    url = "https://api.hubapi.com/crm/v3/objects/tickets/search"
    payload = {
        "filterGroups": [{
            "filters": [
                {"propertyName": "hs_pipeline", "operator": "EQ", "value": PIPELINE_SUPORTE_ID},
                {"propertyName": "createdate", "operator": "GTE", "value": str(int(inicio.timestamp() * 1000))},
                {"propertyName": "createdate", "operator": "LTE", "value": str(int(fim.timestamp() * 1000))}
            ]
        }],
        "properties": ["subject"],
        "limit": 1
    }
    try:
        response = requests.post(url, headers=HEADERS, json=payload, timeout=15)
        response.raise_for_status()
        return response.json().get("total", 0)
    except Exception:
        return 0


# --- ANÁLISE ---

def analisar_tickets(tickets):
    """Organiza os dados dos tickets em métricas."""
    total = len(tickets)

    # Canal
    canais = {"Chat": 0, "E-mail/Formulário": 0}
    for t in tickets:
        source = t.get("properties", {}).get("hs_object_source", "")
        subject = t.get("properties", {}).get("subject", "")
        if "CHAT" in source.upper() or "bot" in subject.lower():
            canais["Chat"] += 1
        else:
            canais["E-mail/Formulário"] += 1

    # Prioridade (estágio atual)
    estagios = {}
    mapa_estagios = {
        "1": "Novo",
        "1338631792": "🔴 Atendimento — P. Alta",
        "1338631793": "🟡 Atendimento — P. Média",
        "1338631794": "🟢 Atendimento — P. Baixa",
        "3": "🟣 Retorno — Pós Atendimento",
        "164386119": "❇️ Resolvido"
    }
    for t in tickets:
        stage = t.get("properties", {}).get("hs_pipeline_stage", "")
        nome = mapa_estagios.get(stage, stage or "Desconhecido")
        estagios[nome] = estagios.get(nome, 0) + 1

    # Módulos
    modulos = {}
    for t in tickets:
        modulo = t.get("properties", {}).get("modulo_assunto", "") or "Não identificado"
        modulos[modulo] = modulos.get(modulo, 0) + 1
    modulos_ordenados = sorted(modulos.items(), key=lambda x: x[1], reverse=True)

    # Tipo de serviço
    servicos = {}
    for t in tickets:
        servico = t.get("properties", {}).get("servico", "") or "Não identificado"
        servicos[servico] = servicos.get(servico, 0) + 1
    servicos_ordenados = sorted(servicos.items(), key=lambda x: x[1], reverse=True)[:6]

    # Planos
    planos = {}
    for t in tickets:
        plano = t.get("properties", {}).get("plano_contratado_easyjur", "") or "Não identificado"
        planos[plano] = planos.get(plano, 0) + 1
    planos_ordenados = sorted(planos.items(), key=lambda x: x[1], reverse=True)

    # Empresas com mais tickets
    empresas = {}
    for t in tickets:
        empresa = t.get("properties", {}).get("id_empresa_ej", "") or "Não identificada"
        empresas[empresa] = empresas.get(empresa, 0) + 1
    top_empresas = sorted(empresas.items(), key=lambda x: x[1], reverse=True)[:5]

    return {
        "total": total,
        "canais": canais,
        "estagios": estagios,
        "modulos": modulos_ordenados[:6],
        "servicos": servicos_ordenados,
        "planos": planos_ordenados[:5],
        "top_empresas": top_empresas
    }


# --- CONTEXTO.AI ---

def gerar_resumo_executivo(metricas, total_semana_anterior):
    """Chama o Contexto.AI para gerar um resumo executivo dos dados."""
    variacao = metricas["total"] - total_semana_anterior
    variacao_str = f"+{variacao}" if variacao > 0 else str(variacao)

    prompt = f"""Você é um analista de suporte técnico da EasyJur.
Analise os dados da semana de suporte e gere um resumo executivo em português, direto e profissional.

DADOS DA SEMANA:
- Total de tickets: {metricas['total']} ({variacao_str} vs semana anterior com {total_semana_anterior} tickets)
- Por canal: {json.dumps(metricas['canais'], ensure_ascii=False)}
- Por estágio atual: {json.dumps(metricas['estagios'], ensure_ascii=False)}
- Módulos mais acionados: {json.dumps(metricas['modulos'], ensure_ascii=False)}
- Tipos de serviço mais solicitados: {json.dumps(metricas['servicos'], ensure_ascii=False)}
- Por plano: {json.dumps(metricas['planos'], ensure_ascii=False)}
- Top 5 empresas por volume: {json.dumps(metricas['top_empresas'], ensure_ascii=False)}

Gere:
1. Um parágrafo de resumo executivo (3-4 frases) com os principais destaques
2. Até 3 pontos de atenção ou oportunidades identificadas nos dados
3. Uma recomendação prática para a semana seguinte

Responda em texto corrido, sem markdown, sem asteriscos, sem bullets. Use parágrafos separados por quebra de linha dupla para cada seção."""

    try:
        resposta = chamar_contexto_ai(prompt, task_name="relatorio_semanal")
        return resposta.strip() if resposta else "Resumo não disponível."
    except Exception as e:
        print(f"[relatorio] Erro ao chamar Contexto.AI: {e}")
        return "Resumo não disponível esta semana."


# --- HTML DO E-MAIL ---

def montar_html(metricas, resumo, periodo):
    """Monta o HTML do e-mail com branding EasyJur."""

    def barra(valor, total, cor):
        pct = round((valor / total) * 100) if total > 0 else 0
        return f'<div style="background:#F3F4F6;border-radius:6px;height:8px;margin-top:4px;"><div style="background:{cor};width:{pct}%;height:8px;border-radius:6px;"></div></div>'

    # Módulos
    modulos_html = ""
    cores_mod = ["#E5293F", "#F59E0B", "#3B82F6", "#10B981", "#8B5CF6", "#EC4899"]
    for i, (mod, qtd) in enumerate(metricas["modulos"]):
        cor = cores_mod[i % len(cores_mod)]
        pct = round((qtd / metricas["total"]) * 100) if metricas["total"] > 0 else 0
        modulos_html += f"""
        <tr>
          <td style="padding:8px 0;font-size:13px;color:#191919;font-weight:500;">{mod.title()}</td>
          <td style="padding:8px 0;text-align:right;font-size:13px;font-weight:700;color:{cor};">{qtd} <span style="color:#ACBAC2;font-weight:400;">({pct}%)</span></td>
        </tr>"""

    # Estágios
    estagios_html = ""
    cores_est = {
        "🔴 Atendimento — P. Alta": "#E5293F",
        "🟡 Atendimento — P. Média": "#F59E0B",
        "🟢 Atendimento — P. Baixa": "#10B981",
        "🟣 Retorno — Pós Atendimento": "#8B5CF6",
        "❇️ Resolvido": "#06B6D4",
        "Novo": "#3B82F6"
    }
    for estagio, qtd in sorted(metricas["estagios"].items(), key=lambda x: x[1], reverse=True):
        cor = cores_est.get(estagio, "#ACBAC2")
        pct = round((qtd / metricas["total"]) * 100) if metricas["total"] > 0 else 0
        estagios_html += f"""
        <tr>
          <td style="padding:6px 0;font-size:13px;color:#191919;">{estagio}</td>
          <td style="padding:6px 0;text-align:right;font-size:13px;font-weight:700;color:{cor};">{qtd}</td>
          <td style="padding:6px 12px;width:120px;">
            <div style="background:#F3F4F6;border-radius:4px;height:6px;">
              <div style="background:{cor};width:{pct}%;height:6px;border-radius:4px;"></div>
            </div>
          </td>
        </tr>"""

    # Tipo de serviço
    servicos_html = ""
    cores_serv = ["#E5293F", "#F59E0B", "#3B82F6", "#10B981", "#8B5CF6", "#EC4899"]
    for i, (serv, qtd) in enumerate(metricas["servicos"]):
        cor = cores_serv[i % len(cores_serv)]
        pct = round((qtd / metricas["total"]) * 100) if metricas["total"] > 0 else 0
        servicos_html += f"""
        <tr>
          <td style="padding:8px 0;font-size:13px;color:#191919;font-weight:500;">{serv}</td>
          <td style="padding:8px 0;text-align:right;font-size:13px;font-weight:700;color:{cor};">{qtd} <span style="color:#ACBAC2;font-weight:400;">({pct}%)</span></td>
        </tr>"""

    # Resumo em parágrafos
    paragrafos = resumo.split("\n\n")
    resumo_html = "".join(f'<p style="margin:0 0 14px;font-size:14px;color:#374151;line-height:1.75;">{p.strip()}</p>' for p in paragrafos if p.strip())

    # Chat vs Email
    chat_qtd = metricas["canais"].get("Chat", 0)
    email_qtd = metricas["canais"].get("E-mail/Formulário", 0)
    chat_pct = round((chat_qtd / metricas["total"]) * 100) if metricas["total"] > 0 else 0
    email_pct = 100 - chat_pct

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Relatório Semanal de Suporte — EasyJur</title></head>
<body style="margin:0;padding:0;background:#F3F4F6;font-family:'Helvetica Neue',Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#F3F4F6;padding:32px 16px;">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;">

  <!-- HEADER -->
  <tr><td style="background:#191919;border-radius:14px 14px 0 0;padding:28px 32px;">
    <table width="100%" cellpadding="0" cellspacing="0">
      <tr>
        <td>
          <div style="display:inline-flex;align-items:center;gap:10px;">
            <div style="background:#E5293F;width:34px;height:34px;border-radius:8px;display:inline-block;text-align:center;line-height:34px;color:#fff;font-weight:800;font-size:17px;">E</div>
            <span style="color:#fff;font-weight:700;font-size:16px;margin-left:10px;">Easy<span style="color:#E5293F;">Jur</span></span>
          </div>
          <div style="margin-top:16px;">
            <span style="background:rgba(229,41,63,0.15);color:#E5293F;font-size:11px;font-weight:700;padding:3px 12px;border-radius:20px;text-transform:uppercase;letter-spacing:0.05em;">Relatório Semanal</span>
          </div>
          <h1 style="color:#fff;font-size:22px;font-weight:800;margin:10px 0 4px;">Suporte ao Cliente</h1>
          <p style="color:#7F919A;font-size:13px;margin:0;">{periodo}</p>
        </td>
        <td align="right" valign="top">
          <div style="background:rgba(229,41,63,0.1);border:1px solid rgba(229,41,63,0.3);border-radius:12px;padding:16px 20px;text-align:center;display:inline-block;">
            <div style="color:#E5293F;font-size:36px;font-weight:800;line-height:1;">{metricas['total']}</div>
            <div style="color:#7F919A;font-size:11px;font-weight:600;margin-top:4px;">tickets na semana</div>
          </div>
        </td>
      </tr>
    </table>
  </td></tr>

  <!-- RESUMO EXECUTIVO -->
  <tr><td style="background:#fff;padding:28px 32px;border-left:1px solid #E5E7EB;border-right:1px solid #E5E7EB;">
    <p style="font-size:11px;font-weight:700;color:#ACBAC2;text-transform:uppercase;letter-spacing:0.08em;margin:0 0 14px;">Resumo Executivo</p>
    {resumo_html}
  </td></tr>

  <!-- CANAIS -->
  <tr><td style="background:#fff;padding:0 32px 24px;border-left:1px solid #E5E7EB;border-right:1px solid #E5E7EB;">
    <div style="border-top:1px solid #E5E7EB;padding-top:24px;">
      <p style="font-size:11px;font-weight:700;color:#ACBAC2;text-transform:uppercase;letter-spacing:0.08em;margin:0 0 16px;">Canal de origem</p>
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td width="48%" style="background:#F9FAFB;border-radius:10px;padding:16px;text-align:center;">
            <div style="font-size:28px;font-weight:800;color:#191919;">{chat_qtd}</div>
            <div style="font-size:12px;color:#7F919A;margin-top:2px;">Chat</div>
            <div style="font-size:11px;font-weight:700;color:#E5293F;margin-top:4px;">{chat_pct}%</div>
          </td>
          <td width="4%"></td>
          <td width="48%" style="background:#F9FAFB;border-radius:10px;padding:16px;text-align:center;">
            <div style="font-size:28px;font-weight:800;color:#191919;">{email_qtd}</div>
            <div style="font-size:12px;color:#7F919A;margin-top:2px;">E-mail / Formulário</div>
            <div style="font-size:11px;font-weight:700;color:#3B82F6;margin-top:4px;">{email_pct}%</div>
          </td>
        </tr>
      </table>
    </div>
  </td></tr>

  <!-- MÓDULOS -->
  <tr><td style="background:#fff;padding:0 32px 24px;border-left:1px solid #E5E7EB;border-right:1px solid #E5E7EB;">
    <div style="border-top:1px solid #E5E7EB;padding-top:24px;">
      <p style="font-size:11px;font-weight:700;color:#ACBAC2;text-transform:uppercase;letter-spacing:0.08em;margin:0 0 8px;">Módulos mais acionados</p>
      <table width="100%" cellpadding="0" cellspacing="0">
        {modulos_html}
      </table>
    </div>
  </td></tr>

  <!-- TIPO DE SERVIÇO -->
  <tr><td style="background:#fff;padding:0 32px 24px;border-left:1px solid #E5E7EB;border-right:1px solid #E5E7EB;">
    <div style="border-top:1px solid #E5E7EB;padding-top:24px;">
      <p style="font-size:11px;font-weight:700;color:#ACBAC2;text-transform:uppercase;letter-spacing:0.08em;margin:0 0 8px;">Tipos de serviço mais solicitados</p>
      <table width="100%" cellpadding="0" cellspacing="0">
        {servicos_html}
      </table>
    </div>
  </td></tr>

  <!-- ESTÁGIOS -->
  <tr><td style="background:#fff;padding:0 32px 24px;border-left:1px solid #E5E7EB;border-right:1px solid #E5E7EB;">
    <div style="border-top:1px solid #E5E7EB;padding-top:24px;">
      <p style="font-size:11px;font-weight:700;color:#ACBAC2;text-transform:uppercase;letter-spacing:0.08em;margin:0 0 8px;">Distribuição por prioridade</p>
      <table width="100%" cellpadding="0" cellspacing="0">
        {estagios_html}
      </table>
    </div>
  </td></tr>

  <!-- FOOTER -->
  <tr><td style="background:#F9FAFB;border:1px solid #E5E7EB;border-top:none;border-radius:0 0 14px 14px;padding:20px 32px;text-align:center;">
    <p style="font-size:12px;color:#ACBAC2;margin:0;">EasyJur · Relatório gerado automaticamente toda segunda-feira às 08:00</p>
    <p style="font-size:12px;color:#ACBAC2;margin:6px 0 0;">Para dúvidas ou ajustes, contate o time de tecnologia.</p>
  </td></tr>

</table>
</td></tr>
</table>
</body>
</html>"""


# --- ENVIO ---

def enviar_email(html, periodo):
    """Envia o e-mail via SendGrid API."""
    api_key = os.environ.get("SENDGRID_API_KEY")
    if not api_key:
        print("[relatorio] ❌ SENDGRID_API_KEY não configurada.")
        return False

    payload = {
        "personalizations": [{
            "to": [{"email": e} for e in DESTINATARIOS]
        }],
        "from": {"email": EMAIL_USER, "name": "EasyJur Suporte"},
        "subject": f"📊 Relatório Semanal de Suporte — {periodo}",
        "content": [{"type": "text/html", "value": html}]
    }

    try:
        response = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            },
            json=payload,
            timeout=15
        )
        if response.status_code in [200, 202]:
            print(f"[relatorio] ✅ E-mail enviado via SendGrid para {DESTINATARIOS}")
            return True
        else:
            print(f"[relatorio] ❌ SendGrid retornou {response.status_code}: {response.text}")
            return False
    except Exception as e:
        print(f"[relatorio] ❌ Erro ao enviar via SendGrid: {e}")
        return False


# --- FUNÇÃO PRINCIPAL ---

def gerar_e_enviar_relatorio():
    """Função principal — busca dados, gera resumo e envia o e-mail."""
    agora = datetime.datetime.now(datetime.timezone.utc)
    inicio_semana = agora - datetime.timedelta(days=7)
    periodo = f"{inicio_semana.strftime('%d/%m')} a {agora.strftime('%d/%m/%Y')}"

    print(f"[relatorio] Iniciando geração do relatório — período: {periodo}")

    # 1. Busca dados
    tickets = buscar_tickets_semana()
    total_anterior = buscar_tickets_semana_anterior()
    print(f"[relatorio] {len(tickets)} tickets encontrados na semana.")

    if not tickets:
        print("[relatorio] Nenhum ticket na semana. Abortando envio.")
        return False

    # 2. Analisa métricas
    metricas = analisar_tickets(tickets)

    # 3. Gera resumo com Contexto.AI
    print("[relatorio] Gerando resumo executivo com Contexto.AI...")
    resumo = gerar_resumo_executivo(metricas, total_anterior)

    # 4. Monta HTML
    html = montar_html(metricas, resumo, periodo)

    # 5. Envia
    return enviar_email(html, periodo)
