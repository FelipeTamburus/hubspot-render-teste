from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import os
import threading
import time
import requests
import redis
from obs1_contexto_empresa import processar_obs1
from obs2_dor_ticket import processar_obs2
from obs3_similares import processar_obs3
from categorizacao import processar_categorizacao

PIPELINE_SUPORTE_ID = "0"
STAGE_NOVO = "1"
FILA_OBS1 = "fila_obs1"
FILA_OBS2 = "fila_obs2"
FILA_CHAT = "fila_chat"
FILA_CATEG = "fila_categorizacao"
CHAT_TIMEOUT_HORAS = 24

r = redis.from_url(os.environ.get("REDIS_URL"))

ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN_HUBSPOT")
HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

ultima_varredura_chat = None  # registra quando foi a última varredura do worker_chat


# --- HELPERS ---

def verificar_ticket_elegivel(ticket_id):
    """Verifica se o ticket é da pipeline de suporte e está no estágio Novo."""
    url = f"https://api.hubapi.com/crm/v3/objects/tickets/{ticket_id}?properties=hs_pipeline,hs_pipeline_stage,hs_object_source,subject"
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        props = response.json().get("properties", {})
        pipeline = props.get("hs_pipeline", "")
        stage = props.get("hs_pipeline_stage", "")
        elegivel = pipeline == PIPELINE_SUPORTE_ID and stage == STAGE_NOVO
        return elegivel, props
    except Exception as e:
        print(f"[webhook] Erro ao verificar ticket {ticket_id}: {e}")
        return False, {}


def ticket_e_chat(props):
    """Verifica se o ticket veio por canal de chat."""
    source = props.get("hs_object_source", "")
    subject = props.get("subject", "")
    return "CHAT" in source.upper() or "bot" in subject.lower()


def buscar_thread_chat(ticket_id):
    """Busca a thread de conversa associada ao ticket."""
    url = f"https://api.hubapi.com/crm/v3/objects/tickets/{ticket_id}?associations=conversation"
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        resultados = response.json().get("associations", {}).get("conversations", {}).get("results", [])
        return resultados[0]["id"] if resultados else None
    except Exception as e:
        print(f"[chat] Erro ao buscar thread do ticket {ticket_id}: {e}")
        return None


def chat_esta_encerrado(thread_id):
    """Verifica se o chat está encerrado."""
    url = f"https://api.hubapi.com/conversations/v3/conversations/threads/{thread_id}"
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        status = response.json().get("status", "")
        return status in ["ENDED", "CLOSED", "ARCHIVED"]
    except Exception as e:
        print(f"[chat] Erro ao verificar status da thread {thread_id}: {e}")
        return False


# --- WORKERS ---

def worker_categorizacao():
    """Worker de categorização — 1º a rodar, define prioridade e move para coluna correta."""
    print("[worker_categ] Iniciado, aguardando tickets...")
    while True:
        try:
            resultado = r.brpop(FILA_CATEG, timeout=10)
            if resultado:
                _, ticket_id = resultado
                ticket_id = ticket_id.decode("utf-8")
                print(f"[worker_categ] Categorizando ticket {ticket_id}...")
                sucesso = processar_categorizacao(ticket_id)
                print(f"[worker_categ] Categorização {'✅' if sucesso else '❌'} para ticket {ticket_id}.")
        except Exception as e:
            print(f"[worker_categ] Erro: {e}")
            time.sleep(5)


def worker_obs1():
    """Worker da Observação 1 — contexto da empresa e churn."""
    print("[worker_obs1] Iniciado, aguardando tickets...")
    while True:
        try:
            resultado = r.brpop(FILA_OBS1, timeout=10)
            if resultado:
                _, dados = resultado
                dados = json.loads(dados.decode("utf-8"))
                ticket_id = dados["ticket_id"]
                e_chat = dados.get("e_chat", False)

                print(f"[worker_obs1] Processando ticket {ticket_id} (chat: {e_chat})...")
                sucesso = processar_obs1(ticket_id)
                r.set(f"obs1_concluida:{ticket_id}", "1", ex=86400)
                print(f"[worker_obs1] Obs1 {'✅' if sucesso else '❌'} para ticket {ticket_id}.")

                # Para chat: Obs 3 dispara após só a Obs 1
                # Para e-mail: Obs 3 dispara após Obs 1 + Obs 2
                if e_chat:
                    verificar_e_disparar_obs3(ticket_id, requer_obs2=False)
                else:
                    verificar_e_disparar_obs3(ticket_id, requer_obs2=True)

        except Exception as e:
            print(f"[worker_obs1] Erro: {e}")
            time.sleep(5)


def worker_obs2():
    """Worker da Observação 2 — dor do ticket (apenas e-mail/formulário)."""
    print("[worker_obs2] Iniciado, aguardando tickets...")
    while True:
        try:
            resultado = r.brpop(FILA_OBS2, timeout=10)
            if resultado:
                _, ticket_id = resultado
                ticket_id = ticket_id.decode("utf-8")
                print(f"[worker_obs2] Processando ticket {ticket_id}...")
                sucesso = processar_obs2(ticket_id)
                r.set(f"obs2_concluida:{ticket_id}", "1", ex=86400)
                print(f"[worker_obs2] Obs2 {'✅' if sucesso else '❌'} para ticket {ticket_id}.")
                verificar_e_disparar_obs3(ticket_id, requer_obs2=True)
        except Exception as e:
            print(f"[worker_obs2] Erro: {e}")
            time.sleep(5)


def worker_chat():
    """
    Rotina que roda a cada 1 hora verificando os chats na fila.
    Se o chat estiver encerrado, processa a Obs 2.
    Se passou 24h, processa com o que tem e remove da fila.
    """
    global ultima_varredura_chat
    print("[worker_chat] Iniciado, verificando chats a cada 30 minutos...")
    while True:
        try:
            time.sleep(1800)  # aguarda 30 minutos
            ultima_varredura_chat = time.time()
            print("[worker_chat] Iniciando varredura da fila de chats...")

            # Busca todos os tickets na fila de chat
            tickets_chat = r.lrange(FILA_CHAT, 0, -1)
            print(f"[worker_chat] {len(tickets_chat)} ticket(s) na fila de chat.")

            for item in tickets_chat:
                try:
                    dados = json.loads(item.decode("utf-8"))
                    ticket_id = dados["ticket_id"]
                    timestamp_entrada = dados["timestamp"]
                    horas_na_fila = (time.time() - timestamp_entrada) / 3600

                    # Verifica se obs2 já foi processada (evita duplicação)
                    if r.exists(f"obs2_concluida:{ticket_id}"):
                        print(f"[worker_chat] Ticket {ticket_id} já processado. Removendo da fila.")
                        r.lrem(FILA_CHAT, 0, item)
                        continue

                    # Busca thread do chat
                    thread_id = buscar_thread_chat(ticket_id)

                    encerrado = False
                    if thread_id:
                        encerrado = chat_esta_encerrado(thread_id)

                    if encerrado:
                        print(f"[worker_chat] Chat do ticket {ticket_id} encerrado. Processando Obs 2...")
                        threading.Thread(target=processar_obs2_chat, args=(ticket_id, item), daemon=True).start()

                    elif horas_na_fila >= CHAT_TIMEOUT_HORAS:
                        print(f"[worker_chat] Ticket {ticket_id} na fila há {horas_na_fila:.1f}h. Processando com o que tem...")
                        threading.Thread(target=processar_obs2_chat, args=(ticket_id, item), daemon=True).start()

                    else:
                        print(f"[worker_chat] Ticket {ticket_id} — chat ainda aberto ({horas_na_fila:.1f}h na fila). Aguardando...")

                except Exception as e:
                    print(f"[worker_chat] Erro ao processar item da fila: {e}")

        except Exception as e:
            print(f"[worker_chat] Erro na varredura: {e}")


def processar_obs2_chat(ticket_id, item_redis):
    """Processa a Obs 2 para tickets de chat e remove da fila."""
    try:
        sucesso = processar_obs2(ticket_id)
        r.set(f"obs2_concluida:{ticket_id}", "1", ex=86400)
        r.lrem(FILA_CHAT, 0, item_redis)
        print(f"[worker_chat] Obs2 chat {'✅' if sucesso else '❌'} para ticket {ticket_id}. Removido da fila.")
    except Exception as e:
        print(f"[worker_chat] Erro ao processar Obs2 do ticket {ticket_id}: {e}")


def verificar_e_disparar_obs3(ticket_id, requer_obs2=True):
    """
    Verifica se as condições para disparar a Obs 3 foram atendidas.
    Para chat: requer_obs2=False (dispara após só Obs 1)
    Para e-mail: requer_obs2=True (dispara após Obs 1 + Obs 2)
    """
    obs1_ok = r.exists(f"obs1_concluida:{ticket_id}")
    obs2_ok = r.exists(f"obs2_concluida:{ticket_id}")
    obs3_disparada = r.exists(f"obs3_disparada:{ticket_id}")

    if obs3_disparada:
        return

    pronto = obs1_ok if not requer_obs2 else (obs1_ok and obs2_ok)

    if pronto:
        r.set(f"obs3_disparada:{ticket_id}", "1", ex=86400)
        print(f"[coordenador] Condições atendidas. Disparando Obs3 para ticket {ticket_id}...")
        threading.Thread(target=processar_obs3, args=(ticket_id,), daemon=True).start()


# --- REPROCESSAMENTO MANUAL ---

def varredura_manual_chats():
    """
    Força a varredura imediata da fila de chats.
    Mesma lógica do worker_chat mas disparada manualmente via endpoint /varrer-chats.
    """
    global ultima_varredura_chat
    print("[admin] Varredura manual de chats iniciada...")
    tickets_chat = r.lrange(FILA_CHAT, 0, -1)
    total = len(tickets_chat)
    print(f"[admin] {total} ticket(s) na fila de chat.")

    if not tickets_chat:
        print("[admin] Nenhum ticket na fila de chat.")
        return

    for item in tickets_chat:
        try:
            dados = json.loads(item.decode("utf-8"))
            ticket_id = dados["ticket_id"]
            timestamp_entrada = dados["timestamp"]
            horas_na_fila = (time.time() - timestamp_entrada) / 3600

            if r.exists(f"obs2_concluida:{ticket_id}"):
                print(f"[admin] Ticket {ticket_id} já processado. Removendo da fila.")
                r.lrem(FILA_CHAT, 0, item)
                continue

            thread_id = buscar_thread_chat(ticket_id)
            encerrado = False
            if thread_id:
                encerrado = chat_esta_encerrado(thread_id)

            if encerrado:
                print(f"[admin] Chat do ticket {ticket_id} encerrado. Processando Obs 2...")
                threading.Thread(target=processar_obs2_chat, args=(ticket_id, item), daemon=True).start()
            elif horas_na_fila >= CHAT_TIMEOUT_HORAS:
                print(f"[admin] Ticket {ticket_id} na fila há {horas_na_fila:.1f}h. Processando por timeout...")
                threading.Thread(target=processar_obs2_chat, args=(ticket_id, item), daemon=True).start()
            else:
                print(f"[admin] Ticket {ticket_id} — chat ainda aberto ({horas_na_fila:.1f}h na fila). Aguardando...")

        except Exception as e:
            print(f"[admin] Erro ao processar item da fila: {e}")

    ultima_varredura_chat = time.time()
    print("[admin] ✅ Varredura manual concluída.")



def buscar_tickets_estagio_novo():
    """Busca todos os tickets no estágio Novo da pipeline de suporte via API HubSpot."""
    url = "https://api.hubapi.com/crm/v3/objects/tickets/search"
    todos = []
    after = None

    while True:
        payload = {
            "filterGroups": [{
                "filters": [
                    {"propertyName": "hs_pipeline", "operator": "EQ", "value": PIPELINE_SUPORTE_ID},
                    {"propertyName": "hs_pipeline_stage", "operator": "EQ", "value": STAGE_NOVO}
                ]
            }],
            "properties": ["subject", "hs_pipeline", "hs_pipeline_stage", "hs_object_source"],
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
            print(f"[reprocessar] Erro ao buscar tickets novos: {e}")
            break

    return todos


def reprocessar_tickets_novos():
    """
    Busca todos os tickets no estágio Novo da pipeline de suporte
    e coloca cada um nas filas de categorização e observações.
    Responde imediatamente ao endpoint e roda em background.
    """
    print("[reprocessar] Iniciando busca de tickets no estágio Novo...")
    tickets = buscar_tickets_estagio_novo()
    total = len(tickets)
    print(f"[reprocessar] {total} ticket(s) encontrado(s) no estágio Novo.")

    if not tickets:
        print("[reprocessar] Nenhum ticket para reprocessar.")
        return

    adicionados = 0
    for ticket in tickets:
        ticket_id = str(ticket.get("id", ""))
        if not ticket_id:
            continue

        props = ticket.get("properties", {})
        e_chat = "CHAT" in props.get("hs_object_source", "").upper() or "bot" in props.get("subject", "").lower()

        # Fila de categorização
        r.lpush(FILA_CATEG, ticket_id)

        # Filas de observações
        if e_chat:
            dados_obs1 = json.dumps({"ticket_id": ticket_id, "e_chat": True})
            r.lpush(FILA_OBS1, dados_obs1)
            dados_chat = json.dumps({"ticket_id": ticket_id, "timestamp": time.time()})
            r.lpush(FILA_CHAT, dados_chat)
        else:
            dados_obs1 = json.dumps({"ticket_id": ticket_id, "e_chat": False})
            r.lpush(FILA_OBS1, dados_obs1)
            r.lpush(FILA_OBS2, ticket_id)

        adicionados += 1
        print(f"[reprocessar] Ticket {ticket_id} adicionado às filas ({'chat' if e_chat else 'e-mail'}).")

    print(f"[reprocessar] ✅ {adicionados}/{total} tickets adicionados às filas.")


# --- WEBHOOK ---

class WebhookHandler(BaseHTTPRequestHandler):

    def do_HEAD(self):
        """Responde requisições HEAD do UptimeRobot."""
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()

    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "alive"}')
        elif self.path == "/limpar-filas":
            r.delete("fila_obs1")
            r.delete("fila_obs2")
            r.delete("fila_chat")
            r.delete("fila_categorizacao")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "filas limpas"}')
            print("[admin] Filas limpas via endpoint /limpar-filas.")
        elif self.path == "/status-filas":
            obs1 = r.llen("fila_obs1")
            obs2 = r.llen("fila_obs2")
            chat = r.llen("fila_chat")
            categ = r.llen("fila_categorizacao")
            tickets_chat = []
            for item in r.lrange("fila_chat", 0, -1):
                try:
                    dados = json.loads(item.decode("utf-8"))
                    ticket_id = dados.get("ticket_id", "")
                    timestamp = dados.get("timestamp", 0)
                    horas = round((time.time() - timestamp) / 3600, 1)
                    tickets_chat.append({"ticket_id": ticket_id, "horas_na_fila": horas})
                except Exception:
                    pass
            status = {
                "fila_categorizacao": categ,
                "fila_obs1": obs1,
                "fila_obs2": obs2,
                "fila_chat": chat,
                "tickets_chat": tickets_chat
            }
            resposta = json.dumps(status, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(resposta)
            print(f"[admin] Status: categ={categ}, obs1={obs1}, obs2={obs2}, chat={chat}")
        elif self.path == "/proxima-varredura":
            import datetime
            agora = time.time()
            intervalo = 1800  # 30 minutos em segundos
            fuso_brasilia = datetime.timezone(datetime.timedelta(hours=-3))

            if ultima_varredura_chat is None:
                # Worker ainda não rodou desde o início do servidor
                segundos_desde_inicio = agora - servidor_iniciado_em
                segundos_para_proxima = max(0, intervalo - segundos_desde_inicio)
            else:
                segundos_desde_ultima = agora - ultima_varredura_chat
                segundos_para_proxima = max(0, intervalo - segundos_desde_ultima)

            proxima_ts = agora + segundos_para_proxima
            proxima_str = datetime.datetime.fromtimestamp(proxima_ts, tz=fuso_brasilia).strftime("%H:%M:%S")
            ultima_str = datetime.datetime.fromtimestamp(ultima_varredura_chat, tz=fuso_brasilia).strftime("%H:%M:%S") if ultima_varredura_chat else "ainda não rodou"

            mins = int(segundos_para_proxima // 60)
            segs = int(segundos_para_proxima % 60)

            status = {
                "fuso": "America/Sao_Paulo (UTC-3)",
                "ultima_varredura": ultima_str,
                "proxima_varredura": proxima_str,
                "em_minutos": f"{mins}min {segs}s",
                "tickets_na_fila_chat": r.llen("fila_chat")
            }
            resposta = json.dumps(status, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(resposta)
        elif self.path.startswith("/debug-ticket/"):
            ticket_id = self.path.split("/debug-ticket/")[-1].strip()
            from hubspot_client import buscar_thread_conversa, buscar_mensagens_chat
            thread_id = buscar_thread_conversa(ticket_id)
            if not thread_id:
                resposta = json.dumps({"erro": f"Nenhuma thread encontrada para ticket {ticket_id}"}).encode("utf-8")
            else:
                mensagens = buscar_mensagens_chat(thread_id)
                resultado = {"thread_id": thread_id, "total_mensagens": len(mensagens), "mensagens": []}
                for i, msg in enumerate(mensagens):
                    resultado["mensagens"].append({
                        "indice": i,
                        "type": msg.get("type", ""),
                        "text": (msg.get("text", "") or "")[:300],
                        "body": (msg.get("body", "") or "")[:300],
                        "richText": (msg.get("richText", "") or "")[:300],
                        "truncatedPreviewText": (msg.get("truncatedPreviewText", "") or "")[:300],
                        "sender_name": msg.get("senders", [{}])[0].get("name", ""),
                        "createdBy": msg.get("createdBy", ""),
                        "keys": list(msg.keys())
                    })
                resposta = json.dumps(resultado, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(resposta)
        elif self.path.startswith("/debug-thread/"):
            thread_id = self.path.split("/debug-thread/")[-1].strip()
            from hubspot_client import buscar_mensagens_chat
            mensagens = buscar_mensagens_chat(thread_id)
            resultado = []
            for i, msg in enumerate(mensagens):
                resultado.append({
                    "indice": i,
                    "type": msg.get("type", ""),
                    "text": msg.get("text", "")[:200],
                    "body": msg.get("body", "")[:200],
                    "richText": msg.get("richText", "")[:200],
                    "truncatedPreviewText": msg.get("truncatedPreviewText", "")[:200],
                    "sender_name": msg.get("senders", [{}])[0].get("name", ""),
                    "createdBy": msg.get("createdBy", ""),
                    "keys": list(msg.keys())
                })
            resposta = json.dumps(resultado, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(resposta)
        elif self.path == "/reprocessar-novos":
            threading.Thread(target=reprocessar_tickets_novos, daemon=True).start()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "reprocessamento iniciado em background"}')
            print("[admin] Reprocessamento de tickets novos iniciado via endpoint.")
        elif self.path == "/varrer-chats":
            threading.Thread(target=varredura_manual_chats, daemon=True).start()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "varredura de chats iniciada em background"}')
            print("[admin] Varredura manual de chats iniciada via endpoint.")
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path != "/webhook":
            self.send_response(404)
            self.end_headers()
            return

        tamanho = int(self.headers.get("Content-Length", 0))
        corpo = json.loads(self.rfile.read(tamanho))

        # Responde 200 imediatamente
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status": "ok"}')

        if not isinstance(corpo, list):
            corpo = [corpo]

        for evento in corpo:
            tipo = evento.get("subscriptionType", "")
            ticket_id = str(evento.get("objectId", ""))

            if tipo == "ticket.creation" and ticket_id:
                elegivel, props = verificar_ticket_elegivel(ticket_id)

                if not elegivel:
                    print(f"[webhook] Ticket {ticket_id} ignorado — pipeline ou estágio incorreto.")
                    continue

                e_chat = ticket_e_chat(props)

                # Categorização entra SEMPRE na fila (1º a rodar)
                r.lpush(FILA_CATEG, ticket_id)
                print(f"[webhook] Ticket {ticket_id} adicionado à fila de categorização.")

                if e_chat:
                    print(f"[webhook] Ticket {ticket_id} é de CHAT. Adicionando às filas de obs e chat...")
                    dados_obs1 = json.dumps({"ticket_id": ticket_id, "e_chat": True})
                    r.lpush(FILA_OBS1, dados_obs1)
                    dados_chat = json.dumps({
                        "ticket_id": ticket_id,
                        "timestamp": time.time()
                    })
                    r.lpush(FILA_CHAT, dados_chat)
                    print(f"[webhook] Ticket {ticket_id} — Categorização + Obs1 + fila de chat.")

                else:
                    print(f"[webhook] Ticket {ticket_id} é de E-MAIL/FORMULÁRIO. Adicionando às filas normais...")
                    dados_obs1 = json.dumps({"ticket_id": ticket_id, "e_chat": False})
                    r.lpush(FILA_OBS1, dados_obs1)
                    r.lpush(FILA_OBS2, ticket_id)

                    print(f"[webhook] Ticket {ticket_id} — Obs1 e Obs2 nas filas normais.")

    def log_message(self, format, *args):
        print(f"[webhook] {args[0]} {args[1]}")


# --- INÍCIO ---

if __name__ == "__main__":
    servidor_iniciado_em = time.time()
    threading.Thread(target=worker_categorizacao, daemon=True).start()
    threading.Thread(target=worker_obs1, daemon=True).start()
    threading.Thread(target=worker_obs2, daemon=True).start()
    threading.Thread(target=worker_chat, daemon=True).start()

    porta = int(os.environ.get("PORT", 8000))
    print(f"[servidor] Rodando na porta {porta}...")
    print(f"[servidor] Monitorando pipeline {PIPELINE_SUPORTE_ID}, estágio Novo (id={STAGE_NOVO}).")
    print(f"[servidor] Health check disponível em /health")
    print(f"[servidor] Rotina de chats: varredura a cada 30min, timeout de {CHAT_TIMEOUT_HORAS}h.")
    HTTPServer(("0.0.0.0", porta), WebhookHandler).serve_forever()
