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

PIPELINE_SUPORTE_ID = "0"
STAGE_NOVO = "1"
FILA_OBS1 = "fila_obs1"
FILA_OBS2 = "fila_obs2"
FILA_CHAT = "fila_chat"
CHAT_TIMEOUT_HORAS = 24

r = redis.from_url(os.environ.get("REDIS_URL"))

ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN_HUBSPOT")
HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}


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
    print("[worker_chat] Iniciado, verificando chats a cada 1 hora...")
    while True:
        try:
            time.sleep(3600)  # aguarda 1 hora
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
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "filas limpas"}')
            print("[admin] Filas limpas via endpoint /limpar-filas.")
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

                if e_chat:
                    print(f"[webhook] Ticket {ticket_id} é de CHAT. Adicionando à fila de chat e fila Obs1...")

                    # Obs 1 entra na fila normal com flag de chat
                    dados_obs1 = json.dumps({"ticket_id": ticket_id, "e_chat": True})
                    r.lpush(FILA_OBS1, dados_obs1)

                    # Obs 2 vai para fila de chat (rotina horária)
                    dados_chat = json.dumps({
                        "ticket_id": ticket_id,
                        "timestamp": time.time()
                    })
                    r.lpush(FILA_CHAT, dados_chat)

                    print(f"[webhook] Ticket {ticket_id} — Obs1 na fila normal, Obs2 na fila de chat.")

                else:
                    print(f"[webhook] Ticket {ticket_id} é de E-MAIL/FORMULÁRIO. Adicionando às filas normais...")

                    # Obs 1 e Obs 2 entram nas filas normais
                    dados_obs1 = json.dumps({"ticket_id": ticket_id, "e_chat": False})
                    r.lpush(FILA_OBS1, dados_obs1)
                    r.lpush(FILA_OBS2, ticket_id)

                    print(f"[webhook] Ticket {ticket_id} — Obs1 e Obs2 nas filas normais.")

    def log_message(self, format, *args):
        print(f"[webhook] {args[0]} {args[1]}")


# --- INÍCIO ---

if __name__ == "__main__":
    threading.Thread(target=worker_obs1, daemon=True).start()
    threading.Thread(target=worker_obs2, daemon=True).start()
    threading.Thread(target=worker_chat, daemon=True).start()

    porta = int(os.environ.get("PORT", 8000))
    print(f"[servidor] Rodando na porta {porta}...")
    print(f"[servidor] Monitorando pipeline {PIPELINE_SUPORTE_ID}, estágio Novo (id={STAGE_NOVO}).")
    print(f"[servidor] Health check disponível em /health")
    print(f"[servidor] Rotina de chats: varredura a cada 1h, timeout de {CHAT_TIMEOUT_HORAS}h.")
    HTTPServer(("0.0.0.0", porta), WebhookHandler).serve_forever()
