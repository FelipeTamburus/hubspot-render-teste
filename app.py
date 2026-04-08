from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import os
import threading
import time
import redis
from obs1_contexto_empresa import processar_obs1
from obs2_dor_ticket import processar_obs2
from obs3_similares import processar_obs3

PIPELINE_SUPORTE_ID = "0"

r = redis.from_url(os.environ.get("REDIS_URL"))
FILA_OBS1 = "fila_obs1"
FILA_OBS2 = "fila_obs2"

TIMEOUT_OBS3_SEGUNDOS = 9000  # 2.5 horas máximo aguardando obs1 e obs2


# --- WORKERS ---

def worker_obs1():
    """Worker da Observação 1 — contexto da empresa e churn."""
    print("[worker_obs1] Iniciado, aguardando tickets...")
    while True:
        try:
            resultado = r.brpop(FILA_OBS1, timeout=10)
            if resultado:
                _, ticket_id = resultado
                ticket_id = ticket_id.decode("utf-8")
                print(f"[worker_obs1] Processando ticket {ticket_id}...")
                sucesso = processar_obs1(ticket_id)
                # Sinaliza conclusão no Redis para a Obs 3
                chave = f"obs1_concluida:{ticket_id}"
                r.set(chave, "1", ex=86400)  # expira em 24h
                print(f"[worker_obs1] Obs1 {'✅' if sucesso else '❌'} para ticket {ticket_id}.")
                verificar_e_disparar_obs3(ticket_id)
        except Exception as e:
            print(f"[worker_obs1] Erro: {e}")
            time.sleep(5)


def worker_obs2():
    """Worker da Observação 2 — dor do ticket."""
    print("[worker_obs2] Iniciado, aguardando tickets...")
    while True:
        try:
            resultado = r.brpop(FILA_OBS2, timeout=10)
            if resultado:
                _, ticket_id = resultado
                ticket_id = ticket_id.decode("utf-8")
                print(f"[worker_obs2] Processando ticket {ticket_id}...")
                sucesso = processar_obs2(ticket_id)
                # Sinaliza conclusão no Redis para a Obs 3
                chave = f"obs2_concluida:{ticket_id}"
                r.set(chave, "1", ex=86400)  # expira em 24h
                print(f"[worker_obs2] Obs2 {'✅' if sucesso else '❌'} para ticket {ticket_id}.")
                verificar_e_disparar_obs3(ticket_id)
        except Exception as e:
            print(f"[worker_obs2] Erro: {e}")
            time.sleep(5)


def verificar_e_disparar_obs3(ticket_id):
    """
    Verifica se Obs1 e Obs2 já concluíram para um ticket.
    Se sim, dispara a Obs3 em thread separada.
    """
    obs1_ok = r.exists(f"obs1_concluida:{ticket_id}")
    obs2_ok = r.exists(f"obs2_concluida:{ticket_id}")
    obs3_disparada = r.exists(f"obs3_disparada:{ticket_id}")

    if obs1_ok and obs2_ok and not obs3_disparada:
        # Marca que obs3 já foi disparada para evitar duplicação
        r.set(f"obs3_disparada:{ticket_id}", "1", ex=86400)
        print(f"[coordenador] Obs1 + Obs2 concluídas para ticket {ticket_id}. Disparando Obs3...")
        threading.Thread(target=processar_obs3, args=(ticket_id,), daemon=True).start()


# --- WEBHOOK ---

class WebhookHandler(BaseHTTPRequestHandler):

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
                print(f"[webhook] Ticket {ticket_id} recebido. Adicionando às filas...")
                r.lpush(FILA_OBS1, ticket_id)
                r.lpush(FILA_OBS2, ticket_id)
                print(f"[webhook] Ticket {ticket_id} adicionado às filas Obs1 e Obs2.")

    def do_GET(self):
        """Health check para o UptimeRobot manter o servidor acordado."""
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "alive"}')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        print(f"[webhook] {args[0]} {args[1]}")


# --- INÍCIO ---

if __name__ == "__main__":
    # Inicia os dois workers em threads separadas
    threading.Thread(target=worker_obs1, daemon=True).start()
    threading.Thread(target=worker_obs2, daemon=True).start()

    porta = int(os.environ.get("PORT", 8000))
    print(f"[servidor] Rodando na porta {porta}...")
    print(f"[servidor] Workers Obs1 e Obs2 iniciados.")
    print(f"[servidor] Obs3 será disparada automaticamente após conclusão das anteriores.")
    HTTPServer(("0.0.0.0", porta), WebhookHandler).serve_forever()
