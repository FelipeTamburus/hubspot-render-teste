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

r = redis.from_url(os.environ.get("REDIS_URL"))
FILA_OBS1 = "fila_obs1"
FILA_OBS2 = "fila_obs2"

ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN_HUBSPOT")
HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json"
}


def verificar_ticket_elegivel(ticket_id):
    """
    Verifica se o ticket pertence à pipeline de suporte
    e está no estágio Novo (id=1).
    """
    url = f"https://api.hubapi.com/crm/v3/objects/tickets/{ticket_id}?properties=hs_pipeline,hs_pipeline_stage"
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        props = response.json().get("properties", {})
        pipeline = props.get("hs_pipeline", "")
        stage = props.get("hs_pipeline_stage", "")
        return pipeline == PIPELINE_SUPORTE_ID and stage == STAGE_NOVO
    except Exception as e:
        print(f"[webhook] Erro ao verificar ticket {ticket_id}: {e}")
        return False


def worker_obs1():
    print("[worker_obs1] Iniciado, aguardando tickets...")
    while True:
        try:
            resultado = r.brpop(FILA_OBS1, timeout=10)
            if resultado:
                _, ticket_id = resultado
                ticket_id = ticket_id.decode("utf-8")
                print(f"[worker_obs1] Processando ticket {ticket_id}...")
                sucesso = processar_obs1(ticket_id)
                r.set(f"obs1_concluida:{ticket_id}", "1", ex=86400)
                print(f"[worker_obs1] Obs1 {'✅' if sucesso else '❌'} para ticket {ticket_id}.")
                verificar_e_disparar_obs3(ticket_id)
        except Exception as e:
            print(f"[worker_obs1] Erro: {e}")
            time.sleep(5)


def worker_obs2():
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
                verificar_e_disparar_obs3(ticket_id)
        except Exception as e:
            print(f"[worker_obs2] Erro: {e}")
            time.sleep(5)


def verificar_e_disparar_obs3(ticket_id):
    obs1_ok = r.exists(f"obs1_concluida:{ticket_id}")
    obs2_ok = r.exists(f"obs2_concluida:{ticket_id}")
    obs3_disparada = r.exists(f"obs3_disparada:{ticket_id}")

    if obs1_ok and obs2_ok and not obs3_disparada:
        r.set(f"obs3_disparada:{ticket_id}", "1", ex=86400)
        print(f"[coordenador] Obs1 + Obs2 concluídas. Disparando Obs3 para ticket {ticket_id}...")
        threading.Thread(target=processar_obs3, args=(ticket_id,), daemon=True).start()


class WebhookHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        if self.path != "/webhook":
            self.send_response(404)
            self.end_headers()
            return

        tamanho = int(self.headers.get("Content-Length", 0))
        corpo = json.loads(self.rfile.read(tamanho))

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
                # Verifica pipeline E estágio antes de entrar na fila
                if verificar_ticket_elegivel(ticket_id):
                    print(f"[webhook] Ticket {ticket_id} elegível (pipeline 0, estágio Novo). Adicionando às filas...")
                    r.lpush(FILA_OBS1, ticket_id)
                    r.lpush(FILA_OBS2, ticket_id)
                else:
                    print(f"[webhook] Ticket {ticket_id} ignorado — não é da pipeline de suporte ou não está no estágio Novo.")

    def log_message(self, format, *args):
        print(f"[webhook] {args[0]} {args[1]}")


if __name__ == "__main__":
    threading.Thread(target=worker_obs1, daemon=True).start()
    threading.Thread(target=worker_obs2, daemon=True).start()

    porta = int(os.environ.get("PORT", 8000))
    print(f"[servidor] Rodando na porta {porta}...")
    print(f"[servidor] Monitorando tickets na pipeline {PIPELINE_SUPORTE_ID}, estágio Novo (id={STAGE_NOVO}).")
    HTTPServer(("0.0.0.0", porta), WebhookHandler).serve_forever()
