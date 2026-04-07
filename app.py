from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import os
import threading
import time
import redis
from enriquecimento import processar_ticket

os.environ["PYTHONUNBUFFERED"] = "1"

r = redis.from_url(os.environ.get("REDIS_URL"))
FILA_KEY = "fila_tickets"

def worker():
    print("[worker] Iniciado, aguardando tickets na fila...")
    while True:
        try:
            resultado = r.brpop(FILA_KEY, timeout=20)
            if resultado:
                _, ticket_id = resultado
                ticket_id = ticket_id.decode("utf-8")
                print(f"[worker] Processando ticket {ticket_id}...")
                processar_ticket(ticket_id)
        except Exception as e:
            print(f"[worker] Erro: {e}")
            time.sleep(5)

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
                r.lpush(FILA_KEY, ticket_id)
                print(f"[fila] Ticket {ticket_id} adicionado. Total na fila: {r.llen(FILA_KEY)}")

    def log_message(self, format, *args):
        print(f"[webhook] {args[0]} {args[1]}")

if __name__ == "__main__":
    # Inicia o worker em background
    threading.Thread(target=worker, daemon=True).start()

    porta = int(os.environ.get("PORT", 8000))
    print(f"[servidor] Rodando na porta {porta}...")
    HTTPServer(("0.0.0.0", porta), WebhookHandler).serve_forever()
