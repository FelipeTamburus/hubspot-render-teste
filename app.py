from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import os
import redis

r = redis.from_url(os.environ.get("REDIS_URL"))
FILA_KEY = "fila_tickets"

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

        # Joga os tickets na fila Redis
        if not isinstance(corpo, list):
            corpo = [corpo]

        for evento in corpo:
            tipo = evento.get("subscriptionType", "")
            ticket_id = str(evento.get("objectId", ""))

            if tipo == "ticket.creation" and ticket_id:
                r.lpush(FILA_KEY, ticket_id)
                print(f"[fila] Ticket {ticket_id} adicionado à fila. Total: {r.llen(FILA_KEY)}")

    def log_message(self, format, *args):
        print(f"[webhook] {args[0]} {args[1]}")

if __name__ == "__main__":
    porta = int(os.environ.get("PORT", 8000))
    print(f"[servidor] Rodando na porta {porta}...")
    HTTPServer(("0.0.0.0", porta), WebhookHandler).serve_forever()
