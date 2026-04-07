from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import threading
import os
from enriquecimento import processar_ticket

class WebhookHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        if self.path != "/webhook":
            self.send_response(404)
            self.end_headers()
            return

        tamanho = int(self.headers.get("Content-Length", 0))
        corpo = json.loads(self.rfile.read(tamanho))

        # Responde 200 imediatamente — obrigatório, senão o HubSpot retenta
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status": "ok"}')

        # Processa em thread separada para não travar
        threading.Thread(target=processar_eventos, args=(corpo,), daemon=True).start()

    def log_message(self, format, *args):
        print(f"[webhook] {args[0]} {args[1]}")

def processar_eventos(eventos):
    if not isinstance(eventos, list):
        eventos = [eventos]

    for evento in eventos:
        tipo = evento.get("subscriptionType", "")
        ticket_id = str(evento.get("objectId", ""))

        if tipo == "ticket.creation" and ticket_id:
            print(f"[evento] Ticket novo detectado: {ticket_id}")
            processar_ticket(ticket_id)

if __name__ == "__main__":
    porta = int(os.environ.get("PORT", 8000))
    print(f"Servidor rodando na porta {porta}...")
    HTTPServer(("0.0.0.0", porta), WebhookHandler).serve_forever()