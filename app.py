from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import threading
import queue
import os
from enriquecimento import processar_ticket

# Fila que recebe os ticket_ids para processar
fila_tickets = queue.Queue()

def worker():
    """
    Roda em background para sempre.
    Pega um ticket da fila, processa, depois pega o próximo.
    Nunca processa dois ao mesmo tempo.
    """
    print("[fila] Worker iniciado, aguardando tickets...")
    while True:
        ticket_id = fila_tickets.get()
        try:
            print(f"[fila] Processando ticket {ticket_id} ({fila_tickets.qsize()} na fila)")
            processar_ticket(ticket_id)
        except Exception as e:
            print(f"[fila] Erro ao processar ticket {ticket_id}: {e}")
        finally:
            fila_tickets.task_done()

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

        # Coloca os tickets na fila em vez de processar direto
        if not isinstance(corpo, list):
            corpo = [corpo]

        for evento in corpo:
            tipo = evento.get("subscriptionType", "")
            ticket_id = str(evento.get("objectId", ""))

            if tipo == "ticket.creation" and ticket_id:
                fila_tickets.put(ticket_id)
                print(f"[fila] Ticket {ticket_id} adicionado à fila. Total na fila: {fila_tickets.qsize()}")

    def log_message(self, format, *args):
        print(f"[webhook] {args[0]} {args[1]}")

if __name__ == "__main__":
    # Inicia o worker em background antes de subir o servidor
    threading.Thread(target=worker, daemon=True).start()

    porta = int(os.environ.get("PORT", 8000))
    print(f"[servidor] Rodando na porta {porta}...")
    HTTPServer(("0.0.0.0", porta), WebhookHandler).serve_forever()
