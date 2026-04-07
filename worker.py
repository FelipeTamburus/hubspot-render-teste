import os
import time
import redis
from enriquecimento import processar_ticket

r = redis.from_url(os.environ.get("REDIS_URL"))
FILA_KEY = "fila_tickets"

print("[worker] Iniciado, aguardando tickets na fila...")

while True:
    try:
        # Aguarda até 5 segundos por um novo ticket na fila
        resultado = r.brpop(FILA_KEY, timeout=5)

        if resultado:
            _, ticket_id = resultado
            ticket_id = ticket_id.decode("utf-8")
            print(f"[worker] Processando ticket {ticket_id}...")
            processar_ticket(ticket_id)
        else:
            print("[worker] Fila vazia, aguardando...")

    except Exception as e:
        print(f"[worker] Erro: {e}")
        time.sleep(5)
