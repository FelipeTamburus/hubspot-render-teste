import os
import uuid
import requests

CONTEXTO_AI_API_URL = os.environ.get("CONTEXTO_AI_API_URL", "")
CONTEXTO_AI_USERNAME = os.environ.get("CONTEXTO_AI_USERNAME", "")
CONTEXTO_AI_PASSWORD = os.environ.get("CONTEXTO_AI_PASSWORD", "")

_token = None


def autenticar():
    """Autentica no Contexto.AI e armazena o token."""
    global _token
    if not all([CONTEXTO_AI_API_URL, CONTEXTO_AI_USERNAME, CONTEXTO_AI_PASSWORD]):
        print("[contexto_ai] ERRO: Credenciais não configuradas.")
        return False
    try:
        response = requests.post(
            f"{CONTEXTO_AI_API_URL}/token",
            data={"grant_type": "password", "username": CONTEXTO_AI_USERNAME, "password": CONTEXTO_AI_PASSWORD},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30
        )
        response.raise_for_status()
        _token = response.json().get("access_token")
        if _token:
            print("[contexto_ai] ✅ Autenticado com sucesso.")
            return True
        print("[contexto_ai] ❌ Token não encontrado na resposta.")
        return False
    except Exception as e:
        print(f"[contexto_ai] ❌ Erro ao autenticar: {e}")
        return False


def chamar_contexto_ai(user_message, task_name="geral", retentativa=False):
    """
    Chama o Contexto.AI com o prompt completo no user_message.
    Retorna a string de resposta ou None em caso de erro.
    Reautentica automaticamente se o token expirar.
    """
    global _token
    if not _token:
        if not autenticar():
            return None

    session_id = str(uuid.uuid4())
    try:
        response = requests.post(
            f"{CONTEXTO_AI_API_URL}/v1/chat",
            headers={"Authorization": f"Bearer {_token}"},
            files={
                "session_id": (None, session_id),
                "user_message": (None, user_message),
                "use_super_conhecimento": (None, "false")
            },
            timeout=60
        )

        if response.status_code == 401 and not retentativa:
            print(f"[contexto_ai] Token expirado ({task_name}). Reautenticando...")
            _token = None
            return chamar_contexto_ai(user_message, task_name, retentativa=True)

        response.raise_for_status()
        bot_response = response.json().get("bot_response", "")
        if bot_response:
            return bot_response.strip()
        print(f"[contexto_ai] ⚠️ Resposta vazia ({task_name}).")
        return None

    except requests.exceptions.Timeout:
        print(f"[contexto_ai] ❌ Timeout ({task_name}).")
        return None
    except Exception as e:
        print(f"[contexto_ai] ❌ Erro ({task_name}): {e}")
        return None


# Autentica ao importar o módulo
autenticar()
