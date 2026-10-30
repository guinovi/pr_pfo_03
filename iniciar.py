#!/usr/bin/env python3
"""
Lanzador unificado para el PFO 3 (Programación sobre Redes)
Permite iniciar el Servidor Distribuido en segundo plano y ejecutar
el Cliente en modo interactivo o de demostración con un solo comando.
"""

import subprocess
import sys
import time
import socket
import argparse

PUERTO = 12345
HOST = "127.0.0.1"


def esperar_puerto(host, puerto, timeout=5.0):
    inicio = time.time()
    while time.time() - inicio < timeout:
        try:
            with socket.create_connection((host, puerto), timeout=0.5):
                return True
        except (socket.error, ConnectionRefusedError):
            time.sleep(0.2)
    return False


def main():
    parser = argparse.ArgumentParser(description="Lanzador Unificado PFO 3")
    parser.add_argument("--demo", action="store_true", help="Ejecutar demostración automática")
    args = parser.parse_args()

    print("=" * 60)
    print(" INICIANDO SISTEMA DISTRIBUIDO (PFO 3 - CLIENTE / SERVIDOR)")
    print("=" * 60)

    # 1. Iniciar servidor como subproceso en modo silencioso para no mezclar logs con el menú del cliente
    print("▶ Levantando Servidor Distribuido en segundo plano...")
    proceso_servidor = subprocess.Popen([sys.executable, "servidor.py", "--silencioso"])

    try:
        # 2. Esperar disponibilidad del socket TCP
        if not esperar_puerto(HOST, PUERTO):
            print("❌ No se pudo conectar al servidor en el tiempo esperado.")
            return

        print(f"✔ Servidor activo en TCP {HOST}:{PUERTO} (3 Workers x 3 Hilos)")
        print("▶ Conectando Cliente...")
        time.sleep(0.2)

        # 3. Ejecutar cliente en el mismo proceso (interactivo o demo)
        cmd_cliente = [sys.executable, "cliente.py"]
        if args.demo:
            cmd_cliente.append("--demo")
        subprocess.run(cmd_cliente)

    except KeyboardInterrupt:
        print("\n[INTERRUPCIÓN] Finalizando...")
    finally:
        print("\n▶ Apagando Servidor Distribuido...")
        proceso_servidor.terminate()
        try:
            proceso_servidor.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proceso_servidor.kill()
        print("✔ Servidor apagado limpiamente.")


if __name__ == "__main__":
    main()
