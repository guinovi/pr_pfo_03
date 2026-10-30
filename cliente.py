"""
PFO 3: Rediseño como Sistema Distribuido (Cliente-Servidor)
Materia: Programación sobre Redes - 3° E
Alumno: Novillo Guillermo Tadeo

cliente.py:
Cliente TCP que envía tareas al servidor distribuido y recibe resultados.
Implementa:
- Conexión con reintentos y tolerancia a fallos (patrón Clase 10).
- Modo interactivo por consola y modo demostración (--demo).
- Envío individual, por lotes (BATCH) y mediante múltiples conexiones TCP concurrentes
  para evidenciar la atención multicliente del servidor y el pool de hilos de los workers.
"""

import socket
import json
import time
import argparse
from concurrent.futures import ThreadPoolExecutor

HOST_DEFAULT = "127.0.0.1"
PUERTO_DEFAULT = 12345
BUFFER_SIZE = 4096


class ClienteDistribuido:
    def __init__(self, host=HOST_DEFAULT, puerto=PUERTO_DEFAULT):
        self.host = host
        self.puerto = puerto

    def conectar_con_reintentos(self, intentos=4, espera=1.5):
        """
        Intenta conectar al servidor TCP con reintentos (Patrón de la Clase 10).
        """
        for intento in range(intentos):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.settimeout(10.0)
                s.connect((self.host, self.puerto))
                return s
            except (socket.error, ConnectionRefusedError) as e:
                s.close()
                print(f" [!] Intento {intento + 1}/{intentos}: No se pudo conectar a {self.host}:{self.puerto} ({e})")
                if intento < intentos - 1:
                    time.sleep(espera)
        raise ConnectionError(f"No fue posible conectar al servidor {self.host}:{self.puerto} tras {intentos} intentos.")

    def enviar_solicitud(self, solicitud):
        """
        Envía un payload JSON terminado en \\n y espera la respuesta JSON.
        """
        sock = self.conectar_con_reintentos()
        try:
            with sock:
                mensaje = (json.dumps(solicitud) + "\n").encode("utf-8")
                sock.sendall(mensaje)

                # Recepción con buffer en bloques de 4096 bytes
                respuesta_bruta = ""
                while True:
                    bloque = sock.recv(BUFFER_SIZE)
                    if not bloque:
                        break
                    respuesta_bruta += bloque.decode("utf-8", errors="replace")
                    if "\n" in respuesta_bruta:
                        break

                linea = respuesta_bruta.strip().split("\n")[0]
                return json.loads(linea)
        except Exception as e:
            return {"estado": "ERROR", "mensaje": str(e)}

    def enviar_tarea(self, tipo, parametros=None, tarea_id=None):
        solicitud = {
            "accion": "ENVIAR_TAREA",
            "tarea": {
                "id": tarea_id,
                "tipo": tipo,
                "parametros": parametros or {}
            }
        }
        return self.enviar_solicitud(solicitud)

    def enviar_batch(self, lista_tareas):
        solicitud = {
            "accion": "BATCH_TAREAS",
            "tareas": lista_tareas
        }
        return self.enviar_solicitud(solicitud)

    def enviar_rafaga_sockets_concurrentes(self, lista_tareas):
        """
        Simula múltiples clientes conectándose en paralelo por sockets TCP independientes.
        """
        if not lista_tareas:
            return []
        max_hilos = max(1, min(len(lista_tareas), 12))
        with ThreadPoolExecutor(max_workers=max_hilos) as pool:
            futuros = [
                pool.submit(self.enviar_tarea, t.get("tipo", "calculo"), t.get("parametros", {}), t.get("id"))
                for t in lista_tareas
            ]
            respuestas = [f.result() for f in futuros]
        return [r.get("datos", {}) for r in respuestas if r.get("estado") == "OK"]

    def obtener_estado_cluster(self):
        return self.enviar_solicitud({"accion": "ESTADO_CLUSTER"})

    def obtener_historial(self, limite=10):
        return self.enviar_solicitud({"accion": "HISTORIAL", "limite": limite})


def imprimir_resultado_tarea(res):
    if res.get("estado") != "OK":
        print(f" ❌ Error en la tarea: {res.get('mensaje')}")
        return
    d = res.get("datos", {})
    print("\n" + "─" * 60)
    print(f"  ✔ Tarea ID        : {d.get('tarea_id')}")
    print(f"  🏢 Worker Asignado: {d.get('worker_asignado')}")
    print(f"  🧵 Hilo Ejecutor  : {d.get('hilo_ejecutor')}")
    print(f"  ⏱  Duración       : {d.get('duracion_ms')} ms")
    if d.get("s3_key"):
        print(f"  ☁ S3 URI          : {d.get('s3_key')}")
    print(f"  📦 Resultado      : {json.dumps(d.get('resultado', {}), indent=2, ensure_ascii=False)}")
    print("─" * 60)


def ejecutar_demostracion(cliente):
    print("\n" + "=" * 65)
    print(" INICIANDO MODO DEMOSTRACIÓN (PRUEBA INTEGRAL DEL CLÚSTER)")
    print("=" * 65)

    # 1. Tarea individual: Hash Criptográfico
    print("\n▶ 1. Enviando Tarea Individual (Hash SHA-256)...")
    res1 = cliente.enviar_tarea("hash_criptografico", {"texto": "Arquitectura Distribuida PFO 3 Redes"})
    imprimir_resultado_tarea(res1)

    # 2. Tarea individual: Guardar objeto en S3 simulado
    print("\n▶ 2. Enviando Tarea de Almacenamiento Distribuido (Objeto S3 por bloques)...")
    res2 = cliente.enviar_tarea("guardar_archivo_s3", {
        "archivo": "informe_redes_tp3.txt",
        "contenido": "Informe de distribución de carga en clúster de workers."
    })
    imprimir_resultado_tarea(res2)

    # 3. Ráfaga de múltiples clientes TCP concurrentes
    print("\n▶ 3. Enviando RÁFAGA CONCURRENTE de 6 clientes por Sockets TCP simultáneos...")
    tareas_concurrentes = [
        {"tipo": "analisis_texto", "parametros": {"texto": f"Muestra de texto concurrente número {i} para procesar."}}
        if i % 2 == 0 else
        {"tipo": "calculo_intensivo", "parametros": {"n": 50000 + i * 10000, "demora": 0.12}}
        for i in range(1, 7)
    ]
    t_inicio = time.time()
    resultados_rafaga = cliente.enviar_rafaga_sockets_concurrentes(tareas_concurrentes)
    t_total = round((time.time() - t_inicio) * 1000, 2)

    print(f" ✔ Ráfaga de 6 conexiones simultáneas procesada en {t_total} ms:")
    print(" ┌───────────────┬──────────────┬────────────────────────┬─────────────┐")
    print(" │ ID Tarea      │ Worker       │ Hilo del Pool          │ Duración    │")
    print(" ├───────────────┼──────────────┼────────────────────────┼─────────────┤")
    for r in resultados_rafaga:
        tid = r.get("tarea_id", "")[-13:].ljust(13)
        w = r.get("worker_asignado", "").ljust(12)
        h = r.get("hilo_ejecutor", "").ljust(22)
        d = f"{r.get('duracion_ms')} ms".rjust(11)
        print(f" │ {tid} │ {w} │ {h} │ {d} │")
    print(" └───────────────┴──────────────┴────────────────────────┴─────────────┘")

    # 4. Estado y métricas del clúster
    print("\n▶ 4. Consultando Estado del Clúster (RabbitMQ y Workers)...")
    res_metricas = cliente.obtener_estado_cluster()
    m = res_metricas.get("metricas", {})
    print(f"  • Mensajes encolados RabbitMQ: {m.get('total_encoladas')}")
    print(f"  • Mensajes procesados       : {m.get('total_procesadas')}")
    print("  • Distribución por Servidor Worker:")
    for w in m.get("workers", []):
        print(f"     - {w['id']}: {w['tareas_completadas']} tareas procesadas (Pool de {w['hilos_pool']} hilos)")

    print("\n[OK] Demostración completada exitosamente.")


def leer_entero(prompt, valor_defecto, minimo=1, maximo=10000000):
    entrada = input(prompt).strip()
    if not entrada:
        return valor_defecto
    try:
        val = int(entrada)
        return max(minimo, min(val, maximo))
    except ValueError:
        print(f" [!] Valor no numérico. Usando valor por defecto ({valor_defecto}).")
        return valor_defecto


def menu_interactivo(cliente):
    try:
        while True:
            print("\n" + "=" * 55)
            print("  CLIENTE DE TAREAS DISTRIBUIDAS (PFO 3)")
            print("  Conectado a:", f"TCP {cliente.host}:{cliente.puerto}")
            print("=" * 55)
            print(" 1. Enviar tarea individual (Hash / Análisis / S3 / Cálculo)")
            print(" 2. Enviar ráfaga concurrente de tareas (Test de Carga)")
            print(" 3. Ver estado del clúster (RabbitMQ & Workers)")
            print(" 4. Ver historial de tareas (PostgreSQL / SQLite)")
            print(" 5. Ejecutar demostración guiada completa")
            print(" 0. Salir")
            print("=" * 55)
            opcion = input(" Seleccione una opción: ").strip()

            if opcion == "1":
                print("\n Tipos de tarea disponibles:")
                print("  a. Hash SHA-256 criptográfico")
                print("  b. Análisis de texto (palabras y caracteres)")
                print("  c. Guardar archivo en S3 (bloques 4KB)")
                print("  d. Cálculo intensivo multihilo")
                sub = input(" Elija tipo [a/b/c/d]: ").strip().lower()

                if sub == "a":
                    txt = input(" Texto a hashear: ") or "Mensaje de prueba"
                    res = cliente.enviar_tarea("hash_criptografico", {"texto": txt})
                elif sub == "b":
                    txt = input(" Texto a analizar: ") or "Sistemas Distribuidos sobre Redes 2026"
                    res = cliente.enviar_tarea("analisis_texto", {"texto": txt})
                elif sub == "c":
                    nom = input(" Nombre archivo en S3 [reporte.txt]: ") or "reporte.txt"
                    cont = input(" Contenido: ") or "Datos persistidos en el bucket S3"
                    res = cliente.enviar_tarea("guardar_archivo_s3", {"archivo": nom, "contenido": cont})
                else:
                    n_val = leer_entero(" Cantidad de iteraciones [100000]: ", 100000, minimo=100, maximo=5000000)
                    res = cliente.enviar_tarea("calculo_intensivo", {"n": n_val, "demora": 0.2})

                imprimir_resultado_tarea(res)

            elif opcion == "2":
                cant = leer_entero(" ¿Cuántas tareas concurrentes enviar? [6]: ", 6, minimo=1, maximo=50)
                tareas = [
                    {"tipo": "analisis_texto" if i % 2 == 0 else "calculo_intensivo",
                     "parametros": {"texto": f"Carga {i}", "n": 30000, "demora": 0.1}}
                    for i in range(1, cant + 1)
                ]
                print(f"\n Despachando {cant} tareas concurrentes por sockets TCP...")
                t0 = time.time()
                resultados = cliente.enviar_rafaga_sockets_concurrentes(tareas)
                print(f" ✔ Completadas en {round((time.time() - t0)*1000, 2)} ms:")
                for r in resultados:
                    print(f"   • {r.get('tarea_id')}: {r.get('worker_asignado')} -> {r.get('hilo_ejecutor')} ({r.get('duracion_ms')} ms)")

            elif opcion == "3":
                res_m = cliente.obtener_estado_cluster()
                m = res_m.get("metricas", {})
                print("\n" + "─" * 45)
                print("  ESTADO ACTUAL DEL CLÚSTER")
                print("─" * 45)
                print(f"  • Cola RabbitMQ pendientes: {m.get('cola_pendientes')}")
                print(f"  • Total tareas encoladas  : {m.get('total_encoladas')}")
                print(f"  • Total tareas procesadas : {m.get('total_procesadas')}")
                print("  • Servidores Workers:")
                for w in m.get("workers", []):
                    print(f"     - {w['id']}: {w['tareas_completadas']} tareas (Pool: {w['hilos_pool']} hilos)")
                print("─" * 45)

            elif opcion == "4":
                res_h = cliente.obtener_historial(limite=8)
                print("\n" + "─" * 65)
                print("  HISTORIAL EN POSTGRESQL (ÚLTIMAS TAREAS)")
                print("─" * 65)
                for f in res_h.get("historial", []):
                    print(f"  [{f['fecha']}] {f['id'][:12]} | {f['tipo']} | {f['worker']} ({f['hilo']}) | {f['duracion_ms']}ms")
                print("─" * 65)

            elif opcion == "5":
                ejecutar_demostracion(cliente)

            elif opcion == "0":
                print("\n ¡Hasta luego!")
                break
            else:
                print(" Opción no válida. Intente nuevamente.")
    except (KeyboardInterrupt, EOFError):
        print("\n\n Saliendo del cliente...")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cliente para Sistema Distribuido de Tareas")
    parser.add_argument("--host", default=HOST_DEFAULT, help="IP del servidor TCP")
    parser.add_argument("--puerto", type=int, default=PUERTO_DEFAULT, help="Puerto del servidor TCP")
    parser.add_argument("--demo", action="store_true", help="Ejecutar demostración automática")
    args = parser.parse_args()

    cli = ClienteDistribuido(host=args.host, puerto=args.puerto)
    if args.demo:
        ejecutar_demostracion(cli)
    else:
        menu_interactivo(cli)
