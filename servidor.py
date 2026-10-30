"""
PFO 3: Rediseño como Sistema Distribuido (Cliente-Servidor)
Materia: Programación sobre Redes - 3° E
Alumno: Novillo Guillermo Tadeo

servidor.py:
Implementa la arquitectura distribuida con:
- Servidor Socket TCP multihilo (patrón Clase 10).
- Cola de Mensajes simulada (RabbitMQ) desacoplada y thread-safe con consumidor asíncrono.
- Clúster de Servidores Workers, cada uno con su propio Pool de Hilos (ThreadPoolExecutor).
- Almacenamiento Distribuido:
    * PostgreSQL (simulado con SQLite relacional ACID: 'sistema_distribuido.db')
    * Amazon S3 (simulado en 's3_bucket/' con lectura/escritura en bloques de buffer de 4096B según Clase 10).
"""

import socket
import threading
import queue
import json
import time
import os
import sqlite3
import hashlib
import argparse
from concurrent.futures import ThreadPoolExecutor, Future

# Configuración por defecto
HOST_DEFAULT = "127.0.0.1"
PUERTO_DEFAULT = 12345
BUFFER_SIZE = 4096  # Buffer de E/S según Clase 10
DB_FILE = "sistema_distribuido.db"
S3_BUCKET_DIR = "s3_bucket"


class AlmacenamientoDistribuido:
    """
    Simula la capa de Almacenamiento Distribuido de la arquitectura:
    1. PostgreSQL: Base de datos relacional transaccional (SQLite thread-safe).
    2. Amazon S3 / MinIO: Almacén de objetos y archivos procesados usando buffers de 4KB.
    """
    def __init__(self, db_path=DB_FILE, s3_dir=S3_BUCKET_DIR):
        self.db_path = db_path
        self.s3_dir = s3_dir
        self.lock = threading.Lock()
        os.makedirs(self.s3_dir, exist_ok=True)
        self._inicializar_bd()

    def _inicializar_bd(self):
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tareas (
                    id TEXT PRIMARY KEY,
                    tipo TEXT NOT NULL,
                    datos TEXT NOT NULL,
                    estado TEXT NOT NULL,
                    worker_id TEXT,
                    hilo_id TEXT,
                    duracion_ms REAL,
                    s3_key TEXT,
                    creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completado_en TIMESTAMP
                )
            """)
            conn.commit()
            conn.close()

    def registrar_tarea_iniciada(self, tarea_id, tipo, datos):
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO tareas (id, tipo, datos, estado)
                VALUES (?, ?, ?, 'EN_COLA')
            """, (tarea_id, tipo, json.dumps(datos)))
            conn.commit()
            conn.close()

    def registrar_tarea_completada(self, tarea_id, worker_id, hilo_id, duracion_ms, s3_key=None):
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE tareas
                SET estado = 'COMPLETADA',
                    worker_id = ?,
                    hilo_id = ?,
                    duracion_ms = ?,
                    s3_key = ?,
                    completado_en = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (worker_id, hilo_id, duracion_ms, s3_key, tarea_id))
            conn.commit()
            conn.close()

    def guardar_objeto_s3(self, nombre_objeto, contenido_bytes):
        """
        Guarda un objeto en S3 simulado escribiendo en bloques de 4096 bytes (Clase 10).
        Sanitiza el nombre del objeto para prevenir Path Traversal.
        """
        nombre_seguro = os.path.basename(str(nombre_objeto)).strip() or "objeto_s3.bin"
        os.makedirs(self.s3_dir, exist_ok=True)
        ruta_s3 = os.path.join(self.s3_dir, nombre_seguro)
        with open(ruta_s3, "wb", buffering=BUFFER_SIZE) as f:
            offset = 0
            while offset < len(contenido_bytes):
                chunk = contenido_bytes[offset:offset + BUFFER_SIZE]
                f.write(chunk)
                offset += len(chunk)
        return f"s3://distribuido-tasks/{nombre_seguro}"

    def obtener_historial(self, limite=10):
        limite_seguro = max(1, min(int(limite), 100))
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, tipo, estado, worker_id, hilo_id, duracion_ms, s3_key, completado_en
                FROM tareas
                ORDER BY completado_en DESC, creado_en DESC LIMIT ?
            """, (limite_seguro,))
            filas = cursor.fetchall()
            conn.close()
            return [
                {
                    "id": f[0], "tipo": f[1], "estado": f[2],
                    "worker": f[3], "hilo": f[4], "duracion_ms": f[5],
                    "s3_key": f[6], "fecha": f[7]
                }
                for f in filas
            ]


class ColaMensajesRabbitMQSimulada:
    """
    Simula el Message Broker (RabbitMQ):
    - Desacopla la recepción del socket respecto a la ejecución de workers.
    - Soporta múltiples publicadores y consumidores concurrentes de forma thread-safe.
    """
    def __init__(self, nombre_cola="tareas_distribuidas"):
        self.nombre_cola = nombre_cola
        self._cola = queue.Queue()
        self.total_encoladas = 0
        self.total_procesadas = 0
        self.lock = threading.Lock()

    def publicar(self, item):
        with self.lock:
            self.total_encoladas += 1
        self._cola.put(item)

    def consumir(self, timeout=0.5):
        try:
            return self._cola.get(timeout=timeout)
        except queue.Empty:
            return None

    def confirmar_ack(self):
        with self.lock:
            self.total_procesadas += 1
        self._cola.task_done()

    @property
    def pendientes(self):
        return self._cola.qsize()


class ServidorWorker:
    """
    Representa un Servidor Worker independiente.
    Cada nodo cuenta con su propio Pool de Hilos (ThreadPoolExecutor)
    para ejecutar tareas concurrentes sin bloquear la recepción.
    """
    def __init__(self, worker_id, max_hilos=3, almacenamiento=None):
        self.worker_id = worker_id
        self.max_hilos = max_hilos
        self.pool = ThreadPoolExecutor(max_workers=max_hilos, thread_name_prefix=f"{worker_id}_Hilo")
        self.almacenamiento = almacenamiento
        self.tareas_completadas = 0
        self.lock = threading.Lock()

    def ejecutar_tarea(self, tarea):
        """
        Procesa una tarea dentro del pool de hilos del worker.
        Devuelve el diccionario de resultado con telemetría de ejecución.
        """
        inicio = time.time()
        nombre_hilo = threading.current_thread().name
        tarea_id = tarea.get("id", f"t-{int(time.time()*1000)}")
        tipo = tarea.get("tipo", "calculo")
        parametros = tarea.get("parametros") or {}

        # Simular procesamiento según tipo de tarea
        resultado_payload = {}
        s3_key = None

        if tipo == "hash_criptografico":
            texto = str(parametros.get("texto", "datos_por_defecto"))
            resultado_hash = hashlib.sha256(texto.encode("utf-8")).hexdigest()
            resultado_payload = {"hash_sha256": resultado_hash, "longitud": len(texto)}

        elif tipo == "analisis_texto":
            texto = str(parametros.get("texto", ""))
            palabras = len(texto.split())
            caracteres = len(texto)
            resultado_payload = {"total_palabras": palabras, "total_caracteres": caracteres}

        elif tipo == "guardar_archivo_s3":
            nombre_arch = parametros.get("archivo", f"salida_{tarea_id}.txt")
            contenido = str(parametros.get("contenido", f"Resultado de tarea {tarea_id}")).encode("utf-8")
            if self.almacenamiento:
                s3_key = self.almacenamiento.guardar_objeto_s3(nombre_arch, contenido)
            resultado_payload = {"archivo": os.path.basename(str(nombre_arch)), "s3_uri": s3_key, "bytes": len(contenido)}

        else:  # "calculo_intensivo" o genérico
            try:
                n = int(parametros.get("n", 100000))
            except (ValueError, TypeError):
                n = 100000
            suma = sum(i * i for i in range(max(1, min(n, 50000))))
            demora = float(parametros.get("demora", 0.15))
            time.sleep(max(0.0, min(demora, 2.0)))
            resultado_payload = {"suma_cuadrados": suma, "iteraciones": n}

        duracion_ms = round((time.time() - inicio) * 1000, 2)

        # Persistir en almacenamiento distribuido (PostgreSQL simulado)
        if self.almacenamiento:
            self.almacenamiento.registrar_tarea_completada(
                tarea_id=tarea_id,
                worker_id=self.worker_id,
                hilo_id=nombre_hilo,
                duracion_ms=duracion_ms,
                s3_key=s3_key
            )

        with self.lock:
            self.tareas_completadas += 1

        return {
            "tarea_id": tarea_id,
            "estado": "EXITO",
            "worker_asignado": self.worker_id,
            "hilo_ejecutor": nombre_hilo,
            "duracion_ms": duracion_ms,
            "resultado": resultado_payload,
            "s3_key": s3_key
        }

    def detener(self):
        self.pool.shutdown(wait=False)


class GestorClusterWorkers:
    """
    Gestiona el clúster de servidores workers y distribuye las tareas
    extraídas de la cola RabbitMQ hacia los workers usando un hilo consumidor
    asíncrono con balanceo Round-Robin.
    """
    def __init__(self, num_workers=3, hilos_por_worker=3, almacenamiento=None):
        self.almacenamiento = almacenamiento or AlmacenamientoDistribuido()
        self.cola_mensajes = ColaMensajesRabbitMQSimulada()
        self.workers = [
            ServidorWorker(f"Worker-{i+1}", max_hilos=hilos_por_worker, almacenamiento=self.almacenamiento)
            for i in range(num_workers)
        ]
        self._indice_worker = 0
        self._lock = threading.Lock()
        self._activo = True
        self._hilo_broker = threading.Thread(
            target=self._bucle_consumidor_broker,
            name="RabbitMQ_Dispatcher",
            daemon=True
        )
        self._hilo_broker.start()

    def _bucle_consumidor_broker(self):
        """
        Hilo consumidor continuo que extrae mensajes de la cola RabbitMQ
        y los reparte a los pools de hilos de cada ServidorWorker.
        """
        while self._activo:
            mensaje = self.cola_mensajes.consumir(timeout=0.2)
            if mensaje is None:
                continue

            tarea, futuro_respuesta = mensaje
            with self._lock:
                worker = self.workers[self._indice_worker]
                self._indice_worker = (self._indice_worker + 1) % len(self.workers)

            def _procesar_en_worker(w=worker, t=tarea, fut=futuro_respuesta):
                try:
                    res = w.ejecutar_tarea(t)
                    fut.set_result(res)
                except Exception as exc:
                    fut.set_exception(exc)
                finally:
                    self.cola_mensajes.confirmar_ack()

            worker.pool.submit(_procesar_en_worker)

    def despachar(self, tarea):
        """
        Registra la tarea en la BD (PostgreSQL), la publica en RabbitMQ
        y espera de forma asíncrona el resultado producido por el Worker.
        """
        tarea_copia = dict(tarea) if isinstance(tarea, dict) else {}
        tarea_id = tarea_copia.get("id") or f"tsk-{int(time.time()*1000)}-{os.urandom(2).hex()}"
        tarea_copia["id"] = tarea_id

        # 1. Registrar estado inicial en PostgreSQL
        self.almacenamiento.registrar_tarea_iniciada(
            tarea_id,
            tarea_copia.get("tipo", "generica"),
            tarea_copia.get("parametros", {})
        )

        # 2. Publicar en la cola RabbitMQ junto con su Future de retorno
        futuro_respuesta = Future()
        self.cola_mensajes.publicar((tarea_copia, futuro_respuesta))

        # 3. Esperar a que un hilo del pool de algún Worker complete la tarea
        return futuro_respuesta.result(timeout=15.0)

    def obtener_metricas(self):
        return {
            "cola_pendientes": self.cola_mensajes.pendientes,
            "total_encoladas": self.cola_mensajes.total_encoladas,
            "total_procesadas": self.cola_mensajes.total_procesadas,
            "workers": [
                {
                    "id": w.worker_id,
                    "hilos_pool": w.max_hilos,
                    "tareas_completadas": w.tareas_completadas
                }
                for w in self.workers
            ]
        }

    def detener(self):
        self._activo = False
        for w in self.workers:
            w.detener()


class ServidorDistribuidoSockets:
    """
    Servidor TCP central multicliente.
    Recibe tareas por socket y las entrega al clúster de workers.
    """
    def __init__(self, host=HOST_DEFAULT, puerto=PUERTO_DEFAULT, num_workers=3, hilos_por_worker=3, silencioso=False):
        self.host = host
        self.puerto = puerto
        self.silencioso = silencioso
        self.cluster = GestorClusterWorkers(num_workers=num_workers, hilos_por_worker=hilos_por_worker)
        self.activo = False
        self.server_socket = None

    def _log(self, mensaje):
        if not self.silencioso:
            print(mensaje, flush=True)

    def manejar_cliente(self, socket_cliente, direccion):
        """
        Atiende a un cliente conectado en su propio hilo (Patrón Clase 10).
        Soporta múltiples solicitudes sobre la misma conexión TCP.
        """
        self._log(f"[CONEXIÓN] Cliente conectado desde {direccion}")
        buffer = ""
        try:
            with socket_cliente:
                while self.activo:
                    datos = socket_cliente.recv(BUFFER_SIZE)
                    if not datos:
                        break
                    buffer += datos.decode("utf-8", errors="replace")

                    while "\n" in buffer:
                        linea, buffer = buffer.split("\n", 1)
                        if not linea.strip():
                            continue

                        try:
                            solicitud = json.loads(linea)
                            respuesta = self.procesar_solicitud(solicitud)
                        except json.JSONDecodeError:
                            respuesta = {"estado": "ERROR", "mensaje": "JSON inválido"}
                        except Exception as exc:
                            respuesta = {"estado": "ERROR", "mensaje": str(exc)}

                        # Responder al cliente en bloque
                        mensaje_resp = (json.dumps(respuesta) + "\n").encode("utf-8")
                        socket_cliente.sendall(mensaje_resp)
        except (ConnectionResetError, BrokenPipeError, OSError):
            pass
        finally:
            self._log(f"[DESCONEXIÓN] Cliente {direccion} desconectado.")

    def procesar_solicitud(self, solicitud):
        if not isinstance(solicitud, dict):
            return {"estado": "ERROR", "mensaje": "Formato de solicitud inválido"}

        accion = solicitud.get("accion")

        if accion == "ENVIAR_TAREA":
            tarea = solicitud.get("tarea", {})
            return {
                "estado": "OK",
                "datos": self.cluster.despachar(tarea)
            }

        elif accion == "BATCH_TAREAS":
            tareas = solicitud.get("tareas", [])
            if not isinstance(tareas, list) or len(tareas) == 0:
                return {"estado": "OK", "cantidad": 0, "resultados": []}

            max_w = max(1, min(len(tareas), 12))
            with ThreadPoolExecutor(max_workers=max_w) as batch_pool:
                futuros = [batch_pool.submit(self.cluster.despachar, t) for t in tareas]
                resultados = [f.result() for f in futuros]
            return {
                "estado": "OK",
                "cantidad": len(resultados),
                "resultados": resultados
            }

        elif accion == "ESTADO_CLUSTER":
            return {
                "estado": "OK",
                "metricas": self.cluster.obtener_metricas()
            }

        elif accion == "HISTORIAL":
            limite = solicitud.get("limite", 10)
            return {
                "estado": "OK",
                "historial": self.cluster.almacenamiento.obtener_historial(limite)
            }

        elif accion == "PING":
            return {"estado": "OK", "mensaje": "PONG", "timestamp": time.time()}

        return {"estado": "ERROR", "mensaje": f"Acción desconocida: {accion}"}

    def iniciar(self):
        self.activo = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.puerto))
        self.server_socket.listen(16)
        self._log("=" * 65)
        self._log(" SERVIDOR DISTRIBUIDO DE TAREAS (PFO 3 - REDES)")
        self._log(f" Escuchando en TCP {self.host}:{self.puerto}")
        self._log(" Componentes integrados:")
        self._log("  - Cola de Mensajes: RabbitMQ simulada (Dispatcher asíncrono)")
        self._log(f"  - Clúster Workers: {len(self.cluster.workers)} nodos con Pool de Hilos")
        self._log("  - Almacenamiento: PostgreSQL (SQLite) + S3 ('s3_bucket/')")
        self._log(" Presione Ctrl+C para detener el servidor.")
        self._log("=" * 65)

        try:
            while self.activo:
                try:
                    cliente, direccion = self.server_socket.accept()
                    hilo = threading.Thread(
                        target=self.manejar_cliente,
                        args=(cliente, direccion),
                        daemon=True
                    )
                    hilo.start()
                except OSError:
                    break
        except KeyboardInterrupt:
            self._log("\n[APAGADO] Deteniendo servidor...")
        finally:
            self.detener()

    def detener(self):
        if not self.activo:
            return
        self.activo = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass
        self.cluster.detener()
        self._log("[SERVIDOR DETENIDO]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Servidor Distribuido de Tareas por Sockets TCP")
    parser.add_argument("--host", default=HOST_DEFAULT, help="Host de escucha TCP")
    parser.add_argument("--puerto", type=int, default=PUERTO_DEFAULT, help="Puerto de escucha TCP")
    parser.add_argument("--workers", type=int, default=3, help="Cantidad de Servidores Workers")
    parser.add_argument("--hilos", type=int, default=3, help="Hilos por cada Servidor Worker")
    parser.add_argument("--silencioso", action="store_true", help="Ocultar logs por consola (útil en lanzador unificado)")
    args = parser.parse_args()

    servidor = ServidorDistribuidoSockets(
        host=args.host,
        puerto=args.puerto,
        num_workers=args.workers,
        hilos_por_worker=args.hilos,
        silencioso=args.silencioso
    )
    servidor.iniciar()
