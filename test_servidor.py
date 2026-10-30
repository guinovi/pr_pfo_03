"""
Pruebas Unitarias Automatizadas para el Sistema Distribuido (PFO 3)
Materia: Programación sobre Redes
Alumno: Novillo Guillermo Tadeo

Ejecución:
    python3 -m unittest test_servidor.py
    o
    python3 test_servidor.py
"""

import unittest
import threading
import time
import os
import shutil
from servidor import ServidorDistribuidoSockets, AlmacenamientoDistribuido
from cliente import ClienteDistribuido

TEST_PORT = 12456
TEST_DB = "test_sistema_distribuido.db"
TEST_S3 = "test_s3_bucket"


class TestSistemaDistribuido(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Limpiar residuos previos de pruebas
        for path in [TEST_DB, TEST_S3]:
            if os.path.isfile(path):
                os.remove(path)
            elif os.path.isdir(path):
                shutil.rmtree(path)

        # Iniciar servidor en hilo secundario
        cls.almacenamiento = AlmacenamientoDistribuido(db_path=TEST_DB, s3_dir=TEST_S3)
        cls.servidor = ServidorDistribuidoSockets(
            host="127.0.0.1",
            puerto=TEST_PORT,
            num_workers=3,
            hilos_por_worker=2
        )
        cls.servidor.cluster.almacenamiento = cls.almacenamiento
        for w in cls.servidor.cluster.workers:
            w.almacenamiento = cls.almacenamiento

        cls.server_thread = threading.Thread(target=cls.servidor.iniciar, daemon=True)
        cls.server_thread.start()
        time.sleep(0.4)  # Tiempo para enlace de socket

        cls.cliente = ClienteDistribuido(host="127.0.0.1", puerto=TEST_PORT)

    @classmethod
    def tearDownClass(cls):
        cls.servidor.detener()
        time.sleep(0.3)
        for path in [TEST_DB, TEST_S3]:
            if os.path.isfile(path):
                try:
                    os.remove(path)
                except Exception:
                    pass
            elif os.path.isdir(path):
                try:
                    shutil.rmtree(path)
                except Exception:
                    pass

    def test_01_ping_conexion(self):
        """Verifica la conectividad básica TCP mediante PING."""
        res = self.cliente.enviar_solicitud({"accion": "PING"})
        self.assertEqual(res.get("estado"), "OK")
        self.assertEqual(res.get("mensaje"), "PONG")

    def test_02_tarea_hash_criptografico(self):
        """Valida el procesamiento de una tarea criptográfica en un Worker."""
        texto_prueba = "Sistemas Distribuidos 2026"
        res = self.cliente.enviar_tarea("hash_criptografico", {"texto": texto_prueba})
        self.assertEqual(res.get("estado"), "OK")
        d = res.get("datos", {})
        self.assertTrue(d.get("worker_asignado").startswith("Worker-"))
        self.assertIn("Hilo", d.get("hilo_ejecutor"))
        self.assertIn("hash_sha256", d.get("resultado", {}))

    def test_03_almacenamiento_s3_por_bloques_y_seguridad_path(self):
        """Prueba la escritura en S3 simulado con buffers de 4KB y prevención de Path Traversal."""
        contenido = "Prueba de buffer distribuido" * 200
        nombre_intento_traversal = "../../archivo_prueba.txt"
        res = self.cliente.enviar_tarea("guardar_archivo_s3", {
            "archivo": nombre_intento_traversal,
            "contenido": contenido
        })
        self.assertEqual(res.get("estado"), "OK")
        d = res.get("datos", {})
        self.assertEqual(d.get("s3_key"), "s3://distribuido-tasks/archivo_prueba.txt")

        # Verificar que el archivo realmente existe dentro del bucket y no afuera
        ruta_real = os.path.join(TEST_S3, "archivo_prueba.txt")
        self.assertTrue(os.path.exists(ruta_real))
        with open(ruta_real, "r", encoding="utf-8") as f:
            self.assertEqual(f.read(), contenido)

    def test_04_batch_concurrente_y_rafaga_sockets(self):
        """
        Verifica que tanto el despacho BATCH como múltiples clientes TCP concurrentes
        distribuyan las tareas entre los distintos Workers y los hilos de sus pools.
        """
        tareas = [
            {"tipo": "calculo_intensivo", "parametros": {"n": 20000, "demora": 0.08}}
            for _ in range(6)
        ]
        resultados = self.cliente.enviar_rafaga_sockets_concurrentes(tareas)
        self.assertEqual(len(resultados), 6)

        workers_usados = {r.get("worker_asignado") for r in resultados}
        hilos_usados = {r.get("hilo_ejecutor") for r in resultados}

        # Demostrar que se usaron los 3 workers y múltiples hilos del pool
        self.assertEqual(len(workers_usados), 3, "Las 6 tareas deben balancearse entre los 3 Workers")
        self.assertGreater(len(hilos_usados), 3, "Deben usarse múltiples hilos de los pools")

        # Verificar también que un batch vacío no produzca errores
        res_vacio = self.cliente.enviar_batch([])
        self.assertEqual(res_vacio.get("estado"), "OK")
        self.assertEqual(res_vacio.get("cantidad"), 0)

    def test_05_estado_cluster_e_historial(self):
        """Verifica consulta de métricas de RabbitMQ y persistencia en base de datos."""
        res_m = self.cliente.obtener_estado_cluster()
        self.assertEqual(res_m.get("estado"), "OK")
        m = res_m.get("metricas", {})
        self.assertGreaterEqual(m.get("total_procesadas"), 8)

        res_h = self.cliente.obtener_historial(limite=5)
        self.assertEqual(res_h.get("estado"), "OK")
        hist = res_h.get("historial", [])
        self.assertGreaterEqual(len(hist), 1)
        self.assertEqual(hist[0].get("estado"), "COMPLETADA")

    def test_06_reintentos_conexion_puerto_inactivo(self):
        """Verifica que el cliente intente reconectar y falle controladamente si el servidor no existe."""
        cliente_invalido = ClienteDistribuido(host="127.0.0.1", puerto=59999)
        with self.assertRaises(ConnectionError):
            cliente_invalido.conectar_con_reintentos(intentos=2, espera=0.05)


if __name__ == "__main__":
    unittest.main()
