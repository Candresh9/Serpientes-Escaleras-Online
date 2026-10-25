import pygame
import asyncio
import threading
import json
import math
import websockets


# ============================================================
# CONFIGURACIÓN
# ============================================================

SERVER_IP = "192.168.18.43"
PORT = 10000
SERVER_URL = f"ws://{SERVER_IP}:{PORT}"

ANCHO = 1100
ALTO = 760

MARGEN = 35
CELDA = 68

PANEL_X = 755
PANEL_ANCHO = 310

FPS = 60


# ============================================================
# COLORES
# ============================================================

BLANCO = (255, 255, 255)
NEGRO = (20, 20, 20)

GRIS = (220, 220, 220)
GRIS_OSCURO = (80, 80, 80)

ROJO = (235, 80, 80)
AZUL = (70, 150, 245)

VERDE = (80, 200, 110)
VERDE_ESCALERA = (30, 170, 70)

ROJO_SERPIENTE = (210, 50, 50)

FONDO = (235, 235, 235)


# ============================================================
# ESTADO COMPARTIDO
# ============================================================

class EstadoJuego:

    def __init__(self):
        self.mi_id = None
        self.jugadores = {}
        self.animaciones = {}
        self.turno = None
        self.ganador = None
        self.escaleras = {}
        self.serpientes = {}
        self.ultimo_dado = None
        self.ultimo_evento = "Conectando al servidor..."
        self.conectado = False
        self.ejecutando = True
        self.confirmar_abandono = False
        self.lock = threading.RLock()


# ============================================================
# CLIENTE DE RED WEBSOCKET
# ============================================================

class ClienteRed:
    """Cliente WebSocket compatible con el servidor basado en websockets."""

    def __init__(self, estado):
        self.estado = estado
        self.hilo = None
        self.loop = None
        self.cola_envio = None
        self.websocket = None
        self.cerrando = False
        self.conectado_evento = threading.Event()
        self.error_conexion = None

    def conectar(self):
        self.cerrando = False
        self.conectado_evento.clear()
        self.error_conexion = None

        self.hilo = threading.Thread(
            target=self._ejecutar_loop,
            daemon=True
        )
        self.hilo.start()

        if not self.conectado_evento.wait(timeout=8):
            self.error(
                "Tiempo de espera agotado al conectar con el servidor."
            )
            self.cerrar()
            return False

        if self.error_conexion:
            self.error(self.error_conexion)
            return False

        with self.estado.lock:
            self.estado.conectado = True
            self.estado.ultimo_evento = f"Conectado a {SERVER_URL}"

        return True

    def _ejecutar_loop(self):
        try:
            asyncio.run(self._sesion_websocket())
        except Exception as error:
            if not self.cerrando:
                self.error_conexion = (
                    f"No se pudo conectar con {SERVER_URL}: {error}"
                )
                self.conectado_evento.set()
                self.error(self.error_conexion)

    async def _sesion_websocket(self):
        self.loop = asyncio.get_running_loop()
        self.cola_envio = asyncio.Queue()

        try:
            async with websockets.connect(
                SERVER_URL,
                open_timeout=6,
                ping_interval=20,
                ping_timeout=20,
                max_size=1_000_000,
            ) as websocket:

                self.websocket = websocket

                with self.estado.lock:
                    self.estado.conectado = True
                    self.estado.ultimo_evento = f"Conectado a {SERVER_URL}"

                self.conectado_evento.set()

                tarea_envio = asyncio.create_task(
                    self._bucle_envio(websocket)
                )

                try:
                    async for texto in websocket:
                        if self.cerrando:
                            break

                        try:
                            mensaje = json.loads(texto)
                        except (json.JSONDecodeError, TypeError):
                            continue

                        if isinstance(mensaje, dict):
                            self.procesar_mensaje(mensaje)

                finally:
                    tarea_envio.cancel()

                    try:
                        await tarea_envio
                    except asyncio.CancelledError:
                        pass

        except Exception as error:
            if not self.cerrando:
                self.error_conexion = (
                    f"Conexión WebSocket interrumpida: {error}"
                )
                self.conectado_evento.set()
                self.error(self.error_conexion)

        finally:
            self.websocket = None

            with self.estado.lock:
                self.estado.conectado = False

                if not self.cerrando and self.estado.ejecutando:
                    self.estado.ultimo_evento = (
                        "Se perdió la conexión con el servidor."
                    )

    async def _bucle_envio(self, websocket):
        while not self.cerrando:
            mensaje = await self.cola_envio.get()

            if mensaje is None:
                break

            await websocket.send(
                json.dumps(mensaje, ensure_ascii=False)
            )

    def error(self, mensaje):
        with self.estado.lock:
            self.estado.conectado = False
            self.estado.ultimo_evento = mensaje

    def enviar(self, mensaje):
        if self.cerrando or not self.loop or not self.cola_envio:
            return False

        try:
            futuro = asyncio.run_coroutine_threadsafe(
                self.cola_envio.put(mensaje),
                self.loop
            )
            futuro.result(timeout=2)
            return True

        except Exception:
            with self.estado.lock:
                self.estado.conectado = False

                if not self.cerrando:
                    self.estado.ultimo_evento = (
                        "No se pudo enviar el mensaje al servidor."
                    )

            return False

    def procesar_mensaje(self, mensaje):
        tipo = mensaje.get("tipo")

        if tipo in ("asignar_id", "conexion"):
            with self.estado.lock:
                mi_id = mensaje.get(
                    "mi_id",
                    mensaje.get("id", mensaje.get("jugador_id"))
                )

                if mi_id is not None:
                    self.estado.mi_id = mi_id

                self.estado.escaleras = mensaje.get(
                    "escaleras",
                    self.estado.escaleras
                )

                self.estado.serpientes = mensaje.get(
                    "serpientes",
                    self.estado.serpientes
                )

                if mi_id is not None:
                    self.estado.ultimo_evento = (
                        f"Conectado como Jugador {mi_id}."
                    )

            return

        if tipo == "estado":
            with self.estado.lock:
                self.estado.jugadores = mensaje.get("jugadores", {})
                self.estado.turno = mensaje.get("turno")
                self.estado.ganador = mensaje.get("ganador")

                self.estado.ultimo_dado = mensaje.get(
                    "dado",
                    mensaje.get("ultimo_dado")
                )

                self.estado.ultimo_evento = mensaje.get(
                    "ultimo_evento",
                    self.estado.ultimo_evento
                )

                self.estado.escaleras = mensaje.get(
                    "escaleras",
                    self.estado.escaleras
                )

                self.estado.serpientes = mensaje.get(
                    "serpientes",
                    self.estado.serpientes
                )

            return

        if tipo == "resultado_dado":
            with self.estado.lock:
                dado = mensaje.get("dado")

                if dado is not None:
                    self.estado.ultimo_dado = dado

                jugador_id = mensaje.get("jugador_id")

                try:
                    anterior = int(mensaje.get("posicion_anterior"))
                    intermedia = int(mensaje.get("posicion_intermedia"))
                    final = int(
                        mensaje.get(
                            "posicion_final",
                            mensaje.get("posicion")
                        )
                    )

                    if (
                        jugador_id is not None
                        and all(
                            1 <= n <= 100
                            for n in (anterior, intermedia, final)
                        )
                    ):
                        ruta = []

                        if intermedia > anterior:
                            ruta.extend(
                                range(anterior + 1, intermedia + 1)
                            )
                        elif intermedia < anterior:
                            ruta.append(intermedia)

                        if final != intermedia:
                            ruta.append(final)

                        if not ruta:
                            ruta = [final]

                        self.estado.animaciones[str(jugador_id)] = {
                            "ruta": ruta,
                            "indice": 0,
                            "casilla": anterior,
                            "siguiente": pygame.time.get_ticks() + 180,
                        }

                except (TypeError, ValueError):
                    pass

                self.estado.ultimo_evento = mensaje.get(
                    "mensaje",
                    self.estado.ultimo_evento
                )

            return

        if tipo == "mensaje":
            with self.estado.lock:
                self.estado.ultimo_evento = mensaje.get("mensaje", "")
            return

        if tipo in ("reinicio", "reiniciar"):
            with self.estado.lock:
                self.estado.animaciones.clear()
                self.estado.ultimo_dado = None
                self.estado.ganador = None
                self.estado.ultimo_evento = mensaje.get(
                    "mensaje",
                    "Partida reiniciada."
                )
            return

        if tipo in ("lleno", "servidor_cerrando", "servidor_cerrado"):
            with self.estado.lock:
                self.estado.ultimo_evento = mensaje.get(
                    "mensaje",
                    "El servidor se cerró."
                )
                self.estado.conectado = False

                if tipo == "lleno":
                    self.estado.ejecutando = False

            return

    def cerrar(self):
        if self.cerrando:
            return

        self.cerrando = True

        with self.estado.lock:
            self.estado.conectado = False

        if self.loop and self.cola_envio:
            try:
                asyncio.run_coroutine_threadsafe(
                    self.cola_envio.put(None),
                    self.loop
                )
            except Exception:
                pass

        if (
            self.hilo
            and self.hilo.is_alive()
            and threading.current_thread() is not self.hilo
        ):
            self.hilo.join(timeout=2)


# ============================================================
# FUNCIONES DE COORDENADAS
# ============================================================

def centro_casilla(casilla):
    try:
        casilla = int(casilla)
    except (TypeError, ValueError):
        return None

    if casilla < 1 or casilla > 100:
        return None

    fila_desde_abajo = (casilla - 1) // 10
    columna = (casilla - 1) % 10

    if fila_desde_abajo % 2 == 1:
        columna = 9 - columna

    x = MARGEN + columna * CELDA + CELDA // 2
    y = MARGEN + (9 - fila_desde_abajo) * CELDA + CELDA // 2

    return x, y
