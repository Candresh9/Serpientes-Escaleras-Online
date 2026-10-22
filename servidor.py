import asyncio
import json
import os
import random
import websockets


# ============================================================
# CONFIGURACIÓN
# ============================================================

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 10000))

MAX_JUGADORES = 4


# ============================================================
# TABLERO
# ============================================================

ESCALERAS = {
    4: 25,
    13: 46,
    33: 49,
    42: 63,
    50: 69,
    62: 81,
    74: 92
}

SERPIENTES = {
    27: 5,
    40: 3,
    43: 18,
    54: 31,
    66: 45,
    76: 58,
    89: 53,
    99: 41
}


COLORES = [
    "#e53935",
    "#1976d2",
    "#43a047",
    "#8e24aa"
]


# ============================================================
# SERVIDOR DEL JUEGO
# ============================================================

class JuegoServidor:

    def __init__(self):

        self.jugadores = {}

        self.conexiones = {}

        self.siguiente_id = 1

        self.turno = None

        self.ganador = None

        self.ultimo_dado = None

        self.ultimo_evento = "Esperando jugadores..."

        self.lock = asyncio.Lock()


    # ========================================================
    # CREAR ESTADO
    # ========================================================

    def obtener_estado(self):

        jugadores_estado = {}

        for jugador_id, jugador in self.jugadores.items():

            jugadores_estado[str(jugador_id)] = {
                "id": jugador_id,
                "nombre": jugador["nombre"],
                "posicion": jugador["posicion"],
                "casilla": jugador["posicion"],
                "color": jugador["color"]
            }

        return {
            "jugadores": jugadores_estado,
