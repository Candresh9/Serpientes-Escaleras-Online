import asyncio
import json
import os
import random
import websockets

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "10000"))
MAX_JUGADORES = 4

ESCALERAS = {4: 25, 13: 46, 33: 49, 42: 63, 50: 69, 62: 81, 74: 92}
SERPIENTES = {27: 5, 40: 3, 43: 18, 54: 31, 66: 45, 76: 58, 89: 53, 99: 41}
COLORES = ["#e53935", "#1976d2", "#43a047", "#8e24aa"]


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

    def obtener_estado(self):
        jugadores_estado = {}
        for jugador_id, jugador in self.jugadores.items():
            jugadores_estado[str(jugador_id)] = {
                "id": jugador_id,
                "nombre": jugador["nombre"],
                "posicion": jugador["posicion"],
                "casilla": jugador["posicion"],
                "color": jugador["color"],
            }
        return {
            "jugadores": jugadores_estado,
            "turno": self.turno,
            "ganador": self.ganador,
            "ultimo_dado": self.ultimo_dado,
            "dado": self.ultimo_dado,
            "ultimo_evento": self.ultimo_evento,
            "escaleras": ESCALERAS,
            "serpientes": SERPIENTES,
        }

    async def enviar_seguro(self, websocket, texto):
        try:
            await websocket.send(texto)
            return True
        except Exception as error:
            print(f"Error al enviar mensaje: {error}")
            return False

    async def enviar_todos(self, mensaje):
        texto = json.dumps(mensaje, ensure_ascii=False)
        conexiones = list(self.conexiones.values())
        if conexiones:
            await asyncio.gather(
                *(self.enviar_seguro(ws, texto) for ws in conexiones),
                return_exceptions=True,
            )

    async def enviar_estado(self):
        await self.enviar_todos({"tipo": "estado", **self.obtener_estado()})

    async def conectar(self, websocket):
        jugador_id = None
        try:
            async with self.lock:
                if len(self.jugadores) >= MAX_JUGADORES:
                    await websocket.send(json.dumps({
                        "tipo": "lleno",
                        "mensaje": "El servidor está lleno. Máximo 4 jugadores.",
                    }, ensure_ascii=False))
                    return

                jugador_id = self.siguiente_id
                self.siguiente_id += 1
                self.jugadores[jugador_id] = {
                    "id": jugador_id,
                    "nombre": f"Jugador {jugador_id}",
                    "posicion": 1,
                    "color": COLORES[(jugador_id - 1) % len(COLORES)],
                }
                self.conexiones[jugador_id] = websocket
                if self.turno is None:
                    self.turno = jugador_id
                self.ultimo_evento = f"Jugador {jugador_id} se conectó."
                estado_inicial = {
                    "tipo": "conexion",
                    "mi_id": jugador_id,
                    "jugador_id": jugador_id,
                    **self.obtener_estado(),
                }
                await websocket.send(json.dumps(estado_inicial, ensure_ascii=False))
