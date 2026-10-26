
import asyncio
import json
import os
import random

import websockets
from websockets.exceptions import ConnectionClosed

HOST = "0.0.0.0"
PORT = int(os.getenv("PORT", "10000"))
MAX_JUGADORES = 4

ESCALERAS = {
    3: 22, 8: 30, 20: 41, 27: 56,
    36: 55, 51: 67, 71: 92, 80: 99
}

SERPIENTES = {
    17: 4, 54: 34, 62: 19, 64: 60,
    87: 24, 93: 73, 95: 75, 98: 79
}


class JuegoServidor:
    def __init__(self):
        self.clientes = {}
        self.jugadores = {}
        self.turno = None
        self.ganador = None
        self.ultimo_dado = None
        self.ultimo_evento = "Esperando jugadores..."
        self.lock = asyncio.Lock()

    def estado_publico(self):
        return {
            "tipo": "estado",
            "jugadores": self.jugadores.copy(),
            "turno": self.turno,
            "ganador": self.ganador,
            "dado": self.ultimo_dado,
            "ultimo_dado": self.ultimo_dado,
            "ultimo_evento": self.ultimo_evento,
            "escaleras": ESCALERAS,
            "serpientes": SERPIENTES
        }

    async def enviar(self, websocket, mensaje):
        await websocket.send(json.dumps(mensaje, ensure_ascii=False))

    async def difundir(self, mensaje):
        conexiones = list(self.clientes.keys())
        if not conexiones:
            return

        await asyncio.gather(
            *(self.enviar(ws, mensaje) for ws in conexiones),
            return_exceptions=True
        )

    async def difundir_estado(self):
        await self.difundir(self.estado_publico())

    def siguiente_jugador(self, actual):
        ids = list(self.jugadores.keys())
        if not ids:
            return None
        if actual not in ids:
            return ids[0]
        return ids[(ids.index(actual) + 1) % len(ids)]

    async def conectar(self, websocket):
        async with self.lock:
            if len(self.clientes) >= MAX_JUGADORES:
                await self.enviar(websocket, {
                    "tipo": "lleno",
                    "mensaje": "El servidor está lleno. Máximo 4 jugadores."
                })
                return False

            ocupados = set(self.jugadores.keys())
            jugador_id = next(
                str(n) for n in range(1, MAX_JUGADORES + 1)
                if str(n) not in ocupados
            )

            self.clientes[websocket] = jugador_id
            self.jugadores[jugador_id] = {
                "posicion": 1,
                "nombre": f"Jugador {jugador_id}"
            }

            if self.turno is None:
                self.turno = jugador_id

            self.ultimo_evento = f"Jugador {jugador_id} se unió."

            await self.enviar(websocket, {
                "tipo": "conexion",
                "mi_id": jugador_id,
                "id": jugador_id,
                "escaleras": ESCALERAS,
                "serpientes": SERPIENTES
            })

            await self.difundir_estado()
            return True

    async def desconectar(self, websocket):
        async with self.lock:
            jugador_id = self.clientes.pop(websocket, None)
            if jugador_id is None:
                return

            self.jugadores.pop(jugador_id, None)

            if self.ganador == jugador_id:
                self.ganador = None

            if not self.jugadores:
                self.turno = None
                self.ganador = None
                self.ultimo_dado = None
                self.ultimo_evento = "Esperando jugadores..."
            else:
                if self.turno == jugador_id:
                    self.turno = self.siguiente_jugador(jugador_id)
                elif self.turno not in self.jugadores:
                    self.turno = next(iter(self.jugadores))

                self.ultimo_evento = f"Jugador {jugador_id} se desconectó."

            await self.difundir_estado()

    async def lanzar_dado(self, websocket):
        async with self.lock:
            jugador_id = self.clientes.get(websocket)
            if jugador_id is None:
                return

            if self.ganador is not None:
                await self.enviar(websocket, {
                    "tipo": "mensaje",
                    "mensaje": f"La partida terminó. Ganó el jugador {self.ganador}."
                })
                return

            if jugador_id != self.turno:
                await self.enviar(websocket, {
                    "tipo": "mensaje",
                    "mensaje": f"No es tu turno. Juega el jugador {self.turno}."
                })
                return

            dado = random.randint(1, 6)
            anterior = int(self.jugadores[jugador_id]["posicion"])
            propuesta = anterior + dado

            # Se requiere llegar exactamente a la casilla 100.
            intermedia = propuesta if propuesta <= 100 else anterior
            final = intermedia

            evento = f"Jugador {jugador_id} sacó {dado}."

            if intermedia in ESCALERAS:
                final = ESCALERAS[intermedia]
                evento += f" Sube por una escalera hasta {final}."
            elif intermedia in SERPIENTES:
                final = SERPIENTES[intermedia]
                evento += f" Baja por una serpiente hasta {final}."
            elif propuesta > 100:
                evento += " Necesita el número exacto para llegar a 100."

            self.jugadores[jugador_id]["posicion"] = final
            self.ultimo_dado = dado

            if final == 100:
                self.ganador = jugador_id
                self.ultimo_evento = f"¡Jugador {jugador_id} ganó la partida!"
            else:
                self.turno = self.siguiente_jugador(jugador_id)
                self.ultimo_evento = evento

            await self.difundir({
                "tipo": "resultado_dado",
                "jugador_id": jugador_id,
                "dado": dado,
                "posicion_anterior": anterior,
                "posicion_intermedia": intermedia,
                "posicion_final": final,
                "posicion": final,
                "mensaje": self.ultimo_evento,
                "jugadores": self.jugadores.copy(),
                "turno": self.turno,
                "ganador": self.ganador
            })

            await self.difundir_estado()

    async def manejar(self, websocket):
        conectado = await self.conectar(websocket)
        if not conectado:
            await websocket.close()
            return

        try:
            async for texto in websocket:
                try:
                    mensaje = json.loads(texto)
                except (json.JSONDecodeError, TypeError):
                    await self.enviar(websocket, {
                        "tipo": "error",
                        "mensaje": "El mensaje no es JSON válido."
                    })
                    continue

                if not isinstance(mensaje, dict):
                    continue

                tipo = mensaje.get("tipo", mensaje.get("type", ""))

                if tipo in ("lanzar_dado", "tirar_dado", "roll_dice"):
                    await self.lanzar_dado(websocket)

                elif tipo in ("abandonar", "salir", "disconnect"):
                    break

                elif tipo == "ping":
                    await self.enviar(websocket, {"tipo": "pong"})

                else:
                    await self.enviar(websocket, {
                        "tipo": "error",
                        "mensaje": f"Tipo de mensaje no reconocido: {tipo}"
                    })

        except ConnectionClosed:
            pass
        except Exception as error:
            print(f"Error de cliente: {error}")
        finally:
            await self.desconectar(websocket)


async def main():
    juego = JuegoServidor()
    print(f"Servidor WebSocket escuchando en {HOST}:{PORT}")

    async with websockets.serve(
        juego.manejar,
        HOST,
        PORT,
        ping_interval=20,
        ping_timeout=20,
        max_size=1_000_000
    ):
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Servidor detenido.")
