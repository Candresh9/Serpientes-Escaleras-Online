import asyncio
import json
import os
import random
import websockets

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "10000"))
MAX_JUGADORES = 4

ESCALERAS = {
    4: 25,
    13: 46,
    33: 49,
    42: 63,
    50: 69,
    62: 81,
    74: 92,
}

SERPIENTES = {
    27: 5,
    40: 3,
    43: 18,
    54: 31,
    66: 45,
    76: 58,
    89: 53,
    99: 41,
}

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
                *(
                    self.enviar_seguro(websocket, texto)
                    for websocket in conexiones
                ),
                return_exceptions=True,
            )

    async def enviar_estado(self):
        await self.enviar_todos({
            "tipo": "estado",
            **self.obtener_estado(),
        })

    async def conectar(self, websocket):
        jugador_id = None

        try:
            async with self.lock:
                if len(self.jugadores) >= MAX_JUGADORES:
                    await websocket.send(json.dumps({
                        "tipo": "lleno",
                        "mensaje": (
                            "El servidor está lleno. "
                            "Máximo 4 jugadores."
                        ),
                    }, ensure_ascii=False))
                    return

                jugador_id = self.siguiente_id
                self.siguiente_id += 1

                self.jugadores[jugador_id] = {
                    "id": jugador_id,
                    "nombre": f"Jugador {jugador_id}",
                    "posicion": 1,
                    "color": COLORES[
                        (jugador_id - 1) % len(COLORES)
                    ],
                }

                self.conexiones[jugador_id] = websocket

                if self.turno is None:
                    self.turno = jugador_id

                self.ultimo_evento = (
                    f"Jugador {jugador_id} se conectó."
                )

                estado_inicial = {
                    "tipo": "conexion",
                    "mi_id": jugador_id,
                    "jugador_id": jugador_id,
                    **self.obtener_estado(),
                }

                await websocket.send(
                    json.dumps(estado_inicial, ensure_ascii=False)
                )

            await self.enviar_estado()

            async for mensaje in websocket:
                try:
                    datos = json.loads(mensaje)
                except (json.JSONDecodeError, TypeError):
                    continue

                if isinstance(datos, dict):
                    await self.procesar_mensaje(
                        jugador_id,
                        datos,
                    )

        except websockets.exceptions.ConnectionClosed:
            pass

        except Exception as error:
            print(f"Error con el jugador {jugador_id}: {error}")

        finally:
            await self.desconectar(jugador_id)

    async def procesar_mensaje(self, jugador_id, datos):
        tipo = datos.get("tipo")

        if tipo == "ping":
            websocket = self.conexiones.get(jugador_id)

            if websocket:
                await self.enviar_seguro(
                    websocket,
                    json.dumps({"tipo": "pong"}),
                )

            return

        if tipo == "nombre":
            nombre = str(datos.get("nombre", "")).strip()[:18]

            if not nombre:
                return

            async with self.lock:
                if jugador_id in self.jugadores:
                    self.jugadores[jugador_id]["nombre"] = nombre
                    self.ultimo_evento = (
                        f"Jugador {jugador_id} ahora se llama {nombre}."
                    )

            await self.enviar_estado()
            return

        if tipo == "lanzar_dado":
            await self.lanzar_dado(jugador_id)

        elif tipo == "reiniciar":
            await self.reiniciar()

        elif tipo == "abandonar":
            await self.desconectar(jugador_id, cerrar=True)

    async def lanzar_dado(self, jugador_id):
        async with self.lock:
            if jugador_id not in self.jugadores:
                return

            if self.ganador is not None:
                return

            if self.turno != jugador_id:
                websocket = self.conexiones.get(jugador_id)

                if websocket:
                    await self.enviar_seguro(
                        websocket,
                        json.dumps({
                            "tipo": "mensaje",
                            "mensaje": "No es tu turno.",
                        }, ensure_ascii=False),
                    )

                return

            dado = random.randint(1, 6)
            self.ultimo_dado = dado

            jugador = self.jugadores[jugador_id]
            posicion_anterior = jugador["posicion"]
            posicion_intermedia = posicion_anterior + dado

            # Para ganar hay que llegar exactamente a la casilla 100.
            # Si el dado supera 100, el jugador permanece en su casilla.
            if posicion_intermedia > 100:
                posicion_intermedia = posicion_anterior

            posicion_final = posicion_intermedia
            evento_especial = None

            # Comprobar si el jugador cae en una escalera.
            if posicion_intermedia in ESCALERAS:
                posicion_final = ESCALERAS[posicion_intermedia]
                evento_especial = "escalera"

            # Comprobar si el jugador cae en una serpiente.
            elif posicion_intermedia in SERPIENTES:
                posicion_final = SERPIENTES[posicion_intermedia]
                evento_especial = "serpiente"

            jugador["posicion"] = posicion_final

            if posicion_final == 100:
                self.ganador = jugador_id
                self.ultimo_evento = (
                    f"¡{jugador['nombre']} ganó la partida!"
                )

            elif evento_especial == "escalera":
                self.ultimo_evento = (
                    f"{jugador['nombre']} sacó {dado} y subió "
                    f"por una escalera de {posicion_intermedia} "
                    f"a {posicion_final}."
                )

            elif evento_especial == "serpiente":
                self.ultimo_evento = (
                    f"{jugador['nombre']} sacó {dado} y bajó "
                    f"por una serpiente de {posicion_intermedia} "
                    f"a {posicion_final}."
                )

            else:
                self.ultimo_evento = (
                    f"{jugador['nombre']} sacó {dado} y llegó "
                    f"a la casilla {posicion_final}."
                )

            # Cambiar el turno al siguiente jugador.
            if self.ganador is None:
                ids = sorted(self.jugadores.keys())

                if ids:
                    indice = ids.index(jugador_id)
                    self.turno = ids[(indice + 1) % len(ids)]

            resultado = {
                "tipo": "resultado_dado",
                "jugador_id": jugador_id,
                "dado": dado,
                "ultimo_dado": dado,

                # El cliente puede usar estos valores para animar
                # el recorrido y después la escalera o serpiente.
                "posicion_anterior": posicion_anterior,
                "posicion_intermedia": posicion_intermedia,
                "posicion_final": posicion_final,

                # Compatibilidad con clientes anteriores.
                "posicion": posicion_final,

                "turno": self.turno,
                "ganador": self.ganador,
                "escaleras": ESCALERAS,
                "serpientes": SERPIENTES,
            }

        # Enviar primero el resultado del dado y después
        # el estado definitivo de la partida.
        await self.enviar_todos(resultado)
        await self.enviar_estado()

    async def reiniciar(self):
        async with self.lock:
            if not self.jugadores:
                return

            for jugador in self.jugadores.values():
                jugador["posicion"] = 1

            self.ganador = None
            self.ultimo_dado = None

            ids = sorted(self.jugadores.keys())
            self.turno = ids[0] if ids else None

            self.ultimo_evento = "La partida fue reiniciada."

        await self.enviar_estado()

    async def desconectar(self, jugador_id, cerrar=False):
        if jugador_id is None:
            return

        async with self.lock:
            if jugador_id not in self.jugadores:
                return

            jugador = self.jugadores[jugador_id]
            nombre = jugador["nombre"]
            websocket = self.conexiones.get(jugador_id)

            if cerrar and websocket:
                await self.enviar_seguro(
                    websocket,
                    json.dumps({
                        "tipo": "servidor_cerrando",
                        "mensaje": "Has abandonado la partida.",
                    }, ensure_ascii=False),
                )

            self.jugadores.pop(jugador_id, None)
            self.conexiones.pop(jugador_id, None)

            if self.jugadores:
                ids = sorted(self.jugadores.keys())

                if self.turno == jugador_id:
                    self.turno = ids[0]

                if self.ganador == jugador_id:
                    self.ganador = None

                self.ultimo_evento = (
                    f"{nombre} abandonó la partida."
                )

            else:
                self.turno = None
                self.ganador = None
                self.ultimo_dado = None
                self.ultimo_evento = "Esperando jugadores..."

        await self.enviar_estado()


async def main():
    juego = JuegoServidor()

    print(f"Servidor iniciando en {HOST}:{PORT}")

    async with websockets.serve(
        juego.conectar,
        HOST,
        PORT,
        ping_interval=20,
        ping_timeout=20,
        max_size=1_000_000,
    ):
        print("Servidor WebSocket listo.")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
