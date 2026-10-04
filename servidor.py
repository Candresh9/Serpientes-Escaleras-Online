```python
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

            "turno": self.turno,

            "ganador": self.ganador,

            "ultimo_dado": self.ultimo_dado,

            "dado": self.ultimo_dado,

            "ultimo_evento": self.ultimo_evento,

            "escaleras": ESCALERAS,

            "serpientes": SERPIENTES
        }


    # ========================================================
    # ENVIAR A TODOS
    # ========================================================

    async def enviar_todos(self, mensaje):

        texto = json.dumps(
            mensaje,
            ensure_ascii=False
        )

        conexiones = list(self.conexiones.values())

        if not conexiones:
            return

        resultados = await asyncio.gather(
            *[
                self.enviar_seguro(ws, texto)
                for ws in conexiones
            ],
            return_exceptions=True
        )

        for resultado in resultados:

            if isinstance(resultado, Exception):
                pass


    # ========================================================
    # ENVIAR ESTADO
    # ========================================================

    async def enviar_estado(self):

        await self.enviar_todos({
            "tipo": "estado",
            **self.obtener_estado()
        })


    # ========================================================
    # ENVÍO SEGURO
    # ========================================================

    async def enviar_seguro(self, websocket, texto):

        try:

            await websocket.send(texto)

            return True

        except Exception:

            return False


    # ========================================================
    # CONECTAR JUGADOR
    # ========================================================

    async def conectar(self, websocket):

        jugador_id = None

        try:

            async with self.lock:

                if len(self.jugadores) >= MAX_JUGADORES:

                    await websocket.send(
                        json.dumps({
                            "tipo": "lleno",
                            "mensaje": "El servidor está lleno. Máximo 4 jugadores."
                        }, ensure_ascii=False)
                    )

                    return


                jugador_id = self.siguiente_id

                self.siguiente_id += 1

                self.jugadores[jugador_id] = {
                    "id": jugador_id,
                    "nombre": f"Jugador {jugador_id}",
                    "posicion": 1,
                    "color": COLORES[
                        (jugador_id - 1) % len(COLORES)
                    ]
                }

                self.conexiones[jugador_id] = websocket


                # El primer jugador obtiene el turno
                if self.turno is None:

                    self.turno = jugador_id


                self.ultimo_evento = (
                    f"Jugador {jugador_id} se conectó."
                )


                estado_inicial = {
                    "tipo": "conexion",
                    "mi_id": jugador_id,
                    "jugador_id": jugador_id,
                    "turno": self.turno,
                    "jugadores": {
                        str(i): {
                            "id": j["id"],
                            "nombre": j["nombre"],
                            "posicion": j["posicion"],
                            "casilla": j["posicion"],
                            "color": j["color"]
                        }
                        for i, j in self.jugadores.items()
                    },
                    "ganador": self.ganador,
                    "ultimo_dado": self.ultimo_dado,
                    "ultimo_evento": self.ultimo_evento,
                    "escaleras": ESCALERAS,
                    "serpientes": SERPIENTES
                }


                await websocket.send(
                    json.dumps(
                        estado_inicial,
                        ensure_ascii=False
                    )
                )


                await self.enviar_estado()


            # =================================================
            # RECIBIR MENSAJES
            # =================================================

            async for mensaje in websocket:

                try:

                    datos = json.loads(mensaje)

                except json.JSONDecodeError:

                    continue


                await self.procesar_mensaje(
                    jugador_id,
                    datos
                )


        except websockets.exceptions.ConnectionClosed:

            pass

        except Exception as e:

            print(
                f"Error jugador {jugador_id}: {e}"
            )

        finally:

            await self.desconectar(jugador_id)


    # ========================================================
    # PROCESAR MENSAJES
    # ========================================================

    async def procesar_mensaje(
        self,
        jugador_id,
        datos
    ):

        tipo = datos.get("tipo")


        # ====================================================
        # PING
        # ====================================================

        if tipo == "ping":

            websocket = self.conexiones.get(jugador_id)

            if websocket:

                try:

                    await websocket.send(
                        json.dumps({
                            "tipo": "pong"
                        })
                    )

                except Exception:

                    pass

            return


        # ====================================================
        # CAMBIAR NOMBRE
        # ====================================================

        if tipo == "nombre":

            nombre = str(
                datos.get("nombre", "")
            ).strip()


            if not nombre:

                return


            nombre = nombre[:18]


            async with self.lock:

                if jugador_id in self.jugadores:

                    self.jugadores[
                        jugador_id
                    ]["nombre"] = nombre


                    self.ultimo_evento = (
                        f"Jugador {jugador_id} ahora se llama {nombre}."
                    )


            await self.enviar_estado()

            return


        # ====================================================
        # LANZAR DADO
        # ====================================================

        if tipo == "lanzar_dado":

            await self.lanzar_dado(jugador_id)

            return


        # ====================================================
        # REINICIAR
        # ====================================================

        if tipo == "reiniciar":

            await self.reiniciar()

            return


        # ====================================================
        # ABANDONAR
        # ====================================================

        if tipo == "abandonar":

            await self.desconectar(
                jugador_id,
                cerrar=True
            )

            return


    # ========================================================
    # LANZAR DADO
    # ========================================================

    async def lanzar_dado(self, jugador_id):

        async with self.lock:

            # -----------------------------------------------
            # Validaciones
            # -----------------------------------------------

            if jugador_id not in self.jugadores:

                return


            if self.ganador is not None:

                return


            if self.turno != jugador_id:

                websocket = self.conexiones.get(
                    jugador_id
                )

                if websocket:

                    try:

                        await websocket.send(
                            json.dumps({
                                "tipo": "mensaje",
                                "mensaje": "No es tu turno."
                            }, ensure_ascii=False)
                        )

                    except Exception:

                        pass

                return


            # -----------------------------------------------
            # DADO
            # -----------------------------------------------

            dado = random.randint(1, 6)

            self.ultimo_dado = dado


            jugador = self.jugadores[
                jugador_id
            ]


            posicion_anterior = jugador[
                "posicion"
            ]


            posicion = posicion_anterior + dado


            # -----------------------------------------------
            # No pasar de 100
            # -----------------------------------------------

            if posicion > 100:

                posicion = posicion_anterior


            # -----------------------------------------------
            # Movimiento
            # -----------------------------------------------

            jugador["posicion"] = posicion


            posicion_final = posicion


            # -----------------------------------------------
            # Escalera
            # -----------------------------------------------

            if posicion in ESCALERAS:

                posicion_final = ESCALERAS[
                    posicion
                ]

                jugador["posicion"] = posicion_final

                self.ultimo_evento = (
                    f"{jugador['nombre']} sacó {dado} "
                    f"y subió por una escalera "
                    f"de {posicion} a {posicion_final}."
                )


            # -----------------------------------------------
            # Serpiente
            # -----------------------------------------------

            elif posicion in SERPIENTES:

                posicion_final = SERPIENTES[
                    posicion
                ]

                jugador["posicion"] = posicion_final

                self.ultimo_evento = (
                    f"{jugador['nombre']} sacó {dado} "
                    f"y bajó por una serpiente "
                    f"de {posicion} a {posicion_final}."
                )


            else:

                self.ultimo_evento = (
                    f"{jugador['nombre']} sacó {dado} "
                    f"y llegó a la casilla {posicion_final}."
                )


            # -----------------------------------------------
            # GANADOR
            # -----------------------------------------------

            if posicion_final == 100:

                self.ganador = jugador_id

                self.ultimo_evento = (
                    f"🏆 {jugador['nombre']} ganó la partida."
                )


            else:

                # -------------------------------------------
                # Siguiente turno
                # -------------------------------------------

                ids = sorted(
                    self.jugadores.keys()
                )

                if ids:

                    try:

                        indice = ids.index(
                            jugador_id
                        )

                        siguiente = (
                            indice + 1
                        ) % len(ids)

                        self.turno = ids[
                            siguiente
                        ]

                    except ValueError:

                        self.turno = ids[0]


        # ====================================================
        # ENVIAR RESULTADO DEL DADO
        # ====================================================

        await self.enviar_todos({

            "tipo": "resultado_dado",

            "jugador_id": jugador_id,

            "dado": dado,

            "ultimo_dado": dado,

            "posicion_anterior":
                posicion_anterior,

            "posicion":
                posicion_final,

            "turno": self.turno,

            "ganador": self.ganador
        })


        # ====================================================
        # ENVIAR ESTADO
        # ====================================================

        await self.enviar_estado()


    # ========================================================
    # REINICIAR
    # ========================================================

    async def reiniciar(self):

        async with self.lock:

            if not self.jugadores:

                return


            for jugador in self.jugadores.values():

                jugador["posicion"] = 1


            self.ganador = None

            self.ultimo_dado = None

            ids = sorted(
                self.jugadores.keys()
            )


            if ids:

                self.turno = ids[0]


            self.ultimo_evento = (
                "🔄 La partida fue reiniciada."
            )


        await self.enviar_estado()


    # ========================================================
    # DESCONECTAR
    # ========================================================

    async def desconectar(
        self,
        jugador_id,
        cerrar=False
    ):

        if jugador_id is None:

            return


        async with self.lock:

            if jugador_id not in self.jugadores:

                return


            jugador = self.jugadores[
                jugador_id
            ]


            nombre = jugador["nombre"]


            websocket = self.conexiones.get(
                jugador_id
            )


            if cerrar and websocket:

                try:

                    await websocket.send(
                        json.dumps({
                            "tipo": "servidor_cerrando",
                            "mensaje": "Has abandonado la partida."
                        }, ensure_ascii=False)
                    )

                except Exception:

                    pass


            self.jugadores.pop(
                jugador_id,
                None
            )

            self.conexiones.pop(
                jugador_id,
                None
            )


            if self.jugadores:

                ids = sorted(
                    self.jugadores.keys()
                )


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

                self.ultimo_evento = (
                    "Esperando jugadores..."
                )


        await self.enviar_estado()


# ============================================================
# MAIN
# ============================================================

async def main():

    juego = JuegoServidor()


    print(
        f"Servidor iniciado en "
        f"{HOST}:{PORT}"
    )


    async with websockets.serve(
        juego.conectar,
        HOST,
        PORT,
        ping_interval=20,
        ping_timeout=20
    ):

        print(
            "Servidor WebSocket listo."
        )

        await asyncio.Future()


if __name__ == "__main__":

    asyncio.run(main())
```
