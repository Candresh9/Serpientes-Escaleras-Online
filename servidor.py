import os
import asyncio
import json
import random
import websockets


# ============================================================
# CONFIGURACIÓN
# ============================================================

HOST = "0.0.0.0"

# Render proporciona PORT automáticamente.
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


# ============================================================
# SERVIDOR
# ============================================================

class ServidorJuego:

    def __init__(self):

        self.clientes = {}
        self.jugadores = {}

        self.siguiente_id = 1

        self.turno_actual = None
        self.ganador = None

        self.ultimo_dado = 0
        self.ultimo_evento = "Esperando jugadores..."

        self.lock = asyncio.Lock()

        print("=" * 60)
        print("     SERPIENTES Y ESCALERAS - SERVIDOR ONLINE")
        print("=" * 60)
        print(f"Puerto: {PORT}")
        print("Esperando jugadores...")
        print()


    # ========================================================
    # CONEXIÓN
    # ========================================================

    async def conectar(self, websocket):

        jugador_id = None

        async with self.lock:

            if len(self.clientes) >= MAX_JUGADORES:

                await websocket.send(
                    json.dumps({
                        "tipo": "lleno",
                        "mensaje": "La partida está llena."
                    })
                )

                return

            # Buscar ID disponible
            for posible_id in range(1, MAX_JUGADORES + 1):

                if posible_id not in self.clientes:

                    jugador_id = posible_id
                    break

            if jugador_id is None:

                await websocket.send(
                    json.dumps({
                        "tipo": "lleno",
                        "mensaje": "La partida está llena."
                    })
                )

                return

            self.clientes[jugador_id] = websocket

            self.jugadores[jugador_id] = {

                "id": jugador_id,

                "nombre":
                    f"Jugador {jugador_id}",

                "casilla": 1,

                "color":
                    self.obtener_color(jugador_id)
            }

            if self.turno_actual is None:

                self.turno_actual = jugador_id

        print(
            f"[CONEXIÓN] Jugador {jugador_id} conectado."
        )

        await self.enviar(
            jugador_id,
            {
                "tipo": "conexion",

                "id": jugador_id,

                "mensaje":
                    "Conectado al servidor.",

                "escaleras":
                    ESCALERAS,

                "serpientes":
                    SERPIENTES
            }
        )

        await self.enviar_estado()

        try:

            async for mensaje in websocket:

                try:

                    datos = json.loads(mensaje)

                    await self.procesar_mensaje(
                        jugador_id,
                        datos
                    )

                except json.JSONDecodeError:

                    await self.enviar(
                        jugador_id,
                        {
                            "tipo": "error",
                            "mensaje":
                                "Mensaje inválido."
                        }
                    )

        except websockets.exceptions.ConnectionClosed:

            pass

        except Exception as error:

            print(
                f"[ERROR JUGADOR {jugador_id}] "
                f"{type(error).__name__}: {error}"
            )

        finally:

            await self.desconectar(jugador_id)


    # ========================================================
    # COLORES
    # ========================================================

    def obtener_color(self, jugador_id):

        colores = [

            (255, 80, 80),

            (80, 120, 255),

            (80, 200, 100),

            (220, 100, 220)
        ]

        return colores[
            (jugador_id - 1) %
            len(colores)
        ]


    # ========================================================
    # ENVIAR
    # ========================================================

    async def enviar(
        self,
        jugador_id,
        datos
    ):

        async with self.lock:

            websocket = self.clientes.get(
                jugador_id
            )

        if websocket is None:

            return False

        try:

            await websocket.send(
                json.dumps(
                    datos,
                    ensure_ascii=False
                )
            )

            return True

        except Exception:

            return False


    # ========================================================
    # BROADCAST
    # ========================================================

    async def broadcast(
        self,
        datos
    ):

        async with self.lock:

            conexiones = list(
                self.clientes.items()
            )

        mensaje = json.dumps(
            datos,
            ensure_ascii=False
        )

        desconectados = []

        for jugador_id, websocket in conexiones:

            try:

                await websocket.send(
                    mensaje
                )

            except Exception:

                desconectados.append(
                    jugador_id
                )

        for jugador_id in desconectados:

            await self.desconectar(
                jugador_id
            )


    # ========================================================
    # PROCESAR MENSAJE
    # ========================================================

    async def procesar_mensaje(
        self,
        jugador_id,
        datos
    ):

        tipo = datos.get("tipo")

        # ----------------------------------------------------
        # DADO
        # ----------------------------------------------------

        if tipo in (
            "dado",
            "lanzar_dado"
        ):

            await self.lanzar_dado(
                jugador_id
            )

        # ----------------------------------------------------
        # REINICIAR
        # ----------------------------------------------------

        elif tipo == "reiniciar":

            await self.reiniciar_partida()

        # ----------------------------------------------------
        # ABANDONAR
        # ----------------------------------------------------

        elif tipo == "abandonar":

            print(
                f"[JUGADOR {jugador_id}] "
                "Abandonó la partida."
            )

            await self.desconectar(
                jugador_id
            )

        # ----------------------------------------------------
        # NOMBRE
        # ----------------------------------------------------

        elif tipo == "nombre":

            nombre = datos.get(
                "nombre",
                f"Jugador {jugador_id}"
            )

            async with self.lock:

                if jugador_id in self.jugadores:

                    self.jugadores[
                        jugador_id
                    ]["nombre"] = str(
                        nombre
                    )[:20]

            await self.enviar_estado()

        # ----------------------------------------------------
        # PING
        # ----------------------------------------------------

        elif tipo == "ping":

            await self.enviar(
                jugador_id,
                {
                    "tipo": "pong"
                }
            )


    # ========================================================
    # LANZAR DADO
    # ========================================================

    async def lanzar_dado(
        self,
        jugador_id
    ):

        async with self.lock:

            if jugador_id not in self.jugadores:

                return

            if self.ganador is not None:

                mensaje_error = {
                    "tipo": "mensaje",
                    "mensaje":
                        "La partida ya terminó."
                }

                websocket = self.clientes.get(
                    jugador_id
                )

                if websocket:

                    await websocket.send(
                        json.dumps(
                            mensaje_error
                        )
                    )

                return

            if self.turno_actual != jugador_id:

                websocket = self.clientes.get(
                    jugador_id
                )

                if websocket:

                    await websocket.send(
                        json.dumps({
                            "tipo": "mensaje",
                            "mensaje":
                                "No es tu turno."
                        })
                    )

                return

            jugador = self.jugadores[
                jugador_id
            ]

            casilla_actual = jugador[
                "casilla"
            ]

            dado = random.randint(
                1,
                6
            )

            self.ultimo_dado = dado

            nueva_casilla = (
                casilla_actual +
                dado
            )

            # ------------------------------------------------
            # NO PASAR DE 100
            # ------------------------------------------------

            if nueva_casilla > 100:

                nueva_casilla = casilla_actual

                self.ultimo_evento = (
                    f"Jugador {jugador_id} "
                    f"sacó {dado}. "
                    f"No puede avanzar."
                )

            else:

                jugador["casilla"] = (
                    nueva_casilla
                )

                self.ultimo_evento = (
                    f"Jugador {jugador_id} "
                    f"sacó {dado}."
                )

                # --------------------------------------------
                # ESCALERA
                # --------------------------------------------

                if nueva_casilla in ESCALERAS:

                    destino = ESCALERAS[
                        nueva_casilla
                    ]

                    jugador["casilla"] = destino

                    self.ultimo_evento = (
                        f"Jugador {jugador_id} "
                        f"subió por una escalera: "
                        f"{nueva_casilla} → {destino}"
                    )

                # --------------------------------------------
                # SERPIENTE
                # --------------------------------------------

                elif nueva_casilla in SERPIENTES:

                    destino = SERPIENTES[
                        nueva_casilla
                    ]

                    jugador["casilla"] = destino

                    self.ultimo_evento = (
                        f"Jugador {jugador_id} "
                        f"cayó por una serpiente: "
                        f"{nueva_casilla} → {destino}"
                    )

            # ------------------------------------------------
            # GANADOR
            # ------------------------------------------------

            if jugador["casilla"] == 100:

                self.ganador = jugador_id

                self.ultimo_evento = (
                    f"Jugador {jugador_id} "
                    f"ha ganado!"
                )

            else:

                ids = sorted(
                    self.jugadores.keys()
                )

                if ids:

                    try:

                        posicion = ids.index(
                            jugador_id
                        )

                        siguiente = (
                            posicion + 1
                        ) % len(ids)

                        self.turno_actual = (
                            ids[siguiente]
                        )

                    except ValueError:

                        self.turno_actual = ids[0]

        # ----------------------------------------------------
        # RESULTADO
        # ----------------------------------------------------

        await self.broadcast(
            {
                "tipo":
                    "resultado_dado",

                "jugador":
                    jugador_id,

                "dado":
                    dado,

                "ok":
                    True,

                "mensaje":
                    self.ultimo_evento
            }
        )

        await self.enviar_estado()


    # ========================================================
    # REINICIAR
    # ========================================================

    async def reiniciar_partida(
        self
    ):

        async with self.lock:

            for jugador in (
                self.jugadores.values()
            ):

                jugador["casilla"] = 1

            self.ganador = None

            self.ultimo_dado = 0

            self.ultimo_evento = (
                "Partida reiniciada."
            )

            ids = sorted(
                self.jugadores.keys()
            )

            if ids:

                self.turno_actual = ids[0]

            else:

                self.turno_actual = None

        print(
            "[PARTIDA] Partida reiniciada."
        )

        await self.broadcast(
            {
                "tipo":
                    "reinicio"
            }
        )

        await self.enviar_estado()


    # ========================================================
    # ESTADO
    # ========================================================

    async def enviar_estado(
        self
    ):

        async with self.lock:

            jugadores = {}

            for jugador_id, jugador in (
                self.jugadores.items()
            ):

                jugadores[
                    str(jugador_id)
                ] = {

                    "id":
                        jugador["id"],

                    "nombre":
                        jugador["nombre"],

                    "casilla":
                        jugador["casilla"],

                    "posicion":
                        jugador["casilla"],

                    "color":
                        jugador["color"]
                }

            estado = {

                "tipo":
                    "estado",

                "jugadores":
                    jugadores,

                "turno":
                    self.turno_actual,

                "ganador":
                    self.ganador,

                "dado":
                    self.ultimo_dado,

                "ultimo_dado":
                    self.ultimo_dado,

                "ultimo_evento":
                    self.ultimo_evento,

                "escaleras":
                    ESCALERAS,

                "serpientes":
                    SERPIENTES
            }

        await self.broadcast(
            estado
        )


    # ========================================================
    # DESCONECTAR
    # ========================================================

    async def desconectar(
        self,
        jugador_id
    ):

        async with self.lock:

            websocket = self.clientes.pop(
                jugador_id,
                None
            )

            jugador = self.jugadores.pop(
                jugador_id,
                None
            )

            if jugador is None:

                return

            ids = sorted(
                self.jugadores.keys()
            )

            era_turno = (
                self.turno_actual ==
                jugador_id
            )

            if era_turno:

                if ids:

                    self.turno_actual = ids[0]

                else:

                    self.turno_actual = None

        print(
            f"[JUGADOR {jugador_id}] "
            "Desconectado."
        )

        if websocket:

            try:

                await websocket.close()

            except Exception:

                pass

        if ids:

            self.ultimo_evento = (
                f"Jugador {jugador_id} "
                "se desconectó."
            )

        else:

            self.ultimo_evento = (
                "Esperando jugadores..."
            )

        await self.enviar_estado()


# ============================================================
# CREAR SERVIDOR
# ============================================================

juego = ServidorJuego()


async def main():

    print(
        f"[SERVIDOR] Escuchando en "
        f"0.0.0.0:{PORT}"
    )

    async with websockets.serve(
        juego.conectar,
        HOST,
        PORT,
        ping_interval=20,
        ping_timeout=20
    ):

        print(
            "[SERVIDOR] Servidor online."
        )

        print(
            "Presiona CTRL+C para detener."
        )

        print()

        await asyncio.Future()


# ============================================================
# EJECUTAR
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        print()
        print(
            "[SERVIDOR] Servidor detenido."
        )