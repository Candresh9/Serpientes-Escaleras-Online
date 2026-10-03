import os
import asyncio
import json
import random
import websockets


# ============================================================
# CONFIGURACIÓN
# ============================================================

HOST = "0.0.0.0"

# Render proporciona automáticamente PORT
PORT = int(os.environ.get("PORT", 10000))

MAX_JUGADORES = 4


# ============================================================
# ESCALERAS Y SERPIENTES
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
# JUEGO
# ============================================================

class Juego:

    def __init__(self):

        self.jugadores = {}

        self.conexiones = {}

        self.turno = None

        self.ganador = None

        self.ultimo_dado = 0

        self.ultimo_evento = "Esperando jugadores..."

        self.siguiente_id = 1

        self.lock = asyncio.Lock()


    # ========================================================
    # GENERAR ID
    # ========================================================

    def generar_id(self):

        while True:

            jugador_id = str(self.siguiente_id)

            self.siguiente_id += 1

            if jugador_id not in self.jugadores:
                return jugador_id


    # ========================================================
    # ESTADO
    # ========================================================

    def obtener_estado(self):

        jugadores = {}

        for jugador_id, jugador in self.jugadores.items():

            jugadores[jugador_id] = {
                "id": jugador["id"],
                "nombre": jugador["nombre"],
                "posicion": jugador["posicion"]
            }

        return {
            "tipo": "estado",

            "jugadores": jugadores,

            "turno": self.turno,

            "ganador": self.ganador,

            "dado": self.ultimo_dado,

            "ultimo_dado": self.ultimo_dado,

            "ultimo_evento": self.ultimo_evento,

            "escaleras": ESCALERAS,

            "serpientes": SERPIENTES
        }


    # ========================================================
    # ENVIAR A TODOS
    # ========================================================

    async def enviar_todos(self, mensaje):

        datos = json.dumps(mensaje)

        conexiones = list(self.conexiones.items())

        for jugador_id, websocket in conexiones:

            try:

                await websocket.send(datos)

            except Exception:

                pass


    # ========================================================
    # ENVIAR ESTADO
    # ========================================================

    async def enviar_estado(self):

        await self.enviar_todos(
            self.obtener_estado()
        )


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
                            "mensaje": "La partida está llena."
                        })
                    )

                    await websocket.close()

                    return


                jugador_id = self.generar_id()

                self.jugadores[jugador_id] = {

                    "id": jugador_id,

                    "nombre": f"Jugador {jugador_id}",

                    "posicion": 0
                }

                self.conexiones[jugador_id] = websocket

                if self.turno is None:

                    self.turno = jugador_id

                self.ultimo_evento = (
                    f"Jugador {jugador_id} se ha conectado."
                )


            # ----------------------------------------------
            # INFORMACIÓN DEL JUGADOR
            # ----------------------------------------------

            await websocket.send(
                json.dumps({
                    "tipo": "conexion",
                    "id": jugador_id,
                    "nombre": self.jugadores[jugador_id]["nombre"]
                })
            )


            # ----------------------------------------------
            # ESTADO INICIAL
            # ----------------------------------------------

            await self.enviar_estado()


            # ----------------------------------------------
            # RECIBIR MENSAJES
            # ----------------------------------------------

            async for mensaje in websocket:

                try:

                    datos = json.loads(mensaje)

                    await self.procesar_mensaje(
                        jugador_id,
                        datos
                    )

                except json.JSONDecodeError:

                    await websocket.send(
                        json.dumps({
                            "tipo": "error",
                            "mensaje": "Mensaje inválido."
                        })
                    )


        except websockets.exceptions.ConnectionClosed:

            pass

        except Exception as e:

            print(
                f"Error con jugador {jugador_id}: {e}"
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
        # CAMBIAR NOMBRE
        # ====================================================

        if tipo == "nombre":

            nombre = datos.get(
                "nombre",
                f"Jugador {jugador_id}"
            )

            nombre = str(nombre).strip()

            if not nombre:

                nombre = f"Jugador {jugador_id}"

            nombre = nombre[:20]

            if jugador_id in self.jugadores:

                self.jugadores[jugador_id]["nombre"] = nombre

                self.ultimo_evento = (
                    f"{nombre} se ha unido a la partida."
                )

                await self.enviar_estado()

            return


        # ====================================================
        # PING
        # ====================================================

        if tipo == "ping":

            websocket = self.conexiones.get(jugador_id)

            if websocket:

                await websocket.send(
                    json.dumps({
                        "tipo": "pong"
                    })
                )

            return


        # ====================================================
        # ABANDONAR
        # ====================================================

        if tipo == "abandonar":

            await self.desconectar(
                jugador_id,
                cerrar=False
            )

            return


        # ====================================================
        # REINICIAR
        # ====================================================

        if tipo == "reiniciar":

            if self.ganador is not None:

                self.reiniciar()

                await self.enviar_todos({
                    "tipo": "reinicio",
                    "mensaje": "La partida ha sido reiniciada."
                })

                await self.enviar_estado()

            return


        # ====================================================
        # LANZAR DADO
        # ====================================================

        if tipo == "lanzar_dado":

            await self.lanzar_dado(
                jugador_id
            )

            return


        # ====================================================
        # COMPATIBILIDAD
        # ====================================================

        if tipo == "dado":

            await self.lanzar_dado(
                jugador_id
            )

            return


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


            # ----------------------------------------------
            # PARTIDA TERMINADA
            # ----------------------------------------------

            if self.ganador is not None:

                await self.enviar_error(
                    jugador_id,
                    "La partida ya terminó."
                )

                return


            # ----------------------------------------------
            # TURNO
            # ----------------------------------------------

            if self.turno != jugador_id:

                await self.enviar_error(
                    jugador_id,
                    "No es tu turno."
                )

                return


            # ----------------------------------------------
            # DADO
            # ----------------------------------------------

            dado = random.randint(1, 6)

            self.ultimo_dado = dado


            jugador = self.jugadores[jugador_id]

            posicion_anterior = jugador["posicion"]

            nueva_posicion = posicion_anterior + dado


            # ----------------------------------------------
            # NO PASAR DE 100
            # ----------------------------------------------

            if nueva_posicion > 100:

                nueva_posicion = posicion_anterior


            jugador["posicion"] = nueva_posicion


            # ----------------------------------------------
            # ESCALERA
            # ----------------------------------------------

            if nueva_posicion in ESCALERAS:

                destino = ESCALERAS[nueva_posicion]

                jugador["posicion"] = destino

                self.ultimo_evento = (
                    f"{jugador['nombre']} sacó {dado}, "
                    f"subió por una escalera "
                    f"hasta {destino}."
                )


            # ----------------------------------------------
            # SERPIENTE
            # ----------------------------------------------

            elif nueva_posicion in SERPIENTES:

                destino = SERPIENTES[nueva_posicion]

                jugador["posicion"] = destino

                self.ultimo_evento = (
                    f"{jugador['nombre']} sacó {dado}, "
                    f"cayó por una serpiente "
                    f"hasta {destino}."
                )


            else:

                self.ultimo_evento = (
                    f"{jugador['nombre']} sacó {dado}."
                )


            # ----------------------------------------------
            # GANADOR
            # ----------------------------------------------

            if jugador["posicion"] >= 100:

                jugador["posicion"] = 100

                self.ganador = jugador_id

                self.ultimo_evento = (
                    f"🎉 {jugador['nombre']} ha ganado!"
                )


            # ----------------------------------------------
            # SIGUIENTE TURNO
            # ----------------------------------------------

            else:

                self.siguiente_turno()


        # ====================================================
        # RESULTADO DEL DADO
        # ====================================================

        await self.enviar_todos({

            "tipo": "resultado_dado",

            "jugador": jugador_id,

            "dado": dado
        })


        # ====================================================
        # ESTADO
        # ====================================================

        await self.enviar_estado()


    # ========================================================
    # SIGUIENTE TURNO
    # ========================================================

    def siguiente_turno(self):

        ids = list(self.jugadores.keys())

        if not ids:

            self.turno = None

            return


        try:

            indice = ids.index(
                self.turno
            )

        except ValueError:

            indice = -1


        siguiente = (
            indice + 1
        ) % len(ids)


        self.turno = ids[siguiente]


    # ========================================================
    # REINICIAR
    # ========================================================

    def reiniciar(self):

        for jugador in self.jugadores.values():

            jugador["posicion"] = 0

        self.ganador = None

        self.ultimo_dado = 0

        self.ultimo_evento = (
            "Nueva partida iniciada."
        )


        ids = list(
            self.jugadores.keys()
        )

        if ids:

            self.turno = ids[0]

        else:

            self.turno = None


    # ========================================================
    # ERROR
    # ========================================================

    async def enviar_error(
        self,
        jugador_id,
        mensaje
    ):

        websocket = self.conexiones.get(
            jugador_id
        )

        if websocket:

            try:

                await websocket.send(
                    json.dumps({
                        "tipo": "error",
                        "mensaje": mensaje
                    })
                )

            except Exception:

                pass


    # ========================================================
    # DESCONECTAR
    # ========================================================

    async def desconectar(
        self,
        jugador_id,
        cerrar=True
    ):

        if jugador_id is None:

            return


        websocket = self.conexiones.pop(
            jugador_id,
            None
        )

        jugador = self.jugadores.pop(
            jugador_id,
            None
        )


        if jugador is None:

            return


        nombre = jugador["nombre"]


        if websocket and cerrar:

            try:

                await websocket.close()

            except Exception:

                pass


        # ----------------------------------------------
        # CAMBIAR TURNO
        # ----------------------------------------------

        if self.turno == jugador_id:

            ids = list(
                self.jugadores.keys()
            )

            if ids:

                self.turno = ids[0]

            else:

                self.turno = None


        self.ultimo_evento = (
            f"{nombre} abandonó la partida."
        )


        await self.enviar_estado()


# ============================================================
# SERVIDOR
# ============================================================

juego = Juego()


async def main():

    print(
        "=========================================="
    )

    print(
        " SERPIENTES Y ESCALERAS - SERVIDOR"
    )

    print(
        "=========================================="
    )

    print(
        f"Escuchando en {HOST}:{PORT}"
    )


    async with websockets.serve(

        juego.conectar,

        HOST,

        PORT,

        ping_interval=20,

        ping_timeout=20

    ):

        print(
            "Servidor WebSocket iniciado."
        )

        print(
            "Esperando jugadores..."
        )

        await asyncio.Future()


# ============================================================
# INICIAR
# ============================================================

if __name__ == "__main__":

    asyncio.run(main())
