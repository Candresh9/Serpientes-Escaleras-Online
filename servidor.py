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
