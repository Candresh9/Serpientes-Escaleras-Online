import pygame
import socket
import threading
import json
import math


# ============================================================
# CONFIGURACIÓN
# ============================================================

SERVER_IP = "192.168.18.43"
PORT = 5555

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

        self.turno = None

        self.ganador = None

        self.escaleras = {}

        self.serpientes = {}

        self.ultimo_dado = None

        self.ultimo_evento = (
            "Conectando al servidor..."
        )

        self.conectado = False

        self.ejecutando = True

        self.confirmar_abandono = False

        self.lock = threading.RLock()


# ============================================================
# CLIENTE DE RED
# ============================================================

class ClienteRed:

    def __init__(
        self,
        estado
    ):

        self.estado = estado

        self.sock = None

        self.hilo = None

        self.lock_envio = threading.Lock()

        self.cerrando = False


    # ========================================================
    # CONECTAR
    # ========================================================

    def conectar(self):

        try:

            sock = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM
            )

            sock.settimeout(5)

            sock.connect(
                (
                    SERVER_IP,
                    PORT
                )
            )

            sock.settimeout(None)

            self.sock = sock

            self.cerrando = False

            with self.estado.lock:

                self.estado.conectado = True

                self.estado.ultimo_evento = (
                    f"Conectado a "
                    f"{SERVER_IP}:{PORT}"
                )

            self.hilo = threading.Thread(
                target=self.recibir,
                daemon=True
            )

            self.hilo.start()

            return True

        except socket.timeout:

            self.error(
                "Tiempo de espera agotado. "
                "No se encontró el servidor."
            )

        except ConnectionRefusedError:

            self.error(
                "Conexión rechazada. "
                "Inicia primero servidor.py."
            )

        except OSError as error:

            self.error(
                f"No se pudo conectar: {error}"
            )

        return False


    # ========================================================
    # ERROR
    # ========================================================

    def error(
        self,
        mensaje
    ):

        with self.estado.lock:

            self.estado.conectado = False

            self.estado.ultimo_evento = mensaje


    # ========================================================
    # ENVIAR
    # ========================================================

    def enviar(
        self,
        mensaje
    ):

        if self.sock is None:
            return False

        try:

            datos = (
                json.dumps(
                    mensaje,
                    ensure_ascii=False
                )
                + "\n"
            )

            with self.lock_envio:

                self.sock.sendall(
                    datos.encode("utf-8")
                )

            return True

        except OSError:

            with self.estado.lock:

                self.estado.conectado = False

                if not self.cerrando:

                    self.estado.ultimo_evento = (
                        "Se perdió la conexión."
                    )

            return False


    # ========================================================
    # RECIBIR
    # ========================================================

    def recibir(self):

        buffer = ""

        try:

            while not self.cerrando:

                if self.sock is None:
                    break

                datos = self.sock.recv(
                    4096
                )

                if not datos:
                    break

                buffer += datos.decode(
                    "utf-8",
                    errors="replace"
                )

                while "\n" in buffer:

                    linea, buffer = (
                        buffer.split(
                            "\n",
                            1
                        )
                    )

                    if not linea.strip():
                        continue

                    try:

                        mensaje = json.loads(
                            linea
                        )

                    except json.JSONDecodeError:

                        continue

                    self.procesar_mensaje(
                        mensaje
                    )

        except OSError:

            pass

        finally:

            with self.estado.lock:

                self.estado.conectado = False

                if (
                    self.estado.ejecutando
                    and not self.cerrando
                ):

                    self.estado.ultimo_evento = (
                        "El servidor se desconectó."
                    )

                    self.estado.ejecutando = False


    # ========================================================
    # PROCESAR MENSAJE
    # ========================================================

    def procesar_mensaje(
        self,
        mensaje
    ):

        tipo = mensaje.get(
            "tipo"
        )

        # ----------------------------------------------------
        # ID
        # ----------------------------------------------------

        if tipo in (
            "asignar_id",
            "conexion"
        ):

            with self.estado.lock:

                self.estado.mi_id = (
                    mensaje.get("id")
                )

                self.estado.escaleras = (
                    mensaje.get(
                        "escaleras",
                        self.estado.escaleras
                    )
                )

                self.estado.serpientes = (
                    mensaje.get(
                        "serpientes",
                        self.estado.serpientes
                    )
                )

                self.estado.ultimo_evento = (
                    f"Conectado como "
                    f"Jugador "
                    f"{self.estado.mi_id}."
                )

            return


        # ----------------------------------------------------
        # ESTADO
        # ----------------------------------------------------

        elif tipo == "estado":

            with self.estado.lock:

                self.estado.jugadores = (
                    mensaje.get(
                        "jugadores",
                        {}
                    )
                )

                self.estado.turno = (
                    mensaje.get(
                        "turno"
                    )
                )

                self.estado.ganador = (
                    mensaje.get(
                        "ganador"
                    )
                )

                if "dado" in mensaje:

                    self.estado.ultimo_dado = (
                        mensaje.get(
                            "dado"
                        )
                    )

                else:

                    self.estado.ultimo_dado = (
                        mensaje.get(
                            "ultimo_dado"
                        )
                    )

                self.estado.ultimo_evento = (
                    mensaje.get(
                        "ultimo_evento",
                        self.estado.ultimo_evento
                    )
                )

                self.estado.escaleras = (
                    mensaje.get(
                        "escaleras",
                        self.estado.escaleras
                    )
                )

                self.estado.serpientes = (
                    mensaje.get(
                        "serpientes",
                        self.estado.serpientes
                    )
                )

            return


        # ----------------------------------------------------
        # RESULTADO DADO
        # ----------------------------------------------------

        elif tipo == "resultado_dado":

            with self.estado.lock:

                if mensaje.get("ok"):

                    self.estado.ultimo_dado = (
                        mensaje.get(
                            "dado"
                        )
                    )

                self.estado.ultimo_evento = (
                    mensaje.get(
                        "mensaje",
                        "Resultado recibido."
                    )
                )

            return


        # ----------------------------------------------------
        # MENSAJE
        # ----------------------------------------------------

        elif tipo == "mensaje":

            with self.estado.lock:

                self.estado.ultimo_evento = (
                    mensaje.get(
                        "mensaje",
                        ""
                    )
                )

            return


        # ----------------------------------------------------
        # REINICIO
        # ----------------------------------------------------

        elif tipo == "reinicio":

            with self.estado.lock:

                self.estado.ultimo_dado = 0

                self.estado.ganador = None

                self.estado.ultimo_evento = (
                    "Partida reiniciada."
                )

            return


        # ----------------------------------------------------
        # SERVIDOR CERRADO
        # ----------------------------------------------------

        elif tipo in (
            "lleno",
            "servidor_cerrando",
            "servidor_cerrado"
        ):

            with self.estado.lock:

                self.estado.ultimo_evento = (
                    mensaje.get(
                        "mensaje",
                        "El servidor se cerró."
                    )
                )

                self.estado.conectado = False

                self.estado.ejecutando = False

            return


    # ========================================================
    # CERRAR
    # ========================================================

    def cerrar(self):

        self.cerrando = True

        with self.estado.lock:

            self.estado.conectado = False

        sock = self.sock

        self.sock = None

        if sock:

            try:

                sock.shutdown(
                    socket.SHUT_RDWR
                )

            except OSError:

                pass

            try:

                sock.close()

            except OSError:

                pass


# ============================================================
# COORDENADAS
# ============================================================

def coordenadas_casilla(
    casilla
):

    try:

        casilla = int(
            casilla
        )

    except (
        ValueError,
        TypeError
    ):

        return None

    if not 1 <= casilla <= 100:

        return None

    indice = casilla - 1

    fila = indice // 10

    columna = indice % 10

    if fila % 2 == 1:

        columna = 9 - columna

    x = (
        MARGEN
        + columna * CELDA
    )

    y = (
        MARGEN
        + (9 - fila) * CELDA
    )

    return x, y


def centro_casilla(
    casilla
):

    posicion = coordenadas_casilla(
        casilla
    )

    if posicion is None:
        return None

    x, y = posicion

    return (
        x + CELDA // 2,
        y + CELDA // 2
    )


# ============================================================
# RENDERIZADOR
# ============================================================

class RenderizadorJuego:

    def __init__(
        self,
        pantalla,
        estado
    ):

        self.pantalla = pantalla

        self.estado = estado

        self.fuente_numero = pygame.font.SysFont(
            "Arial",
            16,
            bold=True
        )

        self.fuente_titulo = pygame.font.SysFont(
            "Arial",
            23,
            bold=True
        )

        self.fuente = pygame.font.SysFont(
            "Arial",
            17
        )

        self.fuente_negrita = pygame.font.SysFont(
            "Arial",
            18,
            bold=True
        )

        self.fuente_boton = pygame.font.SysFont(
            "Arial",
            18,
            bold=True
        )

        self.fuente_grande = pygame.font.SysFont(
            "Arial",
            26,
            bold=True
        )

        # ----------------------------------------------------
        # BOTONES
        # ----------------------------------------------------

        self.boton_dado = pygame.Rect(
            PANEL_X + 20,
            570,
            PANEL_ANCHO - 40,
            50
        )

        self.boton_abandonar = pygame.Rect(
            PANEL_X + 20,
            635,
            PANEL_ANCHO - 40,
            45
        )


    # ========================================================
    # TABLERO
    # ========================================================

    def dibujar_tablero(self):

        for fila in range(10):

            for columna in range(10):

                x = (
                    MARGEN
                    + columna * CELDA
                )

                y = (
                    MARGEN
                    + fila * CELDA
                )

                numero_fila = 9 - fila

                if numero_fila % 2 == 0:

                    numero = (
                        numero_fila * 10
                        + columna
                        + 1
                    )

                else:

                    numero = (
                        numero_fila * 10
                        + (9 - columna)
                        + 1
                    )

                color = (
                    (245, 245, 245)
                    if (
                        (fila + columna)
                        % 2 == 0
                    )
                    else
                    (225, 235, 245)
                )

                pygame.draw.rect(
                    self.pantalla,
                    color,
                    (
                        x,
                        y,
                        CELDA,
                        CELDA
                    )
                )

                pygame.draw.rect(
                    self.pantalla,
                    GRIS_OSCURO,
                    (
                        x,
                        y,
                        CELDA,
                        CELDA
                    ),
                    1
                )

                texto = (
                    self.fuente_numero.render(
                        str(numero),
                        True,
                        NEGRO
                    )
                )

                self.pantalla.blit(
                    texto,
                    (
                        x + 5,
                        y + 5
                    )
                )


    # ========================================================
    # ESCALERAS
    # ========================================================

    def dibujar_escaleras(self):

        with self.estado.lock:

            escaleras = dict(
                self.estado.escaleras
            )

        for inicio, fin in escaleras.items():

            p1 = centro_casilla(
                inicio
            )

            p2 = centro_casilla(
                fin
            )

            if not p1 or not p2:
                continue

            x1, y1 = p1

            x2, y2 = p2

            dx = x2 - x1

            dy = y2 - y1

            distancia = max(
                1,
                math.hypot(
                    dx,
                    dy
                )
            )

            ox = (
                -dy
                / distancia
                * 6
            )

            oy = (
                dx
                / distancia
                * 6
            )

            pygame.draw.line(
                self.pantalla,
                VERDE_ESCALERA,
                (
                    int(x1 + ox),
                    int(y1 + oy)
                ),
                (
                    int(x2 + ox),
                    int(y2 + oy)
                ),
                5
            )

            pygame.draw.line(
                self.pantalla,
                VERDE_ESCALERA,
                (
                    int(x1 - ox),
                    int(y1 - oy)
                ),
                (
                    int(x2 - ox),
                    int(y2 - oy)
                ),
                5
            )

            for i in range(1, 8):

                t = i / 8

                x = (
                    x1
                    + dx * t
                )

                y = (
                    y1
                    + dy * t
                )

                pygame.draw.line(
                    self.pantalla,
                    VERDE_ESCALERA,
                    (
                        int(x - ox),
                        int(y - oy)
                    ),
                    (
                        int(x + ox),
                        int(y + oy)
                    ),
                    3
                )


    # ========================================================
    # SERPIENTES
    # ========================================================

    def dibujar_serpientes(self):

        with self.estado.lock:

            serpientes = dict(
                self.estado.serpientes
            )

        for inicio, fin in serpientes.items():

            p1 = centro_casilla(
                inicio
            )

            p2 = centro_casilla(
                fin
            )

            if not p1 or not p2:
                continue

            x1, y1 = p1

            x2, y2 = p2

            dx = x2 - x1

            dy = y2 - y1

            longitud = max(
                1,
                math.hypot(
                    dx,
                    dy
                )
            )

            ox = -dy / longitud

            oy = dx / longitud

            puntos = []

            for i in range(31):

                t = i / 30

                curva = (
                    math.sin(
                        t * math.pi * 4
                    )
                    * 12
                )

                x = (
                    x1
                    + dx * t
                    + ox * curva
                )

                y = (
                    y1
                    + dy * t
                    + oy * curva
                )

                puntos.append(
                    (
                        int(x),
                        int(y)
                    )
                )

            pygame.draw.lines(
                self.pantalla,
                (120, 20, 20),
                False,
                puntos,
                12
            )

            pygame.draw.lines(
                self.pantalla,
                ROJO_SERPIENTE,
                False,
                puntos,
                8
            )

            pygame.draw.circle(
                self.pantalla,
                ROJO_SERPIENTE,
                (
                    int(x1),
                    int(y1)
                ),
                12
            )

            pygame.draw.circle(
                self.pantalla,
                NEGRO,
                (
                    int(x1),
                    int(y1)
                ),
                12,
                2
            )


    # ========================================================
    # JUGADORES
    # ========================================================

    def dibujar_jugadores(self):

        with self.estado.lock:

            jugadores = dict(
                self.estado.jugadores
            )

        posiciones = {}

        for jugador_id, jugador in (
            jugadores.items()
        ):

            casilla = jugador.get(
                "posicion",
                jugador.get(
                    "casilla",
                    1
                )
            )

            posiciones.setdefault(
                str(casilla),
                []
            ).append(
                (
                    jugador_id,
                    jugador
                )
            )

        offsets = [
            (-12, -12),
            (12, -12),
            (-12, 12),
            (12, 12)
        ]

        for casilla, lista in posiciones.items():

            centro = centro_casilla(
                casilla
            )

            if not centro:
                continue

            for indice, (
                jugador_id,
                jugador
            ) in enumerate(lista):

                x, y = centro

                ox, oy = offsets[
                    indice % len(offsets)
                ]

                x += ox
                y += oy

                try:

                    color = tuple(
                        int(c)
                        for c in jugador[
                            "color"
                        ]
                    )

                except (
                    KeyError,
                    TypeError,
                    ValueError
                ):

                    colores = [
                        (60, 120, 255),
                        (240, 70, 70),
                        (70, 190, 90),
                        (240, 180, 50)
                    ]

                    try:

                        color = colores[
                            int(jugador_id)
                            % len(colores)
                        ]

                    except (
                        ValueError,
                        TypeError
                    ):

                        color = GRIS_OSCURO

                pygame.draw.circle(
                    self.pantalla,
                    color,
                    (
                        x,
                        y
                    ),
                    17
                )

                pygame.draw.circle(
                    self.pantalla,
                    NEGRO,
                    (
                        x,
                        y
                    ),
                    17,
                    2
                )

                texto = (
                    self.fuente_numero.render(
                        str(jugador_id),
                        True,
                        BLANCO
                    )
                )

                self.pantalla.blit(
                    texto,
                    texto.get_rect(
                        center=(
                            x,
                            y
                        )
                    )
                )


    # ========================================================
    # BOTÓN
    # ========================================================

    def dibujar_boton(
        self,
        rect,
        texto,
        color,
        habilitado=True
    ):

        color_actual = (
            color
            if habilitado
            else
            (150, 150, 150)
        )

        pygame.draw.rect(
            self.pantalla,
            color_actual,
            rect,
            border_radius=8
        )

        pygame.draw.rect(
            self.pantalla,
            NEGRO,
            rect,
            2,
            border_radius=8
        )

        superficie = (
            self.fuente_boton.render(
                texto,
                True,
                BLANCO
            )
        )

        self.pantalla.blit(
            superficie,
            superficie.get_rect(
                center=rect.center
            )
        )


    # ========================================================
    # PANEL
    # ========================================================

    def dibujar_panel(self):

        pygame.draw.rect(
            self.pantalla,
            BLANCO,
            (
                PANEL_X,
                35,
                PANEL_ANCHO,
                680
            ),
            border_radius=10
        )

        pygame.draw.rect(
            self.pantalla,
            GRIS_OSCURO,
            (
                PANEL_X,
                35,
                PANEL_ANCHO,
                680
            ),
            2,
            border_radius=10
        )

        with self.estado.lock:

            mi_id = self.estado.mi_id

            turno = self.estado.turno

            ganador = self.estado.ganador

            dado = self.estado.ultimo_dado

            evento = self.estado.ultimo_evento

            conectado = self.estado.conectado

            confirmar = (
                self.estado.confirmar_abandono
            )

            jugadores = dict(
                self.estado.jugadores
            )

        def texto(
            mensaje,
            x,
            y,
            color=NEGRO,
            fuente=None
        ):

            fuente = (
                fuente
                or self.fuente
            )

            self.pantalla.blit(
                fuente.render(
                    str(mensaje),
                    True,
                    color
                ),
                (
                    x,
                    y
                )
            )

        texto(
            "ESCALERAS Y SERPIENTES",
            PANEL_X + 15,
            55,
            NEGRO,
            self.fuente_titulo
        )

        texto(
            f"Tu jugador: "
            f"{mi_id if mi_id is not None else '-'}",
            PANEL_X + 20,
            105
        )

        texto(
            f"Jugadores: {len(jugadores)}/4",
            PANEL_X + 20,
            132
        )

        texto(
            "Estado: "
            + (
                "CONECTADO"
                if conectado
                else "DESCONECTADO"
            ),
            PANEL_X + 20,
            159,
            VERDE
            if conectado
            else ROJO
        )

        if ganador is not None:

            texto(
                f"¡GANÓ JUGADOR {ganador}!",
                PANEL_X + 20,
                205,
                VERDE,
                self.fuente_negrita
            )

        elif (
            turno is not None
            and str(mi_id) == str(turno)
        ):

            texto(
                "¡ES TU TURNO!",
                PANEL_X + 20,
                205,
                VERDE,
                self.fuente_negrita
            )

        else:

            texto(
                f"Turno: "
                f"{turno if turno is not None else '-'}",
                PANEL_X + 20,
                205
            )

        texto(
            f"Último dado: "
            f"{dado if dado is not None else '-'}",
            PANEL_X + 20,
            240
        )

        pygame.draw.line(
            self.pantalla,
            GRIS,
            (
                PANEL_X + 15,
                285
            ),
            (
                PANEL_X + PANEL_ANCHO - 15,
                285
            ),
            2
        )

        texto(
            "Jugadores conectados",
            PANEL_X + 20,
            305,
            NEGRO,
            self.fuente_negrita
        )

        y = 340

        for jugador_id in sorted(
            jugadores,
            key=lambda x: int(x)
        ):

            jugador = jugadores[
                jugador_id
            ]

            color = tuple(
                jugador.get(
                    "color",
                    [100, 100, 100]
                )
            )

            pygame.draw.circle(
                self.pantalla,
                color,
                (
                    PANEL_X + 32,
                    y + 10
                ),
                9
            )

            posicion = jugador.get(
                "posicion",
                jugador.get(
                    "casilla",
                    1
                )
            )

            texto(
                f"Jugador {jugador_id}: "
                f"casilla {posicion}",
                PANEL_X + 50,
                y
            )

            y += 30

        texto(
            "Último evento:",
            PANEL_X + 20,
            470,
            NEGRO,
            self.fuente_negrita
        )

        palabras = (
            str(evento)
            .replace(
                "\n",
                " "
            )
            .split()
        )

        linea = ""

        lineas = []

        for palabra in palabras:

            prueba = (
                linea
                + " "
                + palabra
            ).strip()

            if self.fuente.size(
                prueba
            )[0] > PANEL_ANCHO - 40:

                lineas.append(
                    linea
                )

                linea = palabra

            else:

                linea = prueba

        if linea:

            lineas.append(
                linea
            )

        for i, linea in enumerate(
            lineas[:3]
        ):

            texto(
                linea,
                PANEL_X + 20,
                500 + i * 21,
                NEGRO,
                self.fuente
            )

        # ----------------------------------------------------
        # COMPROBAR SI PUEDE LANZAR
        # ----------------------------------------------------

        puede_lanzar = (

            conectado

            and mi_id is not None

            and turno is not None

            and ganador is None

            and str(mi_id) == str(turno)
        )

        self.dibujar_boton(
            self.boton_dado,
            "LANZAR DADO",
            VERDE,
            puede_lanzar
        )

        self.dibujar_boton(
            self.boton_abandonar,
            (
                "CONFIRMAR ABANDONO"
                if confirmar
                else "ABANDONAR PARTIDA"
            ),
            ROJO,
            conectado
        )


    # ========================================================
    # PANTALLA ERROR
    # ========================================================

    def dibujar_error(self):

        self.pantalla.fill(
            FONDO
        )

        titulo = (
            self.fuente_grande.render(
                "CONEXIÓN FINALIZADA",
                True,
                ROJO
            )
        )

        self.pantalla.blit(
            titulo,
            (
                30,
                30
            )
        )

        with self.estado.lock:

            mensaje = (
                self.estado.ultimo_evento
            )

        y = 90

        for linea in mensaje.split(
            "\n"
        ):

            superficie = (
                self.fuente.render(
                    linea,
                    True,
                    NEGRO
                )
            )

            self.pantalla.blit(
                superficie,
                (
                    30,
                    y
                )
            )

            y += 30

        texto = (
            self.fuente.render(
                "Presiona ESC o cierra la ventana.",
                True,
                GRIS_OSCURO
            )
        )

        self.pantalla.blit(
            texto,
            (
                30,
                y + 20
            )
        )


    # ========================================================
    # DIBUJAR TODO
    # ========================================================

    def dibujar(self):

        self.pantalla.fill(
            FONDO
        )

        self.dibujar_tablero()

        self.dibujar_escaleras()

        self.dibujar_serpientes()

        self.dibujar_jugadores()

        self.dibujar_panel()


# ============================================================
# JUEGO CLIENTE
# ============================================================

class JuegoCliente:

    def __init__(self):

        pygame.init()

        self.pantalla = pygame.display.set_mode(
            (
                ANCHO,
                ALTO
            )
        )

        pygame.display.set_caption(
            "Serpientes y Escaleras - Cliente"
        )

        self.reloj = pygame.time.Clock()

        self.estado = EstadoJuego()

        self.red = ClienteRed(
            self.estado
        )

        self.renderizador = RenderizadorJuego(
            self.pantalla,
            self.estado
        )


    # ========================================================
    # LANZAR DADO
    # ========================================================

    def lanzar_dado(self):

        with self.estado.lock:

            conectado = (
                self.estado.conectado
            )

            mi_id = (
                self.estado.mi_id
            )

            turno = (
                self.estado.turno
            )

            ganador = (
                self.estado.ganador
            )

        if not conectado:

            self.actualizar_evento(
                "No estás conectado."
            )

            return

        if mi_id is None:

            self.actualizar_evento(
                "Esperando identificación..."
            )

            return

        if ganador is not None:

            self.actualizar_evento(
                "La partida ya terminó."
            )

            return

        if (
            turno is None
            or str(mi_id) != str(turno)
        ):

            self.actualizar_evento(
                f"No es tu turno. "
                f"Turno del jugador {turno}."
            )

            return

        enviado = self.red.enviar(
            {
                "tipo": "lanzar_dado"
            }
        )

        if enviado:

            self.actualizar_evento(
                "Lanzando dado..."
            )

        else:

            self.actualizar_evento(
                "No se pudo enviar el lanzamiento."
            )


    # ========================================================
    # ACTUALIZAR EVENTO
    # ========================================================

    def actualizar_evento(
        self,
        mensaje
    ):

        with self.estado.lock:

            self.estado.ultimo_evento = (
                mensaje
            )


    # ========================================================
    # ABANDONAR
    # ========================================================

    def abandonar_partida(self):

        with self.estado.lock:

            if not self.estado.confirmar_abandono:

                self.estado.confirmar_abandono = True

                self.estado.ultimo_evento = (
                    "Pulsa otra vez para "
                    "confirmar el abandono."
                )

                return

            self.estado.ejecutando = False

        self.red.enviar(
            {
                "tipo": "abandonar"
            }
        )

        self.red.cerrar()


    # ========================================================
    # EVENTOS
    # ========================================================

    def manejar_evento(
        self,
        evento
    ):

        if evento.type == pygame.QUIT:

            with self.estado.lock:

                self.estado.ejecutando = False

            return

        if (
            evento.type
            == pygame.MOUSEBUTTONDOWN
            and evento.button == 1
        ):

            if self.renderizador.boton_dado.collidepoint(
                evento.pos
            ):

                self.lanzar_dado()

            elif self.renderizador.boton_abandonar.collidepoint(
                evento.pos
            ):

                self.abandonar_partida()

        elif evento.type == pygame.KEYDOWN:

            if evento.key == pygame.K_SPACE:

                self.lanzar_dado()

            elif evento.key == pygame.K_ESCAPE:

                with self.estado.lock:

                    self.estado.ejecutando = False


    # ========================================================
    # EJECUTAR
    # ========================================================

    def ejecutar(self):

        conectado = self.red.conectar()

        while True:

            with self.estado.lock:

                ejecutando = (
                    self.estado.ejecutando
                )

            if not ejecutando:
                break

            for evento in pygame.event.get():

                with self.estado.lock:

                    conectado_actual = (
                        self.estado.conectado
                    )

                if conectado_actual:

                    self.manejar_evento(
                        evento
                    )

                else:

                    if evento.type == pygame.QUIT:

                        with self.estado.lock:

                            self.estado.ejecutando = False

                    elif (
                        evento.type
                        == pygame.KEYDOWN
                        and evento.key
                        == pygame.K_ESCAPE
                    ):

                        with self.estado.lock:

                            self.estado.ejecutando = False

            with self.estado.lock:

                conectado_actual = (
                    self.estado.conectado
                )

            if conectado_actual:

                self.renderizador.dibujar()

            else:

                self.renderizador.dibujar_error()

            pygame.display.flip()

            self.reloj.tick(
                FPS
            )

        self.red.cerrar()

        pygame.quit()


# ============================================================
# INICIO
# ============================================================

if __name__ == "__main__":

    JuegoCliente().ejecutar()