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

        # Animaciones por jugador: la posición visual puede avanzar
        # sin alterar la posición oficial recibida del servidor.
        self.animaciones = {}

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
