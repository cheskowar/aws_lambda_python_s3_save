#!/usr/bin/python3
# -*- encoding:utf-8 -*-

import logging

from suds.client import Client


__author__ = 'Carlos Añorve/Omar Reyes/Benjamin N'
__version__ = '1.0'


class ClientWCFDatamaster:
    def __init__(self, ip):
        self.__logger = logging.getLogger('ClienteWCFDatamaster')
        self.__ip = ip
        self.__ruta = 'http://{}/webserver/Service1.svc?WSDL'.format(self.__ip)
        self.__client = None
        self.mensaje_de_error = None

    @property
    def client(self):
        self.make_client()
        return self.__client

    def make_client(self):
        try:
            print('Creando cliente con la ruta {}'.format(self.__ruta))
            self.__client = Client(self.__ruta)
        except Exception as details:
            self.mensaje_de_error = 'Error al intentar crear el cliente al wcf {}\n' \
                               'Detalles de error: {}'.format(self.__ruta, details)
            print(self.mensaje_de_error)

    def web_service_archivo_contrasena(self, nombre_archivo, id_descarga, fecha_de_modificcion_de_archivo,
                                       id_interno, carpeta_s3, ruta_script):
        try:
            result = self.client.service.WebServiceArchivoContrasena(nombre_archivo,
                                                                       nombre_archivo,
                                                                       id_descarga,
                                                                       fecha_de_modificcion_de_archivo,
                                                                       id_interno,
                                                                       carpeta_s3,
                                                                       ruta_script)
        except (AttributeError, Exception) as details:
            self.mensaje_de_error = 'Error al intetar llamar al servicio: WebServiceArchivoContrasena\n' \
                                    'Detalles de error: {}'.format(details)
            print(self.mensaje_de_error)
            return False
        return result

    def llamar_web_service(self, nombre_archivo, archivos_internos, id_descarga,
                           fecha_modificacion_de_archivo,
                           id_interno, carpeta_s3):
        try:
            result = self.client.service.llamarWebService(nombre_archivo, archivos_internos,
                                                            id_descarga, fecha_modificacion_de_archivo,
                                                            id_interno, carpeta_s3)
        except (AttributeError, Exception) as details:
            self.mensaje_de_error = 'Error al intetar llamar al servicio: llamarWebService\n' \
                                    'Detalles de error: {}'.format(details)
            print(self.mensaje_de_error)
            return False
        return result
