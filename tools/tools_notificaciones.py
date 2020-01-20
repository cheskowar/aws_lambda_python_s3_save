#!/usr/bin/python3
# -*- encoding:utf-8 -*-

import logging
import os
import requests, json

__author__ = 'Carlos Añorve/Omar Reyes/Benjamin N'
__version__ = '1.1'

__logger = logging.getLogger('notificaciones')

url = os.environ['URL_Notificaciones']
header = {"Content-type": "application/json"}

def enviar_notificacion(id_tipo_notificacion, descripcion, parametros='',
                        id_carga='', id_descarga='', id_archivo='', debug=False):

    __evento = {
        "id_tipo_notificacion": id_tipo_notificacion,
        "descripcion": str(descripcion),
        "parametros": parametros,
        "id_carga": id_carga,
        "id_descarga": id_descarga,
        "id_archivo": id_archivo
    }

    try:
        response_decoded_json = requests.post(url, data=json.dumps(__evento), headers=header)
        response_json = response_decoded_json.json()
        print("Envía la notificación, response_json: ", response_json)

    except Exception as details:
        print(f'Ocurrió un error en la invocación de la API de notificaciones\n'
             f' con el id_tipo_notificacion {id_tipo_notificacion} y los detalles del error: {details}')
