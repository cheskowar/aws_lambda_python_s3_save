# -*- coding: utf-8 -*-
import os
import pytz
import logging
from datetime import datetime
from tools import tools_sql as ts
from tools import tools_descarga as td
from tools import tools_validaciones as tv
from tools import tools_notificaciones as tn

__author__ = 'Carlos Antonio/Omar Reyes/Benjamin N
__version__ = '1.1'

HOST = os.environ['HOST']
USER = os.environ['USER']
PASSWD = os.environ['PASSWD']

try:
    DATABASE = os.environ['DATABASE']
except (KeyError, Exception):
    DATABASE = 'carga'
debug = False

logger = logging.getLogger('Lambda handler')

def lambda_handler(event, context):
    del context
    print('Inicia proceso de descarga.')
    control_descarga = ts.SQLDownloadControl(HOST, USER, PASSWD, DATABASE)
    event['control_descarga'] = control_descarga
    descarga = td.Descarga(td.event2lowecase(event))
    print('Se añaden variables al objeto.')
    descarga.nombre_archivo_cache = 'cache_promedio'
    descarga.puerto = 22
    descarga.directorio_local = '/tmp/'
    print('Inicia el proceso de descarga.')

    try:
        if not descarga.start():
            print('Error al intentar descargar el archivo: {}'.format(descarga.nombre_archivo))
            raise Exception("Error al intentar descargar el archivo")
        if descarga.protocolo in ['script', 'webservice', 'derechos']:
            print("Protocolo no soportado", descarga.protocolo)
            raise Exception("Protocolo no soportado")
    except Exception as error:
        control_descarga.GuardarError(descarga.id_ArchivoInstancia)

    print('Revisar promedios de horario y tamaño de archivo y notifica en caso de ser necesario.')
    Var = tv.proceso_promedios(descarga, HOST, USER, PASSWD, DATABASE)
    descarga.tamano_archivo = Var[1][0]
    Tam_Hash = Var[1]

    try:
        print("Validación del Hash")
        if Tam_Hash[3] == True:  # Se detiene la lambda de descargas por validación del hash
            descarga.borrarRegistroRepetido()
            descarga.eliminar_archivo_descargado()
            return {"estado":"Aunque el proveedor puso nuevamente el archivo en su servidor, éste no cambió"}
        elif Tam_Hash[3] == '':
            print("Regreso cadenas vacia, primera vez que se sube el archivo")
            print("No envia notificacion de reenvio")
        elif Tam_Hash[3] == False:
            print("El hash es diferente, se eviara notifiacion")
            try:
                hllegada = datetime.utcnow().replace(tzinfo=pytz.UTC).astimezone(pytz.timezone('America/Mexico_City')).replace(tzinfo=None) 
                hour = str(hllegada.time().hour) if hllegada.time().hour >= 10 else '0'+str(hllegada.time().hour)
                minutes = str(hllegada.time().minute) if hllegada.time().minute >= 10 else '0'+str(hllegada.time().minute)
                tn.enviar_notificacion(id_tipo_notificacion = '83',descripcion = 'El hash es diferente en un reenvio',
                                        parametros =[{"idAI" :event['id_archivo_instancia'],"Hllegada":hour+":"+minutes, "servicio": "descarga"}])
            except Exception as err: 
                print("La notificacion de hash diferente no se envio, los motivos son: ",err)
        else:
            pass

            
    except Exception as error:
        print("Error al intentar extraer el valor del hash")

    tv.validar_fechas_archivo(HOST, USER, PASSWD, DATABASE,
                              descarga.id_archivo,
                              descarga.nombre_archivo,
                              descarga.id_descarga)
    descarga.finish_temporal(Tam_Hash, descarga.id_archivo)
    descarga.eliminar_archivo_descargado()
    
    # # descarga.finish()