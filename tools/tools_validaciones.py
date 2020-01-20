#!/usr/bin/python3
# -*- encoding:utf-8 -*-

import os
import re
import pickle
import zipfile
import logging
import hashlib
from datetime import datetime, timedelta

import pytz
from . import cat_formato_fecha
from . import tools_aws as taws
from . import tools_sql as ts
from . import tools_notificaciones as tn
from . import tools_descarga as td

__author__ = 'Carlos Añorve/Omar Reyes/Benjamin N'
__version__ = '1.1'

logger = logging.getLogger('tools_validaciones')

debug = False


def leer_cache_s3(bucket, folder_cache, id_archivo, nombre_archivo):
    """
    Descarga un archivo de un s3, el archivo se guardara
    en la carpeta temporal de la lambda "/tmp/"
    para su uso posterior.
    Args:
        bucket(str): Nombre del bucket donde se encuentra el archivo.
        folder_cache(str): Nombre de la carpeta en el bucket donde se encuentra el archivo.
        id_archivo(str): id que identifica al archivo, este valor se obtiene de parametro
            event de la funcion principal de la lambda.
        nombre_archivo (str):
    Returns:
        Devuelve un diccionario con la informacion previamente guardada.
            En caso de no existir un archivo previo se genera un diccionario
            con la estructura correspondiente.
    """
    try:
        taws.descarga_de_s3(bucket, nombre_archivo, folder_cache)
        with open(f'/tmp/{nombre_archivo}', 'rb') as df:
            estructura_cache = pickle.loads(df.read())
    except Exception:
        print('No se encontro ningun archivo de cache con el nombre {}\n'
                       'en el bucket y carpeta {}, Se generara '
                       'un archivo nuevo.'.format(nombre_archivo, bucket, folder_cache))
        estructura_cache = {id_archivo: {'Tamanos_mb': [], 'Fechas': []}}
    return estructura_cache


def escribir_s3(estructura_cache, bucket, folder_cache, nombre_archivo):
    """
    Guarda el cache_promedios en un s3.

    Args:
        estructura_cache(): diccionario con la informacion para guardar en cache.
        bucket(str): Nombre del bucket donde esta la carpeta donde se guardara el archivo.
        folder_cache(str): NOmbre de la carpeta en el bucket donde se guardara el archivo.
    Returns:
         None
    """
    try:
        with open(f'/tmp/{nombre_archivo}', 'wb') as df:
            pickle.dump(estructura_cache, df)
            # print("guarda correctamente en el ")
    except Exception as details:
        print('Error al intentar guardar el archivo de promedios en la ubicacion temporal.\n'
                     'Detalles de error: {}'.format(details))
        return False
    try:
        taws.subir_a_s3(bucket, nombre_archivo, carpeta_s3=folder_cache)
    except Exception as details:
        print('Error al intentar cargar el archivo de cache al s3\n'
                       'Detalles del error: {}'.format(details))
        return False
    return True


def __calcular_peso_de_archivo(ruta_archivo):
    try:
        tamanio = os.path.getsize(ruta_archivo)
    except Exception as details:
        print('Error al obtener el peso del archivo.\n'
                       'Detalles: {}'.format(details))
        return False
    return round(float(tamanio / (1024.0 ** 2)), 3)


def __calcular_hash_archivo(ruta_archivo):
    hashsha = hashlib.sha256()
    try:
        f = open(ruta_archivo, "rb")
    except IOError as e:
        print(e)
        return False
    else:
        data = f.read()
        f.close()
        hashsha.update(data)
        return hashsha.hexdigest()


def __calcular_peso_promedio(todos_los_registros):
    tamanio_total = 0
    for registro in todos_los_registros:
        tamanio_total += registro
    return tamanio_total / len(todos_los_registros)


def __calcular_peso_promedio_db(id_archivo):
    help_query = ts.HelpQuery(host, user_bd, passwd_bd, database)
    peso_prom = help_query.func_peso_promedio('datamaster', id_archivo)
    return peso_prom


def __diferencia_peso(peso_archivo, peso_promedio, id_descarga, id_AI, porcentaje):
    try:
        # porcentaje = int(os.environ['porcentaje_tamano_de_tolerancia'])
        porcentaje = porcentaje/100
    except Exception as details:
        print('Error al obtener el porcentaje_tamano_de_tolerancia de usara una tolerancia del 20%\n'
              'Detalles: {}'.format(details))
        porcentaje = .2
    if not peso_promedio:
        peso_promedio = peso_archivo
    tolerancia = peso_promedio * porcentaje
    if peso_archivo > peso_promedio:
        diferencia = peso_archivo - peso_promedio
        estatus = 'Archivo con {}Mb mas que el promedio'.format(diferencia)
    else:
        diferencia = peso_promedio - peso_archivo

        estatus = 'Archivo con {}Mb menos que el promedio'.format(diferencia)
    if diferencia <= tolerancia:
        estatus = ''
    else:
        estatus += '\nPeso promedio: {}\nPeso actual: {}'.format(peso_promedio, peso_archivo)
        print(estatus)

        # parametros = [{"PesoPromedio": peso_promedio, "PesoArchivo": peso_archivo, "idAI":id_AI, "servicio": "descarga"}]
        parametros = [{
            "PesoPromedio": "{:.3f}".format(peso_promedio),
            "PesoArchivo": "{:.3f}".format(peso_archivo),
            "idAI": id_AI, 
            "servicio": "descarga"
        }]
        tn.enviar_notificacion(46, "", parametros, "",
                               id_descarga, "", debug=debug)
        tn.enviar_notificacion(46, estatus, id_descarga=id_descarga, debug=debug)

    return estatus


def __calcular_hora_a_segundos():
    desface = timedelta(hours=6)
    hh_mm_ss = datetime.utcnow() - desface
    segundos = hh_mm_ss.hour * 3600
    segundos += hh_mm_ss.minute * 60
    segundos += hh_mm_ss.second
    return segundos


def __calcular_horario_promedio(todos_los_registros):
    horario_total = timedelta(seconds=0)
    for registro in todos_los_registros:
        horario_total += timedelta(seconds=registro)
    if todos_los_registros:
        return int(horario_total.total_seconds()/len(todos_los_registros))
    else:
        return int(horario_total.total_seconds())

def __diferencia_de_timepo(promedio, id_descarga, id_AI, estado):
    estado = estado.lower()
    if estado=="descargado":
        print("El archivo ya fue descargado, y no se notifica como fuera de horario")
        return
    if promedio["horaFin"]==None:
        promedio["horaFin"]='22:00:00'
    hora_actual = datetime.utcnow().replace(tzinfo=pytz.UTC).astimezone(pytz.timezone('America/Mexico_City')).time()
    try:
        if hora_actual < datetime.strptime(promedio["horaInicio"], '%H:%M:%S').time() or hora_actual > datetime.strptime(promedio["horaFin"], '%H:%M:%S').time():
            estatus = """La hora de llegada es entre las '%s' y 
                         las '%s', por lo que el archivo
                         llegó fuera de horario""" % (promedio["horaInicio"], promedio["horaFin"])
            print(estatus)
            hora_actual = str(hora_actual)[:5]
            promedio["horaInicio"] = promedio["horaInicio"][:5]
            promedio["horaFin"] = promedio["horaFin"][:5]
            parametros = [{"Hllegada":str(hora_actual), "Hpromedio":str(promedio), "idAI":id_AI, "servicio": "descarga"}]
            tn.enviar_notificacion(47, "", parametros, "",
                                   id_descarga, "", debug=debug)
        else:
            estatus="El archivo llegó dentro de su horario de llegada"
            print(estatus)
    except Exception as error:
        estatus = "En la tabla TB_Archivo, no contiene los campos de horaInicio o horaNotificacion"
    return estatus

def __seconds_to_string(seconds):
    try:
        str_time = timedelta(seconds=seconds).__str__()
    except Exception as details:
        print('Error al obtener el strgin de los segundos\n'
                       'Detalles: {}'.format(details))
        return seconds
    else:
        return str_time


def proceso_promedios(info_archivo:td.Descarga, host, user_bd, passwd_bd, database) -> tuple:
    help_query = ts.HelpQuery(host, user_bd, passwd_bd, database)
    if help_query.validar_promedios(info_archivo.id_archivo):
        print('nombre_archivo: {}'.format(info_archivo.nombre_archivo, info_archivo.nombre_archivo_cache))
        ruta_archivo = os.path.join(info_archivo.directorio_local, info_archivo.nombre_archivo)
        cache = leer_cache_s3(info_archivo.bucket, info_archivo.folder_cache,
                              info_archivo.id_archivo, info_archivo.nombre_archivo_cache)
        if not cache:
            return ()
        try:
            datos = cache[info_archivo.id_archivo]
        except (KeyError, Exception) as details:
            print('Aun no hay informacion para el archivo {}\n'
                  'Detalles: {}'.format(info_archivo.nombre_archivo, details))
            cache[info_archivo.id_archivo] = {'Tamanos_mb': [], 'Fechas': []}
            datos = cache[info_archivo.id_archivo]
        registros_de_peso = datos['Tamanos_mb']
        registro_de_horario = datos['Fechas']
        horario_actual = __calcular_hora_a_segundos()
        peso_de_archivo = __calcular_peso_de_archivo(ruta_archivo)
        ArcHash = __calcular_hash_archivo(ruta_archivo)
        V = help_query.validarControlDescarga(info_archivo.id_ArchivoInstancia, ArcHash)

        if len(registro_de_horario) >= 20:
            registro_de_horario.pop(0)
        registro_de_horario.append(horario_actual)
        if len(registros_de_peso) >= 20:
            registros_de_peso.pop(0)
        registros_de_peso.append(peso_de_archivo)
        horario_promedio = __calcular_horario_promedio(registro_de_horario)
        hora_inicio_fin = help_query.calcular_hora_InicioFin(info_archivo.id_archivo)
        peso_promedio_s3 = __calcular_peso_promedio(registros_de_peso)
        print("Peso promedio cache S3: {}".format(peso_promedio_s3))
        print("id archivo: {}".format(info_archivo.id_archivo))
        peso_promedio = help_query.func_peso_promedio('datamaster', info_archivo.id_archivo)
        print("Peso promedio DB: {}".format(peso_promedio))
        datos['Tamanos_mb'] = registros_de_peso
        datos['Fechas'] = registro_de_horario
        cache[info_archivo.id_archivo] = datos
        escribir_s3(cache, info_archivo.bucket, info_archivo.folder_cache, info_archivo.nombre_archivo_cache)

        estadoDescargaArchivo = help_query.EstadoDescargaArchivo(info_archivo.id_ArchivoInstancia)

        __diferencia_de_timepo(hora_inicio_fin, info_archivo.id_descarga, info_archivo.id_ArchivoInstancia, estadoDescargaArchivo)
        porcentaje = help_query.porcentaje_tolerancia_peso(info_archivo.id_archivo)
        __diferencia_peso(peso_de_archivo, peso_promedio, info_archivo.id_descarga, info_archivo.id_ArchivoInstancia, porcentaje)
        informacion__resultante = ((horario_actual, horario_promedio),
                                   (peso_de_archivo, peso_promedio, ArcHash, V),
                                   cache)
        info_archivo.control_descarga.spp_control_validaciones(info_archivo.id_archivo, info_archivo.id_descarga,
                                                               peso_promedio, __seconds_to_string(horario_promedio),
                                                               peso_de_archivo, __seconds_to_string(horario_actual))
        return informacion__resultante
    else:
        print('Ya se proceso la informacion del dia de hoy')
        ruta_archivo = os.path.join(info_archivo.directorio_local, info_archivo.nombre_archivo)
        peso_de_archivo = __calcular_peso_de_archivo(ruta_archivo)
        ArcHash = __calcular_hash_archivo(ruta_archivo)
        informacion_resultante = ((), (peso_de_archivo, "", ArcHash), None)
        return informacion_resultante


def __separar_nombre_extencion(nombre_archivo):
    nombre_archivo, extencion = os.path.splitext(nombre_archivo)
    return nombre_archivo, extencion


def __comparar_extenciones(nombre_archivo1, nombre_archivo2):
    nombre1, extencion1 = __separar_nombre_extencion(nombre_archivo1)
    nombre2, extencion2 = __separar_nombre_extencion(nombre_archivo2)
    if extencion1.lower() != extencion2.lower():
        print('Extenciones diferentes')
        return False
    else:
        print('Extenciones iguales.')
        return True


def __extraer_nombre_del_zip(nombre_archivo, directorio='/tmp/'):
    print('Extrayedo nombres del arhivo zip')
    try:
        zip_file = zipfile.ZipFile('{}'.format(os.path.join(directorio, nombre_archivo)))
    except Exception as details:
        mensaje_de_error = 'Errro al intentar recuperar la informacion del archivo {}\n' \
                           'Detalles: {}'.format(nombre_archivo, details)
        print(mensaje_de_error)
        tn.enviar_notificacion('', mensaje_de_error, debug=debug)
        return []
    else:
        return [archivo for archivo in zip_file.namelist() if os.path.split(archivo)[1]]


def __eliminar_fecha_del_nombre(nombre_archivo):
    formatos = str('|').join(cat_formato_fecha.dictformatos.keys())
    formatos = re.compile(formatos)
    for formato in formatos.findall(nombre_archivo):
        nombre_archivo = nombre_archivo.replace(formato, ' ')
    return nombre_archivo.split()


def __extraer_fecha_de_archivo(nombre_archivo_descargado, nombre_archivo_en_base):
    nombre_archivo_descargado = nombre_archivo_descargado.lower()
    print('Nombre archivo: {}'.format(nombre_archivo_descargado))
    for parte_nombre in __eliminar_fecha_del_nombre(nombre_archivo_en_base):
        nombre_archivo_descargado = nombre_archivo_descargado.replace(parte_nombre.lower(), '')
    print('Fecha: {}'.format(nombre_archivo_descargado))
    return nombre_archivo_descargado


def validar_fechas_archivo(host, user, passd, database, id_archivo, nombre_archivo, id_descarga):
    if __separar_nombre_extencion(nombre_archivo)[1].lower() == '.zip':
        help_query = ts.HelpQuery(host, user, passd, database)
        nombre_en_base = help_query.nombre_archivo(id_archivo=id_archivo)
        print(f'Nombre en base: {nombre_en_base}')
        if not __comparar_extenciones(nombre_en_base, nombre_archivo):
            mensaje_de_error = 'La extencion del archivo no coincide con el definido en base.'
            print(mensaje_de_error)
            tn.enviar_notificacion('', mensaje_de_error, id_archivo=id_archivo, debug=debug)
            return False
        print('Procesando archivo zip')
        nombre_archivos_internos_en_base = help_query.archivos_internos(id_archivo=id_archivo)
        if nombre_archivos_internos_en_base:
            if debug:
                archivos_internos = __extraer_nombre_del_zip(nombre_archivo, directorio='tmp')
            else:
                archivos_internos = __extraer_nombre_del_zip(nombre_archivo)
            if len(archivos_internos) == 1 and len(archivos_internos) == 1:
                print('Proceso para compara fechas.')
                fecha_archivo = __extraer_fecha_de_archivo(nombre_archivo, nombre_en_base)
                fecha_archi_interno = __extraer_fecha_de_archivo(archivos_internos[0],
                                                                 nombre_archivos_internos_en_base[0]['nombreArchivo'])
                if fecha_archivo != fecha_archi_interno:
                    mensaje_de_error = 'La fecha del archivo zip no coincide con la del archivo interno. ' \
                                       'Nombre archivo: {}' \
                                       'Nombre archivo inerno: {}'.format(nombre_archivo, archivos_internos[0])
                    print(mensaje_de_error)
                    tn.enviar_notificacion(52, mensaje_de_error, id_descarga=id_descarga, debug=debug)
                    return False
                else:
                    return True
    else:
        return True
