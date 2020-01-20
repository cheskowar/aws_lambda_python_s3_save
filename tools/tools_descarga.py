#!/usr/bin/python3
# -*- encoding:utf-8 -*-


import os
import ssl
import json
import shutil
import ftplib
import logging
import datetime
import time
import hashlib
from urllib import request
import pytz

import paramiko
from . import tools_aws as taws
from . import tools_wcf as tw
from . import tools_notificaciones as tn
from . import tools_sql as ts
from . import helpers

author = 'Carlos Añorve/Omar Reyes/Benjamin N'
version = '1.1'
HOST = os.environ['HOST']
USER = os.environ['USER']
PASSWD = os.environ['PASSWD']
debug = False

METODOS = ['ftp', 'sftp', 'http', 'https',
           's3', 'script',
           'webservice', 'derechos']

MENSAJE_DE_DESCARGA_EXITOSA = 'Se descargo correctamente el archivo: {}'


class Evento:
    def __init__(self, carpetaorigen: str, intervalo: str, ultimarevision: str,
                 fechamodificacionarchivo: str, numerocargas: (int, str), idinterno: (int, str),
                 idproveedor: (int, str), horanotificacion: str, contrasena: str,
                 carpetas3: str, protocolo: int, nombrearchivo: str,
                 horariosnotificacion: str, fechaultimadescarga: str,
                 idlayout: (int, str), extension: str, usuario: str, tiempocargaarchivo: (int, str),
                 rutadescarga: str, archivosinternos: list, rutascript: str,
                 parametros: dict, idarchivo: (int, str), horainicio: str, id_archivo_instancia: (int, str),
                 tipodesc:str, control_descarga: ts.SQLDownloadControl = None):
        self.__logger = logging.getLogger('Evento')
        self.__intervalo = intervalo,
        self.__ultima_revicion = ultimarevision,
        self.__numero_cargas = numerocargas
        self.__hora_notificacion = horanotificacion
        self.__horarios_notificacion = horariosnotificacion
        self.__fecha_ultima_Descarga = fechaultimadescarga
        self.__extencion = extension
        self.__timepo_carga_archivo = tiempocargaarchivo
        self.__hora_inicio = horainicio
        self.__extencion_archivo = None
        self.archivos_internos = archivosinternos
        self.control_descarga = control_descarga
        self.protocolo = protocolo  # Define que metodo de descarga se usara
        self.id_interno = idinterno  # Id interno del archivo
        self.id_archivo = idarchivo  # Id del archivo
        self.id_ArchivoInstancia = id_archivo_instancia
        self.tipodesc=tipodesc
        self.fecha_modificacion_de_archivo = fechamodificacionarchivo  # Fecha de la ultima modificacion del archivo
        self.__carpeta_origen = carpetaorigen  # carpeta de donde proviene el archivo
        self.ip_ftp = rutadescarga  # Ip del servidor ftp
        self.ip_sftp = rutadescarga  # Ip del servidor sftp
        self.usuario = usuario  # Nombre de usuario para realizar el login
        self.contrasena = contrasena  # Contraseña correspondiente al usuario
        self.nombre_archivo = nombrearchivo  # Nombre del archivo que se quiere descargar
        self.direccion_web = rutadescarga  # Dirreccion http o https
        self.bucket_descarga_s3 = rutadescarga  # Nombre del bucket de donde decargar el archivo
        self.ruta_script = rutascript  # Ruta de script para Benchmarks
        self.carpeta_destino = carpetas3  # Carpeta donde se guardara el archivo en s3
        self.id_proveedor = idproveedor  # Id del proveedor
        self.id_layout = idlayout  # Id del layout del archivo
        self.__parametros = parametros  # Parametros obtenidos de tb_Archivo
        self.__puerto = None  # puerto para la conexion
        self.__directorio_local = None  # Ruta donde se guardan los archivos antes del s3
        self.__nombre_archivo_cache = None
        self.bucket = None  # Bucket de en donde depositar los archivos
        self.ip_wcf = None  # Ip de donde se encuentran los WCF
        self.lambda_derechos = None  # ARN de la lambda de derechos
        self.folder_cache = None  # Folder donde se guardan los archivos cache
        # self.corregir_parametros()
        self.folder_temporal = 'Temporal'

        if not (control_descarga is None):
            self.id_descarga = self.controlDescarga()

        self.corregir_parametros()
        self.__tamanio_archivo = None

    def controlDescarga(self):
        estado='En Carga'
        hash=''
        id_descarga=self.control_descarga.start(self.id_ArchivoInstancia,
                                       self.fecha_modificacion_de_archivo, estado,
                                       0, hash, "carga", self.tipodesc)
        return id_descarga

    def corregir_parametros(self):
        self.get__ip_ftp()
        self.get_ip_sftp()
        self.get_bucket()
        self.get_ip_procesos()
        self.get_name_folder_cache()
        self.get_arn_lambda_derechos()
        self.get_ruta_web()
        self.get_archivos_internos()

    @property
    def extencion_archivo(self):
        if self.__extencion is None:
            ext = os.path.splitext(self.nombre_archivo)[1]
            if ext:
                return ext.replace('.', '').lower()
            else:
                return 'o'
        else:
            return self.__extencion.lower()

    @extencion_archivo.setter
    def extencion_archivo(self, value):
        if isinstance(value, str):
            self.__extencion_archivo = value
        else:
            print('Extencion no valida.')

    @property
    def tamano_archivo(self):
        return self.__tamanio_archivo

    @tamano_archivo.setter
    def tamano_archivo(self, value):
        try:
            value = float(value)
        except ValueError:
            print('El valor obtenido no es un valor numerico\n'
                  'Valor {} '.format(value))
            self.__tamanio_archivo = 0
        else:
            self.__tamanio_archivo = value

    @property
    def puerto(self):
        return self.__puerto

    @puerto.setter
    def puerto(self, value):
        try:
            puerto = int(value)
        except (ValueError, Exception) as details:
            mensaje_de_error = 'Error al asignar el puerto, asegurate que el puerto sea un numero entero.\n' \
                               'Detalles: {}'.format(details)
            print(mensaje_de_error)
            tn.enviar_notificacion('', mensaje_de_error, debug=debug)
        else:
            self.__puerto = puerto

    @property
    def directorio_local(self):
        if not self.__directorio_local:
            return '/tmp/'
        return self.__directorio_local

    @directorio_local.setter
    def directorio_local(self, value):
        mensaje_de_error = None
        try:
            value = int(value)
        except ValueError:
            try:
                value = float(value)
            except ValueError:
                self.__directorio_local = value
            else:
                mensaje_de_error = 'Ladirrecion {} no es valida para directorio_local'.format(value)
        else:
            mensaje_de_error = 'Ladirrecion {} no es valida para directorio_local'.format(value)
        if mensaje_de_error:
            print(mensaje_de_error)
            tn.enviar_notificacion('', mensaje_de_error, debug=debug)

    @property
    def nombre_archivo_cache(self):
        if not self.__nombre_archivo_cache:
            return 'cache_promedio'
        return self.__nombre_archivo_cache

    @nombre_archivo_cache.setter
    def nombre_archivo_cache(self, value):
        self.__nombre_archivo_cache = value

    @property
    def carpeta_origen(self):
        if not self.__carpeta_origen:
            return ''
        return self.__carpeta_origen

    @property
    def parametros(self):
        if not self.__parametros:
            return dict()
        else:
            try:
                self.__parametros = json.loads(self.__parametros)
            except Exception as details:
                print('Problemas al cargar como json los parametros\n'
                      'Detalles: {}'.format(details))
                if isinstance(self.__parametros, dict):
                    return self.__parametros
                return dict()

    def get__ip_ftp(self):
        try:
            self.ip_ftp = self.ip_ftp.split('//')[1]
        except (IndexError, Exception) as details:
            if self.protocolo == 'ftp':
                mensaje_de_error = 'Error al intentar extraer la ip para la conexion con el servidor ftp\n' \
                                   'Detalles de error: {}'.format(details)
                print(mensaje_de_error)
                insBD = ts.HelpQuery(HOST, USER, PASSWD, "datamaster")
                proveedor = insBD.consultar_proveedor(self.id_proveedor)
                parametros = [{"protocolo": str(self.protocolo), "proveedor": proveedor,
                               "idAI":self.id_ArchivoInstancia, "servicio": "descarga"}]
                tn.enviar_notificacion(66, str(details), parametros, "",
                                       self.id_descarga, "", debug=debug)
                RegistroError = ts.SQLDownloadControl(HOST, USER, PASSWD, "carga")
                RegistroError.GuardarError(self.id_ArchivoInstancia)
            else:
                self.ip_ftp = None

    def get_ip_sftp(self):
        try:
            self.ip_sftp = self.ip_sftp.split('//')[1]
        except (IndexError, Exception) as details:
            if self.protocolo == 'sftp':
                mensaje_de_error = 'Error al intetar extraer la ip para la conexion con el servidor sftp\n' \
                                   'Detalles de error: {}'.format(details)
                print(mensaje_de_error)
                parametros = [{"protocolo": str(self.protocolo), "idAI":self.id_ArchivoInstancia,
                                "servicio": "descarga"}]
                tn.enviar_notificacion(66, str(details), parametros, "",
                                       self.id_descarga, "", debug=debug)
                RegistroError = ts.SQLDownloadControl(HOST, USER, PASSWD, "carga")
                RegistroError.GuardarError(self.id_ArchivoInstancia)
            else:
                self.ip_sftp = None

    def get_bucket(self):
        try:
            self.bucket = os.environ['bucket']
        except (KeyError, Exception) as detils:
            mensaje_de_error = 'error al intetar recuperar el nombre del bucket\n' \
                               'Detalles: {}'.format(detils)
            print(mensaje_de_error)
            parametros = [{"protocolo": str(self.bucket), "idAI":self.id_ArchivoInstancia,
                        "servicio": "descarga"}]
            tn.enviar_notificacion(66, "Error al conectarse con el s3", parametros, "",
                                   self.id_descarga, "", debug=debug)
            RegistroError = ts.SQLDownloadControl(HOST, USER, PASSWD, "carga")
            RegistroError.GuardarError(self.id_ArchivoInstancia)

    def get_ip_procesos(self):
        try:
            self.ip_wcf = os.environ['urlArchivo']
        except (KeyError, Exception) as details:
            mensaje_de_error = 'Error al intentar recuperar la ip del EC2 de procesos\n' \
                               'Detalles: {}'.format(details)
            print(mensaje_de_error)
            parametros = [{"protocolo": str(self.ip_wcf), "idAI":self.id_ArchivoInstancia,
                           "servicio": "descarga"}]
            tn.enviar_notificacion(66, "Error al conectarse", parametros, "",
                                   self.id_descarga, "", debug=debug)
            RegistroError = ts.SQLDownloadControl(HOST, USER, PASSWD, "carga")
            RegistroError.GuardarError(self.id_ArchivoInstancia)

    def get_name_folder_cache(self):
        try:
            self.folder_cache = os.environ['folder_cache']
        except (KeyError, Exception) as details:
            mensaje_de_error = 'Errro al recuperar la folder donde se almacena el cache\n' \
                               'Detalles: {}'.format(details)
            print(mensaje_de_error)
            tn.enviar_notificacion('', mensaje_de_error, debug=debug)
            RegistroError = ts.SQLDownloadControl(HOST, USER, PASSWD, "carga")
            RegistroError.GuardarError(self.id_ArchivoInstancia)

    def get_arn_lambda_derechos(self):
        try:
            self.lambda_derechos = os.environ['lambda_derechos']
        except (KeyError, Exception) as details:
            if self.protocolo == 'derechos':
                mensaje_de_error = 'Error al conseguir el arn de la lambda que procesa los derechos.\n' \
                                   'Detalles de error: {}'.format(details)
                print(mensaje_de_error)
                tn.enviar_notificacion('', mensaje_de_error, debug=debug)
            else:
                self.lambda_derechos = None
            RegistroError = ts.SQLDownloadControl(HOST, USER, PASSWD, "carga")
            RegistroError.GuardarError(self.id_ArchivoInstancia)

    def get_ruta_web(self):
        try:
            self.direccion_web = self.direccion_web.split('//')[1]
        except (IndexError, Exception) as details:
            if self.protocolo in ('http', 'https'):
                mensaje_de_error = 'Error al intetar extraer la ruta para la conexion con {}\n' \
                                   'Detalles de error: {}'.format(self.protocolo, details)
                print(mensaje_de_error)
                tn.enviar_notificacion('', mensaje_de_error, debug=debug)
            else:
                self.direccion_web = None
            RegistroError = ts.SQLDownloadControl(HOST, USER, PASSWD, "carga")
            RegistroError.GuardarError(self.id_ArchivoInstancia)

    def get_archivos_internos(self):
        #FIXME Revisar la función y si no es necesaria quitar.
        print('Sacando internos')
        if self.archivos_internos:
            self.archivos_internos[0]["id_archivo_instancia"]=None
            self.archivos_internos[0]["tipodesc"]=None
            archivos_internos = [ArchivosInternos(event2lowecase(event)) for event in self.archivos_internos]

            self.archivos_internos = archivos_internos
        else:
            self.archivos_internos = []

class ArchivosInternos(Evento):
    def __init__(self, kwargs):
        super().__init__(**kwargs)


class Descarga(Evento):
    def __init__(self, kwargs):
        super().__init__(**kwargs)
        self.__logger = logging.getLogger('Descarga')
        self.__metadatas = {}
    
    def eliminar_archivo_descargado(self):
        """
            Esta funcion tiene como objetivo eliminar el archivo descargado una vez terminado el proceso
            de descarga y carga de la lambda de descargas, para dejar limpio de archivos los contenedores
            de AWS en la siguiente ejecucion de la lambda

        """
        ruta_archivo = os.path.join(self.directorio_local, self.nombre_archivo)
        print("Eliminado archivo '{}'".format(ruta_archivo))
        if os.path.isfile(ruta_archivo):
            print("\tEl archivo existe en el temporal")
            try: 
                os.remove(ruta_archivo)
                print("\tSe eliminó el archivo {} : {}".format(ruta_archivo,not os.path.isfile(ruta_archivo)))
            except Exception as e:
                print("\tError en la eliminación del archivo, motivos:",e)
        

    def start(self):
        if not (self.protocolo in METODOS):
            self.protocolo_no_soportado()
        else:
            print('Se usara el protocolo {}'.format(self.protocolo))
            if self.protocolo == 'ftp':
                return self.ftp()
            elif self.protocolo == 'sftp':
                return self.sftp()
            elif self.protocolo == 'http':
                return self.http()
            elif self.protocolo == 'https':
                return self.https()
            elif self.protocolo == 's3':
                return self.s3()
            elif self.protocolo == 'script':
                return self.script()
            elif self.protocolo == 'webservice':
                # return self.webservice()
                self.__logger.error('Proceso no desarrollado.')
            elif self.protocolo == 'derechos':
                return self.derechos_bolsa()

    def finish_temporal(self, Tam_Hash, id_archivo):
        print('Iniciando carga del archivo {} al s3 {} a la carpeta {}'.format(self.nombre_archivo,
                                                                               self.bucket,
                                                                               self.folder_temporal))

        metadatas = self.parametros
        metadatas['IdArchivo'] = str(self.id_archivo)
        metadatas['IdDescarga'] = str(self.id_descarga)
        metadatas['idInterno'] = str(self.id_interno)
        metadatas['idArchivoInstancia'] = str(self.id_ArchivoInstancia)
        metadatas['FechaDescarga'] = datetime.datetime.now().replace(tzinfo=pytz.UTC).astimezone(pytz.timezone('America/Mexico_City')).strftime('%Y/%m/%d %H:%M:%S')
        try:
            taws.subir_a_s3(self.bucket, self.nombre_archivo, self.id_ArchivoInstancia,
                            self.id_descarga, carpeta_s3=self.folder_temporal, metadatas=metadatas)
        except Exception as details:
            mensaje_de_error = 'Ocurrio un error al intetnar cargar el archivo {} ' \
                               'al s3 {} a la carpeta {}\n' \
                               'Detalles: {}'.format(self.nombre_archivo, self.bucket, self.folder_temporal, details)
            print(mensaje_de_error)

            parametros = [{"path": str(self.bucket), "idAI":self.id_ArchivoInstancia,
                           "servicio": "descarga"}]
            tn.enviar_notificacion(21, str(details), parametros, "",
                                   self.id_descarga, "", debug=debug)

            self.finalizar_carga('Error', Tam_Hash)
        else:
            self.finalizar_carga('Descargado', Tam_Hash)
        finally:
            self.__metadatas = metadatas

    def finish(self):
        print('Finalizando proceso de descarga con extencion {}'.format(self.extencion_archivo))
        if (self.extencion_archivo in ('xls', 'xlsx')) or (self.tamano_archivo > 40):
            self.llamar_a_wcf_procesos('')
        elif self.extencion_archivo == 'zip':
            self.llamar_a_wcf_procesos(self.zip())
        else:
            if self.extencion_archivo == 'json':
                registros = self.json()
            else:
                registros = self.texto_plano()
            nombre, extencion = os.path.splitext(self.nombre_archivo)
            nombre_para_guardar = '{}-_{}_{}_0_{}{}'.format(nombre, self.id_descarga,
                                                            self.id_interno, registros,
                                                            extencion)
            self.__metadatas['Registros'] = str(registros)
            try:
                taws.subir_a_s3(self.bucket, self.nombre_archivo, carpeta_s3=self.carpeta_destino,
                                metadatas=self.__metadatas, nuevo_nombre=nombre_para_guardar)
            except Exception as details:
                mensaje_de_error = 'Ocurrio un error al intetnar cargar el archivo {} ' \
                                   'al s3 {} a la carpeta {}\n' \
                                   'Detalles: {}'.format(self.nombre_archivo, self.bucket, self.carpeta_destino,
                                                         details)
                print(mensaje_de_error)

                parametros = [{"path": str(self.bucket), "idAI":self.id_ArchivoInstancia,
                               "servicio": "descarga"}]
                tn.enviar_notificacion(21, str(details), parametros, "",
                                       self.id_descarga, "", debug=debug)
                RegistroError = ts.SQLDownloadControl(HOST, USER, PASSWD, "carga")
                RegistroError.GuardarError(self.id_ArchivoInstancia)
                self.finalizar_carga('Error')
            else:
                self.finalizar_carga('Descargado')

    def llamar_a_wcf_procesos(self, final_file):
        print(self.ip_wcf)
        client_zip = tw.ClientWCFDatamaster(self.ip_wcf)
        if not client_zip.llamar_web_service(self.nombre_archivo, final_file,
                                             self.id_descarga, self.fecha_modificacion_de_archivo,
                                             self.id_interno, self.carpeta_destino):
            print(client_zip.mensaje_de_error)
            tn.enviar_notificacion(22, client_zip.mensaje_de_error, 'idDescarga={}'.format(self.id_descarga),
                                   id_descarga=self.id_descarga, debug=debug)

    def finalizar_carga(self, estatus, Tam_Hash):
        try:
            tamanoMB = float(Tam_Hash[0])
            Hash = str(Tam_Hash[2])
            self.control_descarga.end(self.id_ArchivoInstancia, self.id_descarga, estatus, tamanoMB, Hash)
            
            return True
        except Exception as details:
            mensaje_de_error = 'No fue posible actualizar del la base de datos "carga" ' \
                               'la tabla "tb_ControlDescarga"\n' \
                               'Detalles: {}'.format(details)
            print(mensaje_de_error)

            tn.enviar_notificacion(23, str(details), "", "",
                                   self.id_descarga, "", debug=debug)

            return False
        # else:
        #     return True

    def protocolo_no_soportado(self):
        mensaje_de_error = 'El protocolo {} no es soportado por la lambda de descarga'.format(self.protocolo)
        print(mensaje_de_error)
        parametros = [{"protocolo":self.protocolo, "idAI":self.id_ArchivoInstancia,
                       "servicio": "descarga"}]
        tn.enviar_notificacion(20, mensaje_de_error, parametros, "",
                               self.id_descarga, "", debug=debug)
        RegistroError = ts.SQLDownloadControl(HOST, USER, PASSWD, "carga")
        RegistroError.GuardarError(self.id_ArchivoInstancia)
        return False

    def ftp(self):
        try:
            print('Creando conexion con servidor ftp')
            ftp = ftplib.FTP(self.ip_ftp)
            ftp.login(self.usuario, self.contrasena)
        except (ftplib.error_reply, Exception) as details:
            mensaje_de_error = 'Error al intentar conectarse al servidor ftp con la ip {} \n' \
                               'detalles del error: {}'.format(self.ip_ftp, details)
            print(f'{mensaje_de_error}')

            parametros = [{"ip": self.ip_ftp, "idAI":self.id_ArchivoInstancia, "servicio": "descarga"}]
            tn.enviar_notificacion(15, str(details), parametros, "",
                                   self.id_descarga, "", debug=debug)
            RegistroError = ts.SQLDownloadControl(HOST, USER, PASSWD, "carga")
            RegistroError.GuardarError(self.id_ArchivoInstancia)
            return False
        try:
            print('Descargando el archivo')
            with open(os.path.join(self.directorio_local, self.nombre_archivo), 'wb') as new_file_ftp:
                ftp.retrbinary('RETR {}'.format(os.path.join(self.carpeta_origen, self.nombre_archivo)),
                               new_file_ftp.write)
            # raise Exception("Forzar la Excepcion")
        except Exception as details:
            mensaje_de_error = """Error al intentar descargar el archivo:
                               {} del ftp con la ip: {}
                               Detalles del error: {}""".format(self.nombre_archivo, self.ip_ftp, details)
            print("detalles", details)
            insBD = ts.HelpQuery(HOST, USER, PASSWD, "datamaster")
            proveedor = insBD.consultar_proveedor(self.id_proveedor)
            print("Proveedor: ", proveedor)
            parametros = [{"protocolo": "ftp", "proveedor": proveedor,
                           "FechaOperacion": helpers.timehelper.now().replace(tzinfo=None).date().strftime("%d/%m/%y"),
                           "idAI":self.id_ArchivoInstancia, "servicio": "descarga"}]
            tn.enviar_notificacion(16, str(details), parametros, "",
                                   self.id_descarga, "", debug=debug)
            RegistroError = ts.SQLDownloadControl(HOST, USER, PASSWD, "carga")
            RegistroError.GuardarError(self.id_ArchivoInstancia)
            return False
        finally:
            ftp.quit()
        print(MENSAJE_DE_DESCARGA_EXITOSA.format(self.nombre_archivo))
        return self.nombre_archivo

    def sftp(self):
        try:
            print('Creando conexion con el servidor sftp')
            transport = paramiko.Transport(self.ip_sftp, self.puerto)
        except Exception as details:
            mensaje_de_error = 'Ocurrio un error en la creacion del socket con la ip: {}' \
                               'y el puerto {}\n' \
                               'Detalles del error: {}'.format(self.ip_sftp, self.puerto, details)
            print(mensaje_de_error)
            
            parametros = [{"FechaHora":helpers.timehelper.now().replace(tzinfo=None).strftime("%H:%M:%S"), 
                            "protocolo":"sftp","idAI":self.id_ArchivoInstancia, "servicio": "descarga"}]
            tn.enviar_notificacion(17, str(details), parametros, "",
                                   self.id_descarga, "", debug=debug)
            RegistroError = ts.SQLDownloadControl(HOST, USER, PASSWD, "carga")
            RegistroError.GuardarError(self.id_ArchivoInstancia)
            return False
        try:
            print('Conectando al sftp con el usuario: {} y contrasena: {}'.format(self.usuario, self.contrasena))
            transport.connect(username=self.usuario,
                              password=self.contrasena)
            sftp = paramiko.SFTPClient.from_transport(transport)
        except Exception as details:
            mensaje_de_error = 'Ocurrio un error al intentar conectarse al sftp con ' \
                               'la ip: {} y el puerto: {}\n' \
                               'Detalles del error: {}'.format(self.ip_sftp, self.puerto, details)
            print(mensaje_de_error)
            parametros = [{"protocolo": "sftp", "idAI":self.id_ArchivoInstancia, "servicio": "descarga"}]
            tn.enviar_notificacion(65, str(details), parametros, "",
                                   self.id_descarga, "", debug=debug)
            transport.close()
            RegistroError = ts.SQLDownloadControl(HOST, USER, PASSWD, "carga")
            RegistroError.GuardarError(self.id_ArchivoInstancia)
            return False
        try:
            print('Descargando el archivo')
            sftp.get(os.path.join(self.carpeta_origen, self.nombre_archivo),
                     os.path.join(self.directorio_local, self.nombre_archivo))
        except Exception as details:
            print("self.carpeta_origen", self.carpeta_origen, "self.nombre_archivo",
                  self.nombre_archivo, "self.directorio_local", self.directorio_local,
                  "self.nombre_archivo", self.nombre_archivo)

            mensaje_de_error = 'Ocurrio un error al intentar descargar el archivo ' \
                               'del servidor sftp con la ip {}\n' \
                               'Detalles del error: {}'.format(self.ip_sftp, details)
            print(mensaje_de_error)

            parametros = [{"protocolo": "sftp", "ip": self.ip_sftp, "NombreArchivo":self.nombre_archivo,
                           "idAI":self.id_ArchivoInstancia, "servicio": "descarga"}]
            tn.enviar_notificacion(18, str(details), parametros, "",
                                   self.id_descarga, "", debug=debug)
            RegistroError = ts.SQLDownloadControl(HOST, USER, PASSWD, "carga")
            RegistroError.GuardarError(self.id_ArchivoInstancia)
            return False
        finally:
            sftp.close()
            transport.close()
        print(MENSAJE_DE_DESCARGA_EXITOSA.format(self.nombre_archivo))
        return self.nombre_archivo

    def http(self):
        return self.https()

    def https(self):
        context = ssl._create_unverified_context()
        try:
            ruta_descarga, nombre_archivo = self.nombre_archivo.split('+_')
            self.direccion_web += ruta_descarga
            self.nombre_archivo = nombre_archivo
        except (ValueError, Exception) as details:
            logging.debug('El nombre de archivo no cuenta con la dirrecion de descarga\n'
                          f'Detalles: {details}')
            self.direccion_web += self.nombre_archivo
        try:
            archivo_local = os.path.join(self.directorio_local, self.nombre_archivo)
            with request.urlopen(self.direccion_web, context=context) as respuesta, open(archivo_local, 'wb') as \
                    archivo_de_salida:
                shutil.copyfileobj(respuesta, archivo_de_salida)
        except Exception as details:
            mensaje_de_error = 'Error al intentar descargar el archivo {} de la direccion {}\n' \
                               'Detalles de error: {}'.format(self.nombre_archivo, self.direccion_web, details)
            print(mensaje_de_error)

            parametros = [{"protocolo": "https", "ip": self.direccion_web,
                           "NombreArchivo":self.nombre_archivo, "idAI":self.id_ArchivoInstancia,
                           "servicio": "descarga"}]
            tn.enviar_notificacion(19, str(details), parametros, "",
                                   self.id_descarga, "", debug=debug)
            return False
        logging.info(MENSAJE_DE_DESCARGA_EXITOSA.format(self.nombre_archivo))
        return self.nombre_archivo

    def s3(self):
        try:
            taws.descarga_de_s3(self.bucket_descarga_s3, self.nombre_archivo, self.carpeta_origen,
                                self.directorio_local)
        except Exception as details:
            mensaje_de_error = 'Error al intentar descargar el archivo del s3 {} ' \
                               'carpeta {} nombre_archivo {}\n' \
                               'Detalles del error: {}'.format(self.bucket_descarga_s3,
                                                               self.carpeta_origen,
                                                               self.nombre_archivo,
                                                               details)
            print(mensaje_de_error)

            insBD = ts.HelpQuery(HOST, USER, PASSWD, "datamaster")
            proveedor = insBD.consultar_proveedor(self.id_proveedor)
            parametros = [{"path_S3": self.bucket_descarga_s3, "carpeta_origen": self.carpeta_origen,
                           "proveedor": proveedor, "idAI":self.id_ArchivoInstancia,
                           "servicio": "descarga"}]
            tn.enviar_notificacion(67, str(details), parametros, "",
                                   self.id_descarga, "", debug=debug)
            RegistroError = ts.SQLDownloadControl(HOST, USER, PASSWD, "carga")
            RegistroError.GuardarError(self.id_ArchivoInstancia)

    def script(self):
        cliente_wcf = tw.ClientWCFDatamaster(self.ip_wcf)
        if not cliente_wcf.client:
            tn.enviar_notificacion(22, cliente_wcf.mensaje_de_error, 'idDescarga={}'.format(self.id_descarga),
                                   id_descarga=self.id_descarga, debug=debug)
            print(cliente_wcf.mensaje_de_error)
            return False
        if not cliente_wcf.web_service_archivo_contrasena(self.nombre_archivo,
                                                          self.nombre_archivo,
                                                          self.fecha_modificacion_de_archivo,
                                                          self.id_interno,
                                                          self.id_interno,
                                                          self.ruta_script):
            print(cliente_wcf.mensaje_de_error)
            tn.enviar_notificacion(22, cliente_wcf.mensaje_de_error, 'idDescarga={}'.format(self.id_descarga),
                                   id_descarga=self.id_descarga, debug=debug)
            return False
        print(MENSAJE_DE_DESCARGA_EXITOSA.format(self.nombre_archivo))
        return self.nombre_archivo

    def derechos_bolsa(self):
        evento = {'idDescarga': self.id_descarga,
                  'idArchivo': self.id_archivo,
                  'folder_save': self.carpeta_destino}
        try:
            taws.call_lambda(self.lambda_derechos, evento)
        except Exception as details:
            mensaje_de_error = 'Ocurrio un error durante la invoacion de la lambda de descarga de ' \
                               'derechos \n' \
                               'Detalles de error: {}'.format(details)
            print(mensaje_de_error)
            tn.enviar_notificacion(1, mensaje_de_error, 'idDescarga={}'.format(self.id_descarga),
                                   id_descarga=self.id_descarga, debug=debug)
            return False
        return self.nombre_archivo

    def zip(self):
        count = len(self.archivos_internos)
        concatenacion_de_nombres_internos = ''
        for archivo in self.archivos_internos:
            concatenacion_de_nombres_internos += '{}-_{}--{}'.format("", archivo.id_interno, archivo.nombre_archivo)
        return concatenacion_de_nombres_internos

    def json(self):
        with open(os.path.join(self.directorio_local, self.nombre_archivo), 'rb') as json_file:
            json_data = json.loads(json_file.read())
        return len(json_data)

    def texto_plano(self):
        registros = 0
        with open(os.path.join(self.directorio_local, self.nombre_archivo), 'rb') as text_file:
            for registro in text_file:
                registros += 1
        return registros

    def borrarRegistroRepetido(self):
        inBD = ts.HelpQuery(HOST, USER, PASSWD, "carga")
        inBD.borrarRegistroRepetido(self.id_ArchivoInstancia)


def event2lowecase(evento):
    logger = logging.getLogger('event2lowecase')
    try:
        nuevo_evento = {campo.lower(): evento[campo] for campo in evento}
    except (Exception, AttributeError) as details:
        logger.error('Error al intentar pasar las llaves del evento a minuscula\n'
                     'Detalles: {}'.format(details))
        return None
    return nuevo_evento
