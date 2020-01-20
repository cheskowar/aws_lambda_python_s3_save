#!/usr/bin/python3
# -*- encoding:utf-8 -*-

import logging
import decimal
import pytz
from datetime import datetime, timedelta, date

import _mssql

from . import tools_notificaciones as tn
from . import helpers

__author__ = 'Carlos Añorve/Omar Reyes/Benjamin N'
__version__ = '1.1'

debug = False


class SQLManagerConnections:
    def __init__(self, host, user, password):
        super(SQLManagerConnections, self).__init__()
        self.__logger = logging.getLogger('SQL')
        self.__host = host
        self.__user = user
        self.__password = password
        self.conexion = None
        self.__max_intentos_para_cerrar_conexiones = 5
        self.__intentos_para_cerrar_conexiones = 0

    def conectar(self, database):
        try:
            print('Conectandose a la base de datos: {}'.format(database))
            self.conexion = _mssql.connect(server=self.__host,
                                           user=self.__user,
                                           password=self.__password,
                                           database=database)
        except _mssql.MssqlDatabaseException as error:
            print('Error al intentar hacer la conexion.'
                                ' Error: {}'.format(error))
            self.conexion = None
            return False
        else:
            print('Conectado a la base de datos: {}'.format(database))

    def verificar_conexiones_abiertas(self):
        if self.__max_intentos_para_cerrar_conexiones > self.__intentos_para_cerrar_conexiones:
            if self.conexion.connected:
                print('La conexion se quedo abierta.'
                                      ' Intentando cerrar...')
                self.conexion.close()
                self.__intentos_para_cerrar_conexiones += 1
                self.verificar_conexiones_abiertas()
            else:
                print('No hay conexiones abieras.')
                self.__intentos_para_cerrar_conexiones = 0
        else:
            print('No fue posible cerrar la conexion despues de {} intentos'
                                  ' intenta matando la conexion desde el sistema.'
                                  ''.format(self.__intentos_para_cerrar_conexiones))
            self.__intentos_para_cerrar_conexiones = 0


class SQLDownloadControl(SQLManagerConnections, object):
    def __init__(self, host, user, password, database):
        super().__init__(host, user, password)
        self.__logger = logging.getLogger('SQLControlDescarga')
        self.__id_descarga = None
        self.__databse = database
        self.__fechaInicio = None

    def __manipular_nuevo_registro(self, databse, query):
        self.conectar(databse)
        if not (self.conexion is None):
            try:
                self.conexion.execute_query('{}'.format(query))
            except Exception as details:
                print(f'Problemas en la ejecucion de la consulta\n'
                                      f'Detalles: {details}')
            resultado = self.__depurar_consulta()
            self.conexion.close()
            self.verificar_conexiones_abiertas()
            return resultado

    @property
    def id_descarga(self):
        return self.__id_descarga
    @property
    def fechainicio(self):
        return self.__fechaInicio

    @id_descarga.setter
    def id_descarga(self, value):
        if isinstance(value, str):
            try:
                self.__id_descarga = int(value)
            except Exception as details:
                print('El id descarga necesita ser un entero\n'
                                    'Detalles: {}'.format(details))
                raise
        elif not isinstance(value, int):
            print('El id descarga no debe ser una tupla o lista')
            raise TypeError
        else:
            self.__id_descarga = value

    def GuardarError(self, idAI):
        try:
            self.__manipular_nuevo_registro('carga',
                                            """UPDATE 
            TB_ControlDescarga set  estado='Error', fechaFin=GETDATE() where idArchivoIns='%i' and idDes = 
            (select top(1) idDes from TB_ControlDescarga where idArchivoIns='%i' 
            order by idDes desc) and fechaInicio = '%s'"""%(int(idAI), int(idAI),self.fechainicio))

        except Exception as error:
            print("Error al consultar la base de datos para registrar el error en TB_ControlDescarga", error)
        return

    def start(self, idArchivoIns, fechaM, estado, tamanoMB, hash, database, tipodesc):
        fechaM = fechaM[0:19].replace("T", " ")
        #FIXME Validar que se inserten en horario CDMX. Se valido qeu se encuentra en horario CDMX
        fechaM_time_utc=datetime.strptime(fechaM, '%Y/%m/%d %H:%M:%S').replace(tzinfo=pytz.UTC).astimezone(pytz.timezone('America/Mexico_City'))
        #FIXME el parseo sobre las fechas modificacion origen no es necesario sobre los archivos de piplatam
        fechaM=fechaM_time_utc.strftime('%Y/%m/%d %H:%M:%S')
        today = datetime.utcnow().replace(tzinfo=pytz.UTC).astimezone(pytz.timezone('America/Mexico_City')).date()
        #FIXME Unificar idDes y estatus en una unica consulta y validar que la consulta sea correcta.
        Status = self.__manipular_nuevo_registro(database,
                """     select top(1) estado, idDes as ValorMayor
                FROM carga.dbo.TB_ControlDescarga 
                where idArchivoIns= '%i' and fechaInicio >= '%s' 
                ORDER BY idDes desc, fechaInicio desc """ % (idArchivoIns, today))
        print("Respuesta de la nueva consulta:",Status)

        # idDes = self.__manipular_nuevo_registro(database,
        #      """select max(idDes) as ValorMayor FROM TB_ControlDescarga 
        #      where idArchivoIns='%i' and fechaInicio >= '%s' """ % (idArchivoIns, today))
        # estatus = self.__manipular_nuevo_registro(database,
        #   """select top 1 estado FROM TB_ControlDescarga where idArchivoIns='%i' 
        #    and fechaInicio >= '%s' ORDER BY idDes desc""" % (idArchivoIns, today))
        # print("Este es el idDes:",idDes)
        # print("Este es el estatus", estatus)

        
        try:
            if Status[0]["estado"] == 'Error' or Status[0]["estado"] == 'En descarga':
                idDes = Status[0]["ValorMayor"]
                print("Ultimo estado=Error/En descarga, idDescarga:", idDes)
            else:
                idDes = Status[0]["ValorMayor"] + 1
                print("Se inicia nueva descarga, idDescarga:", idDes)
        except:
            idDes = 1
            print("Primer registro, idDescarga:", idDes)

        try:
            fechaIni = helpers.timehelper.now().replace(tzinfo = None)
            self.__fechaInicio = fechaIni.strftime('%Y-%m-%d %H:%M:%S')
            microsS = str(int(round(fechaIni.microsecond/1000,0))).zfill(3)
            self.__fechaInicio += '.' + microsS
            print('Registrando primer estado de la descarga.')
            self.__manipular_nuevo_registro(database,
            """INSERT TB_ControlDescarga(idArchivoIns, idDes, fechaInicio, fechaFin, 
            modificacionOrigen, estado, tamanoMB, hash, tipoDesc) 
            VALUES ('%i', '%i', '%s','','%s','En descarga',Null, Null,'%s')
            """ % (idArchivoIns, idDes, self.__fechaInicio ,fechaM, tipodesc))
        except Exception as details:
            print('Error al registrar el primer registro en BD.\n'
                                'Detalles: {}'.format(details))
            parametros = [{"idAI": idArchivoIns, "servicio": "descarga", "idArchivoIns": idArchivoIns}]
            tn.enviar_notificacion(14, str(details), parametros, "",
                                   "", 5, debug=debug)
            return 
        self.conexion.close()
        self.verificar_conexiones_abiertas()
        return idDes

    def end(self, idArchivoIns, idDes, estado="Descargado", tamanoMB=0.0, hash_='Null'):
        today = datetime.utcnow().replace(tzinfo=pytz.UTC).astimezone(pytz.timezone('America/Mexico_City')).date()
        print("Finalizar guardado en bases de datos de carga, en TB_ControlDescarga y TB_ArchivoInstancias")
        try:
            # Actualiza en ControlDescarga
            self.__manipular_nuevo_registro('carga',
            """update TB_ControlDescarga
            set fechaFin = GETDATE(), estado = '%s', tamanoMB = '%f', hash = '%s'
            where idArchivoIns = '%i' and idDes = '%i' and fechaInicio = '%s' """ % (estado, tamanoMB, hash_, idArchivoIns, idDes,self.fechainicio))

            # Actualiza en ArchivosInstancias
            self.__manipular_nuevo_registro('carga',
            """update TB_ArchivosInstancias
                set status = '%s'
            where id = '%i' """ % (estado, idArchivoIns))
        except Exception as details:
            print('Error al guardar en la base de datos .\n'
                  'Detalles: {}'.format(details))
            parametros = [{"idAI": idArchivoIns, "servicio": "descarga", "idArchivoIns": idArchivoIns}]
            tn.enviar_notificacion(14, str(details), parametros, "",
                                   "", 5, debug=debug)
            return
        self.conexion.close()
        self.verificar_conexiones_abiertas()
        return None

    def spp_control_validaciones(self, id_archivo, id_descarga, promedio_mb, horario_promedio,
                                 tamanio_mb, fecha_archivo, database=None):
        if database is None:
            self.conectar(self.__databse)
        else:
            self.conectar(database)
        if not (self.conexion is None):
            try:
                print('Instanciando datos en el spp')
                spp_control_validacion_promedios = self.conexion.init_procedure('dbo.SPP_ControlValidacionPromedios')
                spp_control_validacion_promedios.bind(id_archivo, _mssql.SQLINT8)
                spp_control_validacion_promedios.bind(id_descarga, _mssql.SQLINT8)
                spp_control_validacion_promedios.bind(promedio_mb, _mssql.SQLFLT8)
                spp_control_validacion_promedios.bind(horario_promedio, _mssql.SQLVARCHAR)
                spp_control_validacion_promedios.bind(tamanio_mb, _mssql.SQLFLT8)
                spp_control_validacion_promedios.bind(fecha_archivo, _mssql.SQLVARCHAR)
            except Exception as details:
                mensaje_de_error = 'Error al intentar instanciar el SPP_ControlValidacionPromedios\n' \
                                   'Detalles: {} '.format(details)
                print(mensaje_de_error)
                tn.enviar_notificacion('', mensaje_de_error, id_descarga=id_descarga, debug=debug)
                self.verificar_conexiones_abiertas()
                return False
            try:
                spp_control_validacion_promedios.execute()
            except Exception as details:
                mensaje_de_error = 'Error al intentar ejecutar el SPP_ControlValidacionPromedios\n' \
                                   'Detalles de error {}'.format(details)
                print(mensaje_de_error)
                tn.enviar_notificacion('', mensaje_de_error, id_descarga, debug=debug)
                self.verificar_conexiones_abiertas()
                return False
            self.conexion.close()
            self.verificar_conexiones_abiertas()

    def spq_webservice(self, usuario):
        pass


    def __depurar_consulta(self):
        try:
            resultado = []
            for reg in self.conexion:
                registro = []
                for llave in list(reg.keys()):
                    if type(llave) is str:
                        if type(reg[llave]) is decimal.Decimal:
                            if not reg[llave].is_nan():
                                try:
                                    reg[llave] = int(reg[llave])
                                except ValueError:
                                    reg[llave] = float(reg[llave])
                        registro.append((llave, reg[llave]))
                resultado.append(dict(registro))
        except Exception as details:
            print('Error al intentar depurar la consulta.\n'
                                'Detalles: {}'.format(details))
            self.verificar_conexiones_abiertas()

        else:
            print(resultado)
            return resultado


class HelpQuery(SQLManagerConnections):
    def __init__(self, host, user, passwd, database):
        super().__init__(host, user, passwd)
        self.__logger = logging.getLogger('Helper Query')
        self.__database = database

    def nombre_archivo(self, database=None, id_archivo=None, id_descarga=None, id_carga=None):
        if database is None:
            database = self.__database
        if not id_archivo:
            if id_carga:
                tabla_plus_condition = f'''TB_ControlCarga WHERE id = {id_carga }'''
                id_archivo = self.__consultar_a_bd(database, tabla_plus_condition, 'idArchivo')[0]['idArchivo']
            elif id_descarga:
                tabla_plus_condition = f'''TB_ControlDescarga WHERE id = {id_descarga }'''
                id_archivo = self.__consultar_a_bd(database, tabla_plus_condition, 'idArchivo')[0]['idArchivo']
        if id_archivo:
            nombre_archivo = self.__consultar_a_bd(database, f'TB_Archivo WHERE id = {id_archivo}', 'nombreArchivo')
            try:
                nombre = nombre_archivo[0]['nombreArchivo']
                return nombre
            except IndexError:
                return ''
        else:
            return ''

    def validarControlDescarga(self, id_AI, ArcHash):
        qry = """select top(1) hash from TB_ControlDescarga where idArchivoIns= '%i' and 
        estado='Descargado' order by fechaFin desc """ % (id_AI)
        try:
            result = self.__EjecutarQry("carga", qry)[0]['hash']
            if result==ArcHash:
                return True
            elif result!=ArcHash:
                return False
        except Exception as error:
            return ""


    def spq_webservice(self, id_service, database=None):
        if database is None:
            database = self.__database
        tabla_plus_condition = f'''tb_WebService WHERE id = {id_service}'''
        datos = self.__consultar_a_bd(database, tabla_plus_condition, 'direccion, parametros, Servicio')
        return datos

    def archivos_internos(self, database=None, id_archivo=None):
        if database is None:
            database = self.__database
        try:
            result = self.__consultar_a_bd(database, f'TB_ArchivoInterno WHERE id = {id_archivo}', 'nombreArchivo')
        except Exception as details:
            print('Error al consultar los archivos internos del archivo con id {}\n'
                                  'Detalles: {}'.format(id_archivo, details))
            result = None
        return result

    def validar_promedios(self, id_archivo, database=None):
        desface = timedelta(hours=6)
        if database is None:
            database = self.__database
        try:
            result = self.__consultar_a_bd(database, f'TB_Archivo WHERE id = {id_archivo}',
                                           'fechaUltimaDescarga')[0]['fechaUltimaDescarga']
            hoy = datetime.utcnow() - desface
            print(result.date(), hoy.date())
            result = result.date() < hoy.date()
        except Exception as details:
            print('Error al consultar la fecha ultima descarga\n'
                                  'Detalles: {}'.format(details))
            result = True
        return result

    def EstadoDescargaArchivo(self, idAI):
        try:
            qry="select status from TB_ArchivosInstancias where id='%i'"%(idAI)
            status = self.__EjecutarQry("carga", qry)[0]["status"]
        except Exception as error:
            print("No se pudo consultar a la base de datos para extrael es estatus de la descarga", error)
            return "Pendiente de descarga"
        else:
            return status

    def calcular_hora_InicioFin(self, id):
        try:
            qry=""" select format(horaInicio, 'HH:mm:ss') horaInicio, 
            format(horaNotificacion, 'HH:mm:ss') horaFin FROM TB_Archivo where id='%i' """ % (id)
            h_InicioFin = self.__EjecutarQry("carga", qry)[0]
        except Exception as error:
            print("No fue posible acceder a la base de datos de carga, para consultar horas en TB_Archivo")
            return ""
        else:
            return h_InicioFin

    def porcentaje_tolerancia_peso(self, id):
        try:
            qry = """select ToleranciaTamanoArchivoPorcentaje 
                     from TB_Archivo where id='%i' """ % (id)
            porcentaje = self.__EjecutarQry("carga", qry)[0]["ToleranciaTamanoArchivoPorcentaje"]
        except Exception as details:
            print('Error al consultar la el peso del archivo en la tabla TB_Archivo\n'
                  'Detalles: {}'.format(details))
            porcentaje = False
        return porcentaje

    def consultar_proveedor(self, id):
        try:
            result = self.__consultar_a_bd("datamaster", "CAT_Proveedor where idProveedor={}".format(id), "nombre")[0]["nombre"]
        except Exception as details:
            print('Error al consultar la fecha ultima descarga\n'
                  'Detalles: {}'.format(details))
            result = True
        return result

    def borrarRegistroRepetido(self, idAI):
        try:
            qry = """DELETE TB_ControlDescarga where idArchivoIns='%i'  
            and idDes=(SELECT max(idDes) from TB_ControlDescarga WHERE idArchivoIns='%i' and 
            estado='En descarga')""" % (idAI, idAI)
            result = self.__EjecutarQry("carga", qry)
            result=True
        except Exception as details:
            print('Detalles del error: {}'.format(details))
            result = False
        return result

    def consultar_AliasArchivo(self, id):
        try:
            result = self.__consultar_a_bd("carga", "TB_Archivo where id={}".format(id), "alias")[0]["alias"]
        except Exception as details:
            print('Error al consultar la fecha ultima descarga\n'
                  'Detalles: {}'.format(details))
            result = True
        return result

    def __consultar_a_bd(self, databse, tabla, campos='*'):
        self.conectar(databse)
        if not (self.conexion is None):
            # noinspection PyBroadException
            try:
                self.conexion.execute_query('SELECT {} '
                                            'FROM {}'.format(campos, tabla))
            except Exception as details:
                print(f'Problemas en la ejecucion de la consulta\n'
                                      f'Detalles: {details}')
            resultado = self.__depurar_consulta()
            self.conexion.close()
            self.verificar_conexiones_abiertas()
            return resultado

    def __EjecutarQry(self, databse, qry):
        self.conectar(databse)
        if not (self.conexion is None):
            try:
                self.conexion.execute_query('{}'.format(qry))
            except Exception as details:
                print(f'Problemas en la ejecucion de la consulta\n'
                      f'Detalles: {details}')
            resultado = self.__depurar_consulta()
            self.conexion.close()
            self.verificar_conexiones_abiertas()
            return resultado

    def __depurar_consulta(self):
        try:
            resultado = []
            for reg in self.conexion:
                registro = []
                for llave in list(reg.keys()):
                    if type(llave) is str:
                        if type(reg[llave]) is decimal.Decimal:
                            if not reg[llave].is_nan():
                                try:
                                    reg[llave] = int(reg[llave])
                                except ValueError:
                                    reg[llave] = float(reg[llave])
                        registro.append((llave, reg[llave]))
                resultado.append(dict(registro))
        except Exception as details:
            print('Error al intentar depurar la consulta.\n'
                                'Detalles: {}'.format(details))
            self.verificar_conexiones_abiertas()

        else:
            print(resultado)
            return resultado

    def func_peso_promedio(self, database, id_archivo):
        self.conectar(database)

        if not (self.conexion is None):
            try:
                return self.conexion.execute_scalar("SELECT datamaster.dbo.PesoPromedio({}) AS peso_promedio".format(id_archivo))
            except Exception as error:
                print("Error al calcular peso promedio")
                self.verificar_conexiones_abiertas()
                raise ErrorToInstanceSP(error)


class ErrorToInstanceSP(Exception):
    def __init__(self, message):
        super(Exception, self).__init__(message)


class ErrorToExecuteSP(Exception):
    def __init__(self, message):
        super(Exception, self).__init__(message)
