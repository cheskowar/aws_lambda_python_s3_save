#!/usr/bin/python3
# -*- encoding:utf-8 -*-

import os
import json
import logging
import boto3

__author__ = 'Carlos Añorve/Omar Reyes/Benjamin N'
__version__ = '1.1'

logger = logging.getLogger('tools_aws')


def call_lambda(arn, event, type_invoke='Event',
                aws_access_key_id=None, aws_secret_access_key=None,
                region_name=None):
    """Call any lambda for testing.

    Use this function to test the behavior of the
    lambda function.
    Rememeber that you should have saved the configuration and
    the aws_access_key_id and the aws_secret_access_key in some
    configuration file in .aws path

    Args:
        arn (str): This is the Amazon resource Name (ARM) of your Lambda Function.
        event (dict): This is the param pased to the lambda usually a json object.
        type_invoke (str):
        aws_access_key_id (str):
        aws_secret_access_key (str):
        region_name (str):

    Returns:
        Anything that return the lambda function.
    """
    try:
        print('Try create client lambda.')
        if aws_access_key_id and aws_secret_access_key and region_name:
            client = boto3.client('lambda',
                                  aws_access_key_id=aws_access_key_id,
                                  aws_secret_access_key=aws_secret_access_key,
                                  region_name=region_name)
        elif aws_access_key_id and aws_secret_access_key:
            client = boto3.client('lambda',
                                  aws_access_key_id=aws_access_key_id,
                                  aws_secret_access_key=aws_secret_access_key)
        else:
            client = boto3.client('lambda')
    except Exception as details:
        print('Error to try make client(\'lambda\')\n'
                       'Details: {}'.format(details))
        return None
    # noinspection PyBroadException
    try:
        result = client.invoke(FunctionName=arn,
                               InvocationType=type_invoke,
                               Payload=json.dumps(event))

    except Exception as details:
        print(f'Error to try invoke the lambda function.\n'
                     f'Detalles: {details}')
        return None
    print('Function call_lambda finish successful.')
    return result['Payload'].read()


def descarga_de_s3(bucket, nombre_archivo, carpeta_s3=None, path_save_file='/tmp/', nuevo_nombre=None,
                   aws_access_key_id=None, aws_secret_access_key=None,
                   region_name=None):
    try:
        print('Intentando crear la instancia de s3 para descargar el archivo.')
        if aws_access_key_id and aws_secret_access_key and region_name:
            s3 = boto3.resource('s3',
                                aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key,
                                region_name=region_name)
        elif aws_access_key_id and aws_secret_access_key:
            s3 = boto3.resource('s3',
                                aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key)
        else:
            s3 = boto3.resource('s3')
    except Exception as details:
        print('Error to try connect to s3\nDetails: {}'.format(details))
        return None

    if carpeta_s3 is None:
        path_s3_file = nombre_archivo
    else:
        path_s3_file = f'{carpeta_s3}/{nombre_archivo}'
        # path_s3_file = os.path.join(carpeta_s3, nombre_archivo)
    # path_save = '{}{}'.format(path_save, os.path.split(nombre_archivo)[-1])
    if nuevo_nombre is None:
        path_save_file = os.path.join(path_save_file, nombre_archivo)
    else:
        path_save_file = os.path.join(path_save_file, nuevo_nombre)
    try:
        s3.Bucket(bucket).download_file(path_s3_file, path_save_file)
    except Exception as details:
        print('Error al intentar decargar el archivo de {} a {}\n'
                       'Detalles de error: {}'.format(path_s3_file, path_save_file, details))
        raise ErrorDescargaS3(details)
    else:
        print('Descarga exitosa.')


def subir_a_s3(bucket, nombre_archivo, idArchivoInstancia=None, idDescarga=None, path_save='/tmp/',
               carpeta_s3=None, metadatas=None, nuevo_nombre=None,
               aws_access_key_id=None, aws_secret_access_key=None,
               region_name=None):
    try:
        print('Intentando crear la instancia de s3 para cargar el archivo.')
        if aws_access_key_id and aws_secret_access_key and region_name:
            s3 = boto3.resource('s3',
                                aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key,
                                region_name=region_name)
        elif aws_access_key_id and aws_secret_access_key:
            s3 = boto3.resource('s3',
                                aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key)
        else:
            s3 = boto3.resource('s3')
    except Exception as details:
        print('Error al intetnar hacer la instancia del s3\n'
                       'Detalles: {}'.format(details))
        return None
    if nuevo_nombre is None:
        nuevo_nombre = nombre_archivo
    if carpeta_s3 is None:
        path_s3 = nuevo_nombre
    else:
        path_s3 = '{}/{}'.format(carpeta_s3, nuevo_nombre)
    path_save = '{}{}'.format(path_save, nombre_archivo)
    try:
        if metadatas is None:
            s3.Bucket(bucket).put_object(
                Bucket=bucket,
                Key=path_s3,
                Body=open(path_save, 'rb')
            )
            # s3.Bucket(bucket).upload_file(path_save, path_s3)
        else:
            s3.Bucket(bucket).put_object(
                Bucket=bucket,
                Key=path_s3,
                Body=open(path_save, 'rb'),
                Tagging=f'idArchivoInstancia={idArchivoInstancia}',# &idDescarga={idDescarga}',
                Metadata=metadatas
            )
            # s3.Bucket(bucket).upload_file(path_save, path_s3, ExtraArgs={'Metadata': metadatas})
    except Exception as details:
        print('Error al intentar subir el archivo {} a {}\n'
                       'Detalles de error: {}'.format(path_save, path_s3, details))
        raise ErrorCargaS3(details)
    else:
        print('Se cargo correctamente el archivo.')


class ErrorDescargaS3(Exception):
    def __init__(self, message):
        super(Exception, self).__init__(message)


class ErrorCargaS3(Exception):
    def __init__(self, message):
        super(Exception, self).__init__(message)
