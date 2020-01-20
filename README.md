# Sercivio de Descargas

## Configuraciones previas

Antes de comenzar, ten en cuenta los siguientes puntos:

- Tener credenciales AWS de la cuenta de desarrollo.
- Tener instalado Python >= 3.6
- Tener instalado PIP >= 19.2.1
- Tener instado AWS CLI >= 1.16.189
- En caso de usar windows, tener instalado Botocore >= 1.12.179
- Tener instalado NodeJs >= 10.16.0
- Tener instalado NPM >= 6.9.0
- Tener instalado Serverless Framework >= 1.49.0
- De preferencia usar el IDE [Visual Studio Code](https://code.visualstudio.com/).

Si es la primera vez que se hara un deploy del servicio ten en cuenta lo siguiente:

- Tener un bucket creado para el deploy de la lambda.
- Tener una VPC configurada con sus respectivas subredes y grupos de seguridad.

### Prepara ambiente de desarrollo

Instala _virtualenv_ para crear ambientes virtuales.

    pip install virtualenv

Muevete un nivel arriba de la carpeta root de la aplicación, crea el ambiente virtual.

    virtualenv -p python <nombre_virtual_environment>

Activa el ambiente virtual.

    . ./<nombre_virtual_environment>/Scripts/Activate

## Debug Lambda

Instalación de las librerias de la aplicación, definidas en requirements.txt.

    pip install -r requirements.txt

### Lambda de descarga

Esta lambda recibe un evento (json) enviado por el servicio demonio, en dónde viene toda la información del archivo que se desea descargar, dicho evento tiene la siguiente forma:

Ej. Para VALMER

```JSON
{
    "carpetaOrigen": "Definitivo",
    "intervalo": "10",
    "ultimaRevision": "2019-05-16 17:45:00.007",
    "fechaModificacionArchivo": "2018-08-13 18:25:34.840",
    "numeroCargas": 3,
    "idInterno": 1,
    "idProveedor": 1,
    "horaNotificacion": "",
    "contrasena": "xxxxxxxxxxxx",
    "carpetaS3": "Validacion",
    "protocolo": "sftp",
    "nombreArchivo": "2420190926.zip",
    "horariosNotificacion": "2018-03-07 18:10:00.000",
    "fechaUltimaDescarga": "2019-08-09 16:16:14.900",
    "idLayout": 271,
    "extension": "ZIP",
    "usuario": "covaf",
    "tiempoCargaArchivo": "",
    "rutaDescarga": "sftp://xxxx.xx.xxx.xx",
    "archivosInternos": [],
    "rutaScript": "",
    "parametros": "",
    "idArchivo": 6,
    "horaInicio": "08:55:00.000",
    "id_archivo_instancia"
}
```
