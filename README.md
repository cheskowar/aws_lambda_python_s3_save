# AWS Lambda python

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
