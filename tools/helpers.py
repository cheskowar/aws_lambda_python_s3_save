import pytz
from datetime import datetime

class timehelper(object):

  @staticmethod
  def now():
    '''
    Regresa el tiempo actual en UTC-5, basandose del UTC.
    '''
    return datetime.utcnow().replace(tzinfo=pytz.UTC).astimezone(pytz.timezone('America/Mexico_City'))
  
  @staticmethod
  def today():
    '''
    Regresa la fecha actual en UTC-5, basandose del UTC.
    '''
    return datetime.utcnow().replace(tzinfo=pytz.UTC).astimezone(pytz.timezone('America/Mexico_City')).date()
 
  # @staticmethod
  # def to_posix(dt):
  #   return (dt - datetime(1970, 1, 1, tzinfo=pytz.UTC)).total_seconds()
 
  # @staticmethod
  # def from_posix(p):
  #   return datetime.fromtimestamp(p, pytz.)