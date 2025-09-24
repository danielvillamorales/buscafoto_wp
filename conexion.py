import cx_Oracle
from datetime import datetime

# cx_Oracle.init_oracle_client(lib_dir=r"/u/instantclient_21_6")


class conexion:

    def log(texto):
        with open("log.log", "a+") as f:
            f.read()
            f.write(str(datetime.now()) + ": " + texto + "\n")
            f.close()

    def con():
        conexion = None
        try:
            conexion = cx_Oracle.connect(
                user="logistica",
                password="kostazul",
                dsn="192.168.0.4:1521/xe",
                encoding="UTF-8",
            )
            # conexion =   cx_Oracle.connect(user='ka',password='K#0stazul',dsn='localhost:1521/xe',encoding='UTF-8')
            return conexion

        except Exception as e:
            log("ERROR AL ACTUALIZAR LOS DATOS: " + str(e))
        # finally:
        #    conexion.close()
