#Importación de librerias necesarias para conexión con Cassandra y gestión de fechas
import datetime

from cassandra.cluster import Cluster
from datetime import datetime as dt

#####
#Parte 2: Creación de tablas
#####

# Tabla1 Entidad Paciente: contine un "set" de Alergias (atributo de conjunto)
class Paciente:
    def __init__(self, paciente_dni, paciente_nombre, paciente_fecha_nac, paciente_direccion, paciente_tlf, paciente_alergias):
        self.paciente_dni = paciente_dni # Clustering Key
        self.paciente_nombre = paciente_nombre # Partition Key
        self.paciente_fecha_nac = paciente_fecha_nac
        self.paciente_direccion = paciente_direccion
        self.paciente_tlf = paciente_tlf
        self.paciente_alergias = paciente_alergias # Atributo de conjunto -> set

    def insert(self):
        query = """
        INSERT INTO Tabla1 (paciente_dni, paciente_nombre, paciente_fecha_nac, paciente_direccion, paciente_tlf, paciente_alergias)
        VALUES (?, ?, ? , ? , ?, ?)
        """
        #self.db.session.execute(query, (paciente_nombre, paciente_dni, paciente_fecha_nac, paciente_direccion, paciente_tlf, paciente_alergias))
        insertStatement = session.prepare (query)
        session.execute (insertStatement, [self.paciente_dni, self.paciente_nombre, self.paciente_fecha_nac, self.paciente_direccion, self.paciente_tlf, self.paciente_alergias])
    
        print("Paciente insertado.\n")

# Entiedad Medico
class Medico:
    def __init__(self, medico_dni, medico_nombre, medico_fecha_nac, medico_tlf, medico_especialidades):
        self.medico_dni = medico_dni # Clustering Key
        self.medico_nombre = medico_nombre # Partition Key
        self.medico_fecha_nac = medico_fecha_nac
        self.medico_tlf = medico_tlf
        self.medico_especialidades = medico_especialidades

    def insert(self):

        query = """
        INSERT INTO Medico (medico_nombre, medico_dni, medico_fecha_nac, medico_tlf, medico_especialidades)
        VALUES (?, ?, ?, ?, ?)
        """
        insertStatement = session.prepare (query)
        session.execute (insertStatement, (self.medico_nombre, self.medico_dni, self.medico_fecha_nac, self.medico_tlf, self.medico_especialidades))
        print("Médico insertado.\n")


# Tabla2 Contiene la consulta 2. Obtener según el DNI de un médico todas las citas que atiende.
class Medico_con_Citas:
    def __init__(self, medico_dni, cita_id, cita_fecha_hora, cita_motivo):
        self.medico_dni = medico_dni
        self.cita_id = cita_id
        self.cita_fecha_hora = cita_fecha_hora
        self.cita_motivo = cita_motivo

    def insert(self):

        query = """
        INSERT INTO Tabla2 (medico_dni, cita_id, cita_fecha_hora, cita_motivo)
        VALUES (?, ?, ?, ?)
        """
        insertStatement = session.prepare (query)
        session.execute (insertStatement, (self.medico_dni, self.cita_id, self.cita_fecha_hora, self.cita_motivo))
        print("Cita insertada.\n")

# Tabla 3: Obtener según el DNI de un paciente, todos los tratamientos que tiene incluidos en las citas que tuvo. 
class Tratamientos_por_Paciente:
    def __init__(self, paciente_dni, cita_id, tratamiento_id, tratamiento_descripcion, tratamiento_costo):
        self.paciente_dni = paciente_dni # Partition key
        self.cita_id = cita_id # Clustering Key
        self.tratamiento_id = tratamiento_id # Clustering Key
        self.tratamiento_descripcion = tratamiento_descripcion
        self.tratamiento_costo = tratamiento_costo
    
    def insert(self):
        query = """
        INSERT INTO Tabla3 (paciente_dni, cita_id, tratamiento_id, tratamiento_descripcion, tratamiento_costo)
        VALUES (?, ?, ?, ?, ?)
        """
        insertStatement = session.prepare (query)
        session.execute (insertStatement, (self.paciente_dni, self.cita_id, self.tratamiento_id, self.tratamiento_descripcion, self.tratamiento_costo))
        print("Tratamiento insertado para el paciente.\n")

# Tabla 4: Obtener cuantas citas tiene un paciente. 
class Paciente_num_citas:
    #Constructor con relación 1:n entre Cita y Tratamiento
    def __init__(self, paciente_dni, num_citas): 
        self.Paciente_DNI = paciente_dni # Clustering Key
        self.num_citas = num_citas # Counter
    
    def insert(self):

        query = """
        INSERT INTO Tabla4 (Paciente_DNI, num_citas)
        VALUES (?, ?)
        """
        insertStatement = session.prepare (query)
        session.execute (insertStatement, (self.paciente_dni, self.num_citas))
        print("Número de citas insertado para el paciente.\n")

# Tabla 5: Obtener todas las asociaciones entre recetas y medicamentos a través de la fecha de la receta.  
# Relación contiene
class Recetas_x_Medicamentos:
    #Constructor con relación 1:n entre Cita y Tratamiento
    def __init__(self, receta_fecha_emision, receta_id, medicamento_codigo, medicamento_nombre, medicamento_dosis): 
        self.receta_fecha_emision = receta_fecha_emision # Clustering Key
        self.receta_id = receta_id
        self.medicamento_codigo = medicamento_codigo
        self.medicamento_nombre = medicamento_nombre
        self.medicamento_dosis = medicamento_dosis

    def insert(self):
        query = """
            INSERT INTO Tabla5 (receta_fecha_emision, receta_id, medicamento_codigo, medicamento_nombre, medicamento_dosis)
            VALUES (?, ?, ?, ?, ?)
        """
        insertStatement = session.prepare (query)
        session.execute (insertStatement, (self.receta_fecha_emision, self.receta_id, self.medicamento_codigo, self.medicamento_nombre, self.medicamento_dosis))
        print("Receta insertada con éxito.\n")

# Tabla 6: Obtener los pacientes que tienen una alergia en concreto 
class Alergias_por_Paciente:
    def __init__(self, paciente_alergia, paciente_dni, paciente_nombre): 
        self.paciente_alergia = paciente_alergia # Clustering Key
        self.paciente_dni = paciente_dni
        self.paciente_nombre = paciente_nombre
    
    def insert(self):
        query = """
            INSERT INTO Tabla6 (paciente_alergia, paciente_dni, paciente_nombre)
            VALUES (?, ?, ?)
        """

        insertStatement = session.prepare (query)
        session.execute (insertStatement, (self.paciente_alergia, self.paciente_dni, self.paciente_nombre))

        print("Alergia insertada con éxito.\n")

#####
# Parte 3: Programa Python de gestión de datos
#####

## Métodos de insercion -- Parte 3
# Paciente en tabla de consulta 1, 6 y soporte

def insertarPaciente():

    #Pedimos al usuario del programa los datos del cliente
    dni = ""
    while not dni.isnumeric():
        dni = input ("Dame un dni: ")
    dni = int(dni)
    nombre = input ("Dame nombre del paciente: ")
    direccion = input ("Dame direccion del paciente: ")
    
    fecha_nac = ""
    while not isinstance(fecha_nac, datetime.datetime):
        fecha_nac = input("Fecha de nacimiento (YYYY-MM-DD): ")
        try :
            fecha_nac = dt.strptime(fecha_nac, "%Y-%m-%d") 
        except ValueError:
            print("Error de formato. Prueba de nuevo.")

    tlf = input ("Dame un telefono: ")
    alergias = set() #iniciamos la colección (set) que contendra las alergias a insertar
    alergia = input ("Introduzca una alergia, vacío para parar. ")
    while (alergia != ""):
        alergias.add(alergia)
        alergia = input("Introduzca una alergia, vacío para parar. ")

    p = Paciente (dni, nombre, fecha_nac, direccion, tlf, alergias)
    p.insert() # Insertamos el paciente
    
    #insertar en preferencias por cliente
    ap = Alergias_por_Paciente("", p.paciente_dni, p.paciente_nombre)
    for al in alergias:
        ap.paciente_alergia = al # modificamos la alergia y la insertamos
        ap.insert()


#insert medico
def insertarMedico():

    #Pedimos al usuario del programa los datos del cliente
    dni = ""
    while not dni.isnumeric():
        dni = input ("Dame un dni: ")
    dni = int(dni)
    nombre = input ("Dame nombre del medico: ")
    
    fecha_nac = ""
    while not isinstance(fecha_nac, datetime.datetime):
        fecha_nac = input("Fecha de nacimiento (YYYY-MM-DD): ")
        try :
            fecha_nac = dt.strptime(fecha_nac, "%Y-%m-%d") 
        except ValueError:
            print("Error de formato. Prueba de nuevo.")

    tlf = input ("Dame un telefono: ")
    especialidades = set() #iniciamos la colección (set) que contendra las alergias a insertar
    especialidad = input ("Introduzca una especialidad, vacío para parar. ")
    while (especialidad != ""):
        especialidades.add(especialidad)
        especialidad = input("Introduzca una especialidad, vacío para parar. ")

    m = Medico (dni, nombre, fecha_nac, tlf, especialidades)
    m.insert() # Insertamos el paciente
    
    #insertar en espcialidades
    '''em = Medico("", m.medico_dni, m.medico_nombre)
    for al in especialidades:
        em.paciente_alergia = al # modificamos la alergia y la insertamos
        em.insert()'''
    
# Relación tiene
def tienePacienteCita():
    
    dni = ""
    while not dni.isnumeric():
        dni = input ("Dame un dni de paciente: ")
    dni = int(dni)
    cita_id = ""
    while not cita_id.isnumeric():
        cita_id = input ("Dame un id de cita: ")
    cita_id = int(cita_id)

    query = """
            INSERT INTO SoportePaciente (paciente_dni, cita_id)
            VALUES (?, ?)
        """
    insertStatement = session.prepare (query)
    session.execute (insertStatement, ( dni, cita_id))
    print("Soporte paciente insertado con éxito.")
    
# Relacion contiene insert
def contieneRecetaMedicamento():
    receta_id = ""
    while not receta_id.isnumeric():
        receta_id = input ("Dame id de receta: ")
    receta_id = int(receta_id)
    
    fecha_emision = ""
    while not isinstance(fecha_emision, datetime.datetime):
        fecha_emision = input("Fecha emisión de la receta (YYYY-MM-DD): ")
        try :
            fecha_emision = dt.strptime(fecha_emision, "%Y-%m-%d") 
        except ValueError:
            print("Error de formato. Prueba de nuevo.")

    salir_menu = "a"
    while (salir_menu not in {"N","n",""} ):
        codigo_medicamento = ""
        while not codigo_medicamento.isnumeric():
            codigo_medicamento = input ("Indique el código del medicamento: ")
        codigo_medicamento = int(codigo_medicamento)
        nombre = input ("Dame el nombre del medicamento: ")
        dosis = input ("Dame la dosis del medicamento: ")

        m = Recetas_x_Medicamentos(fecha_emision, receta_id, codigo_medicamento, nombre, dosis)
        m.insert()

        salir_menu = 'a'
        while salir_menu not in {'S','N','s','n', ''}:
            salir_menu = input ("Indique S, si quiere añadir un medicamento y N si no quiere añadir más medicamentos o vacio. ")


#Programa principal

cluster = Cluster(['127.0.0.1'])
session = cluster.connect("israelbru")

#Sigue pidiendo operaciones hasta que se introduzca 0
opcion_menu = -1
while (opcion_menu != 0):
    print ("\nIntroduzca un número para ejecutar una de las siguientes operaciones:")
    print ("1. Insertar un paciente")
    print ("2. Insertar un medico")
    print ("3. Insertar relación Tiene, paciente dni y cita ")
    print ("4. Insertar relación Contiene, receta y medicamento")
    print ("0. Cerrar aplicación")

    opcion_menu = int (input()) #Pedimos numero al usuario
    if (opcion_menu == 1):
        insertarPaciente()
    elif (opcion_menu == 2):
        insertarMedico()
    elif (opcion_menu == 3):
        tienePacienteCita()
    elif (opcion_menu == 4):
        contieneRecetaMedicamento()
    elif (opcion_menu == 0):
        print ("Salimos del1 menu")
    else:
        print ("Número incorrecto")

cluster.shutdown() #cerramos conexion