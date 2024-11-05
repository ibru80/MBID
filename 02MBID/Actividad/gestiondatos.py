#Importación de librerias necesarias para conexión con Cassandra y gestión de fechas
import datetime

from cassandra.cluster import Cluster
from datetime import date

cluster = Cluster(['127.0.0.1'])
session = cluster.connect("israelbru")

#Parte 1: Definición de clases de las entidades y relaciones
# Entidad Paciente: contine un "set" de Alergias (atributo de conjunto)
class Paciente:
    def __init__(self, DNI, Nombre, Fecha_Nac, Direccion, Tlf, Alergias):
        self.DNI = DNI # Clustering Key
        self.Nombre = Nombre # Partition Key
        self.Fecha_Nac = Fecha_Nac
        self.Direccion = Direccion
        self.Tlf = Tlf
        self.Alergias = Alergias # Atributo de conjunto -> set

class Medico:
    def __init__(self, DNI, Nombre, Fecha_Nac, Tlf, Especialidades):
        self.DNI = DNI # Clustering Key
        self.Nombre = Nombre # Partition Key
        self.Fecha_Nac = Fecha_Nac
        self.Tlf = Tlf
        self.Especialidades = Especialidades # Atributo de conjunto -> set

# Cita: 
class Cita:
    #Constructor con relación 1:n entre Cliente y Pedido
    def __init__(self, IdCita, Fecha_Hora, Motivo, DNI_Paciente, DNI_Medico):
        self.IdCita = IdCita # Clustering Key
        self.Fecha_Hora = Fecha_Hora # Partition Key
        self.Motivo = Motivo
        self.DNI_Paciente = DNI_Paciente # Clustering Key
        self.DNI_Medico = DNI_Medico # Clustering Key

# Contiene la consulta 2. Obtener según el DNI de un médico todas las citas que atiende.
class Tabla2:
    def __init__(self, Medico_DNI, Cita_ID, Cita_Fecha_Hora, Cita_Motivo): 
        self.Medico_DNI = Medico_DNI # Clustering Key
        self.Cita_ID = Cita_ID
        self.Cita_Fecha_Hora = Cita_Fecha_Hora
        self.Cita_Motivo = Cita_Motivo # Clustering Key

# Tratamiento: 
class Tratamiento:
    #Constructor con relación 1:n entre Cita y Tratamiento
    def __init__(self, IdTratamiento, descripcion, costo, idCita): 
        self.idTratamiento = IdTratamiento # Clustering Key
        self.descripcion = descripcion
        self.costo = costo
        self.idCita = idCita # Clustering Key

# Consultorio: 
class Consultorio:
    #Constructor con relación 1:n entre Cita y Tratamiento
    def __init__(self, numero_consultorio, hospital, ciudad, idCita): 
        self.numero_consultorio = numero_consultorio # Clustering Key
        self.hospital = hospital
        self.ciudad = ciudad
        self.idCita = idCita # Clustering Key

# Receta: 
class Receta:
    #Constructor con relación 1:n entre Receta, Paciente y Médico
    def __init__(self, idReceta, fecha_emision, idPaciente, idMedico): 
        self.idReceta = idReceta # Clustering Key
        self.fecha_emision = fecha_emision
        self.idPaciente = idPaciente # Clustering Key
        self.idMedico = idMedico # Clustering Key

# Medicamento: 
class Medicamento:
    #Constructor con relación 1:n entre Cita y Tratamiento
    def __init__(self, codigo_medicamento, nombre, dosis, idReceta): 
        self.codigo_medicamento = codigo_medicamento # Clustering Key
        self.nombre = nombre
        self.dosis = dosis
        self.idReceta = idReceta # Clustering Key

# Medicamento: relación n:m entre receta y medicamento
class RecetaMedicamento:
    def __init__ (self, idReceta, codigo_medicamento):
        self.idReceta = idReceta 
        self.codigo_medicamento = codigo_medicamento 

# Función para pedir datos de un cliente e insertarlos en la BBDD
def insertCliente ():
    #Pedimos al usuario del programa los datos del cliente
    nombre = input ("Dame nombre del cliente")
    direccion = input ("Dame direccion del cliente")
    dni = input ("Dame dni del cliente")
    id = input("Dame ID del cliente") #Esto puede ser sustituido por uuid automatico o extraer MAX (id)
    while not id.isnumeric():
        id = input("Dame ID del cliente")
    id =int (id)
    preferencias = set() #iniciamos la colección (set) que contendra las preferencias a insertar
    preferencia = input ("Introduzca una preferencia, vacío para parar")
    while (preferencia != ""):
        preferencias.add(preferencia)
        preferencia = input("Introduzca una preferencia, vacío para parar")

    c = Cliente (id, nombre, dni, direccion, preferencias)
    insertStatement = session.prepare ("INSERT INTO clientes_por_id (id, nombre, dni, direccion, preferencia) VALUES (?, ?, ?, ?, ?)")
    session.execute (insertStatement, [c.IdCliente, c.Nombre, c.DNI, c.Direccion, c.Preferencias])
    insertStatementPref = session.prepare ("INSERT INTO clientes_por_preferencia (preferencia, id, nombre, dni, direccion, preferencias) VALUES (?, ?, ?, ?, ?, ?)")

    #insertar en preferencias por cliente
    for pref in preferencias:
        session.execute(insertStatementPref, [pref, c.IdCliente, c.Nombre, c.DNI, c.Direccion, c.Preferencias])
    insertStatementSoporte5 = session.prepare(
        "INSERT INTO soporte5 (id, direccion) VALUES (?, ?)")
    session.execute(insertStatementSoporte5, [c.IdCliente, c.Direccion])


#Función para pedir los datos de un producto en insertarlos en la BBDD
def insertProducto ():
    #Pedimos al usuario del programa los datos del producto
    nombre = input ("Dame nombre del producto")
    precio = input("Dame precio del producto")
    while not precio.isnumeric():
        precio = input("Dame precio del producto")
    precio = float(precio)
    existencias = input ("Dame existencias del producto")
    while not existencias.isnumeric():
        existencias = input("Dame existencias del producto")
    existencias =int (existencias)
    id = input("Dame ID del producto") #Esto puede ser sustituido por uuid automatico o extraer MAX (id)
    while not id.isnumeric():
        id = input("Dame ID del producto")
    id =int (id)
    propiedad = input ("Introduzca una nueva propiedad")
    propiedades = set(propiedad.split(";"))
    print (propiedades);

    p = ProductoSinPropiedades (id, nombre, precio, existencias)
    insertStatement = session.prepare ("INSERT INTO productos (precio, idproducto, existencias, nombre) VALUES (?, ?, ?, ?)")
    session.execute (insertStatement, [p.Precio, p.IdProducto, p.Existencias, p.Nombre])
    insertStatement2 = session.prepare ("INSERT INTO ejercicio2 (propiedad, producto_id, propiedades) VALUES (?, ?, ?)")
    for propiedadIter in propiedades:
        session.execute (insertStatement2, [propiedadIter, p.IdProducto, propiedades])

	#session.execute (insertStatement, [precio, id, existencias, nombre]) #Sin utilizar la clase y constructor, directamente valores del input


#Función que pide todos los datos de un cliente que compra un producto en un pedido en concreto. Se suministran todos los valores de las entidades.
#Se podría optar por una solución en la que solo se provean los ids y el resto de información se busque en otras tablas
def insertClientePedidosProductos ():
    #Pedimos al usuario del programa los datos del producto
    nombre = input ("Dame nombre del cliente")
    precio = input("Dame precio del producto")
    while not precio.isnumeric():
        precio = input("Dame precio del producto")
    precio = float(precio)
    existencias = input("Dame existencias del producto")
    while not existencias.isnumeric():
        existencias = input("Dame existencias del producto")
    existencias = int(existencias)
    DNI = input ("Dame DNI del cliente")
    direccion = input ("Dame direccion del cliente")
    idCliente = input ("Dame ID del cliente")
    while not idCliente.isnumeric():
        idCliente = input("Dame ID del cliente")
    idCliente = int(idCliente)
    idProducto = input ("Dame ID del Producto")
    while not idProducto.isnumeric():
        idProducto = input("Dame ID del Producto")
    idProducto = int(idProducto)
    IdPedido = input ("Dame ID del Pedido")
    while not IdPedido.isnumeric():
        IdPedido = input("Dame ID del Pedido")
    IdPedido = int(IdPedido)

    hoy = date.today()

    insertStatementClientesPedido = session.prepare ("INSERT INTO Clientes_Pedidos (Pedido_Fecha, Pedido_IdPedidos, Pedido_Nombre, Cliente_DNI, Cliente_Direccion, Cliente_IdCliente) VALUES (?, ?, ?, ?, ?, ?)")
    insertStatementClientesProducto = session.prepare ("INSERT INTO Cliente_Producto (Cliente_Nombre, Cliente_DNI, Producto_IdProducto, Producto_Precio, Producto_Existencias) VALUES (?, ?, ?, ?, ?)")
    insertStatementNumPedidos = session.prepare ("UPDATE numpedidos SET NumPedidos = NumPedidos + 1 WHERE Pedido_Fecha = ?")
    insertEjercicio1 = session.prepare ("INSERT INTO ejercicio1 (id_pedido, id_producto, pedido_fecha) VALUES (?, ?, ?)")
    session.execute(insertStatementClientesPedido, [hoy, IdPedido, nombre, DNI, direccion, idCliente])
    session.execute(insertStatementClientesProducto, [nombre, DNI, idProducto, precio, existencias])
    session.execute(insertStatementNumPedidos, [hoy, ])
    insertStatement2 = session.prepare("INSERT INTO productos_por_id (id, precio,  existencias) VALUES (?, ?, ?)")
    session.execute(insertStatement2, [idProducto, precio, existencias])
    insertStatement3 = session.prepare ("INSERT INTO productos (precio, idproducto, existencias) VALUES (?, ?, ?)")
    session.execute(insertStatement3, [precio, idProducto, existencias])
    session.execute(insertEjercicio1, [IdPedido, idProducto, hoy])
    insertStatement4 = session.prepare ("INSERT INTO productos_cliente (producto_id, cliente_id) VALUES (?, ?)")
    session.execute(insertStatement3, [precio, idProducto, existencias])
    session.execute(insertStatement4, [idProducto, idCliente])
    insertStatement = session.prepare ("INSERT INTO clientes_por_id (id, nombre, dni) VALUES (?, ?, ?)")
    session.execute (insertStatement, [idCliente, nombre, DNI])
    insertStatementSoporte5 = session.prepare(
        "INSERT INTO soporte5 (id, direccion) VALUES (?, ?)")
    session.execute(insertStatementSoporte5, [idCliente, direccion])

#Misma función que la anterior pero en el caso del cliente solo se da la información del Id, buscándose el resto de la información en otras tablas.
def insertClientePedidosProductosSelectCliente ():
    #Pedimos al usuario del programa los datos del producto
    precio = input("Dame precio del producto")
    while not precio.isnumeric():
        precio = input("Dame precio del producto")
    precio = float(precio)
    existencias = input("Dame existencias del producto")
    while not existencias.isnumeric():
        existencias = input("Dame existencias del producto")
    existencias = int(precio)
    nombre = input ("Dame nombre del producto")
    idCliente = input("Dame ID del cliente")
    while not idCliente.isnumeric():
        idCliente = input("Dame ID del cliente")
    idCliente = int(idCliente)
    idProducto = input("Dame ID del Producto")
    while not idProducto.isnumeric():
        idProducto = input("Dame ID del Producto")
    idProducto = int(idProducto)
    IdPedido = input("Dame ID del Pedido")
    while not IdPedido.isnumeric():
        IdPedido = input("Dame ID del Pedido")
    IdPedido = int(IdPedido)
    hoy = date.today()
    cliente = extraerDatosCliente (idCliente)
    if (cliente != None):
        insertStatementClientesPedido = session.prepare(
            "INSERT INTO Clientes_Pedidos (Pedido_Fecha, Pedido_IdPedidos, Pedido_Nombre, Cliente_DNI, Cliente_Direccion, Cliente_IdCliente) VALUES (?, ?, ?, ?, ?, ?)")
        insertStatementClientesProducto = session.prepare(
            "INSERT INTO Cliente_Producto (Cliente_Nombre, Cliente_DNI, Producto_IdProducto, Producto_Precio, Producto_Existencias) VALUES (?, ?, ?, ?, ?)")
        insertStatementNumPedidos = session.prepare(
            "UPDATE numpedidos SET NumPedidos = NumPedidos + 1 WHERE Pedido_Fecha = ?")
        session.execute(insertStatementClientesPedido,
                        [hoy, IdPedido, nombre, cliente.DNI, cliente.Direccion, idCliente])
        session.execute(insertStatementClientesProducto,
                        [cliente.Nombre, cliente.DNI, idProducto, precio, existencias])
        session.execute(insertStatementNumPedidos, [hoy, ])
        insertStatement2 = session.prepare("INSERT INTO productos_por_id (id, precio,  existencias) VALUES (?, ?, ?)")
        session.execute(insertStatement2, [idProducto, precio, existencias])
        insertStatement3 = session.prepare("INSERT INTO productos (precio, idproducto, existencias) VALUES (?, ?, ?)")
        session.execute(insertStatement3, [precio, idProducto, existencias])
#Función que ejecuta un SELECT contra la base de datos y extrae la información de un cliente según su ID
def extraerDatosCliente(idCliente):
    select = session.prepare ("SELECT * FROM clientes_por_id WHERE id = ?") #solo va a devolver una filia pero lo tratamos como si fuesen varias
    filas = session.execute (select, [idCliente,])#Importante, aunque solo haya un valor a reemplazar en el preparedstatemente, hay que poner la ','
    for fila in filas:
        c = Cliente (idCliente, fila.nombre, fila.dni,  fila.direccion, fila.preferencia) #creamos instancia del cliente
        return c
#Función que ejecuta un SELECT contra la base de datos y extrae la información de los clientes que compraron un producto según su ID
def extraerClientesProducto (idProducto):
    select = session.prepare ("SELECT cliente_id FROM productos_cliente WHERE producto_id = ?") #solo va a devolver una filia pero lo tratamos como si fuesen varias
    idsClientes = [] #donde almacenaremos el retorno de la consulta
    filas = session.execute (select, [idProducto,]) #Importante, aunque solo haya un valor a reemplazar en el preparedstatemente, hay que poner la ','
    for fila in filas: #procesando todos los clientes que compraron ese producto
        idsClientes.append(fila.cliente_id) #como solo queremos los Ids, simplemente vamos añadiendo los valores
    return idsClientes
#Función que ejecuta un SELECT contra la base de datos y extrae la información de un producto según su ID
def extraerDatosProducto(idProducto):
    select = session.prepare ("SELECT * FROM productos_por_id WHERE id = ?") #solo va a devolver una filia pero lo tratamos como si fuesen varias
    filas = session.execute (select, [idProducto,])#Importante, aunque solo haya un valor a reemplazar en el preparedstatemente, hay que poner la ','
    for fila in filas:
        p = ProductoSinPropiedades (idProducto, fila.nombre, fila.precio,  fila.existencias)
        return p
# Función que ejecuta un SELECT contra la base de datos y extrae la información de los productos comprados por un cliente segun su DNI y nombre
def extraerProductosCompradosCliente(nombre, dni):
    select = session.prepare ("SELECT Producto_IdProducto, Producto_Precio, Producto_Existencias FROM cliente_producto WHERE cliente_nombre = ? and cliente_dni = ?") #solo va a devolver una filia pero lo tratamos como si fuesen varias
    filas = session.execute (select, [nombre, dni])
    productos = []
    for fila in filas:
        p = ProductoSinPropiedades (fila.producto_idproducto, "", fila.producto_precio,  fila.producto_existencias)
        productos.append(p)
    return productos
#Función que actualiza el precio de un producto
def actualizarPrecioProducto ():
    precio = input("Dame precio del producto")
    while not precio.isnumeric():
        precio = input("Dame precio del producto")
    precio = float(precio)
    idProducto = input("Dame ID del Producto")
    while not idProducto.isnumeric():
        idProducto = input("Dame ID del Producto")
    idProducto = int(idProducto)
    clientes = extraerClientesProducto(idProducto) #tenemos que saber que clientes han comprado ese producto para poder actualizar en la tabla cliente_producto
    updatePrecioClienteProducto = session.prepare ("UPDATE cliente_producto SET producto_precio = ? WHERE cliente_nombre = ? AND cliente_DNI = ? AND producto_idproducto=?")
    for clienteId in clientes: #por cada cliente ejecutamos un UPDATE
        infoCliente = extraerDatosCliente(clienteId) #infoCliente será una variable que almacena toda la información de un cliente
        session.execute(updatePrecioClienteProducto,[precio, infoCliente.Nombre, infoCliente.DNI, idProducto])
    infoProducto = extraerDatosProducto(idProducto)
    if (infoProducto != None): #Comprobar que el idproducto este introducido en la BBDD, si no lo está, no ejecutamos ninguna operación.
        borrarProducto = session.prepare ("DELETE FROM productos WHERE precio = ? AND idproducto = ?")
        session.execute(borrarProducto, [infoProducto.Precio,idProducto])
        insertStatement = session.prepare("INSERT INTO productos (precio, idproducto, existencias, nombre) VALUES (?, ?, ?, ?)")
        session.execute(insertStatement, [precio, idProducto, infoProducto.Existencias, infoProducto.Nombre])
        updateSoporte = session.prepare("UPDATE productos_por_id SET precio = ? WHERE id = ?")
        session.execute(updateSoporte, [precio, idProducto])
#Función que procesa la información de un cliente dado por el usuario y la muestra
def consultaClientePorId():
    id = input("Dame ID del cliente") #Esto puede ser sustituido por uuid automatico o extraer MAX (id)
    while not id.isnumeric():
        id = input("Dame ID del cliente")
    id =int (id)
    cliente = extraerDatosCliente (id)
    if (cliente != None): #si el cliente no existe no mostramos nada
        print ("DNI: ", cliente.DNI)
        print ("Nombre: ", cliente.Nombre)
        print ("Direccion: ", cliente.Direccion)

#Función que procesa la información de los productos comprados por un cliente
def consultaProductosCompradosCliente():
    dni = input ("Dame DNI del cliente")
    nombre = input ("Dame Nombre del cliente")
    productos = extraerProductosCompradosCliente(nombre, dni)
    for producto in productos:
        print ("Id: ", producto.IdProducto)
        print ("Existencias: ", producto.Existencias)
        print ("Precio: ", producto.Precio)

# Función que ejecuta un SELECT contra la base de datos y extrae la información dlos productos que tienen un determinado precio
def extraerProductos(precio):
    select = session.prepare("SELECT idproducto, nombre, existencias FROM productos WHERE precio = ?")  # solo va a devolver una filia pero lo tratamos como si fuesen varias
    filas = session.execute(select, [precio, ])
    productos = []
    for fila in filas:
        p = ProductoSinPropiedades(fila.idproducto, fila.nombre, precio, fila.existencias)
        productos.append(p)
    return productos
    pass

#Función que procesa la información de los productos que tienen un detrerminado precio
def consultaDatosProductosPrecio():
    precio = input("Introduzca precio de los productos a consultar") #Esto puede ser sustituido por uuid automatico o extraer MAX (id)
    while not precio.isnumeric():
        precio = input("Introduzca precio de los productos a consultar")
    precio =int (precio)
    productos = extraerProductos (precio)
    for producto in productos:
        print ("Id: ", producto.IdProducto)
        print ("Nombre: ", producto.Nombre)
        print ("Existencias: ", producto.Existencias)
        print ("Precio: ", producto.Precio)
#Hacer ejercicio 3
def borrarPedidoFecha (): #Ejercicio 3
    fecha = input ("Introduzca una fecha para borrar los pedidos en el formato YYYY-MM-DD")
    year, month, day = map (int, fecha.split('-'))
    date1 = datetime.date(year, month, day)
    deleteStatement = session.prepare ("DELETE FROM clientes_pedidos WHERE pedido_Fecha = ?")
    session.execute (deleteStatement, [date1, ])
    deleteStatement2 = session.prepare ("DELETE FROM numpedidos WHERE pedido_fecha = ?")
    session.execute (deleteStatement2, [date1, ])
#Hacer consulta ejercicio 4
def consultaEjercicio4 (direccion):
    selectejercicio4 = session.prepare ("SELECT * FROM soporte5 WHERE direccion = ?")
    filas = session.execute (selectejercicio4, [direccion, ])
    clientes = []
    for cliente in filas:
        id = cliente.id
        clientes.append (id)
    print (clientes)
    return clientes

#Hacer ejercicio 5
def actualizacionNombreCliente (): #Ejercicio 5
    nombre = input("Dame el nombre del cliente")
    direccion = input("Dame la dirección del cliente")
    ids = consultaEjercicio4(direccion)
    if (ids != None):
        for id in ids:
            updateClienteId = session.prepare("UPDATE clientes_por_id SET nombre = ? WHERE id = ?")
            session.execute(updateClienteId, [nombre, id])


#Programa principal
#Conexión con Cassandra
cluster = Cluster()
#cluster = Cluster(['192.168.0.1', '192.168.0.2'], port=..., ssl_context=...)
session = cluster.connect('practica2')
numero = -1
#Sigue pidiendo operaciones hasta que se introduzca 0
while (numero != 0):
    print ("Introduzca un número para ejecutar una de las siguientes operaciones:")
    print ("1. Insertar un cliente")
    print ("2. Insertar un producto")
    print ("3. Insertar relación entre cliente, producto y pedido (todos datos)")
    print ("4. Insertar relación entre cliente, producto y pedido (solo id cliente)")
    print ("5. Consultar datos cliente según su id")
    print ("6. Consultar datos de los productos comprados por un cliente dando DNI y nombre")
    print ("7. Consultar datos de los productos que tienen un precio dado")
    print ("8. Actualizar precio producto")
    print ("9. Borrar los pedidos de una fecha")
    print ("10. Consultar los clientes que tienen una direccion asignada")
    print ("11. Actualizar el nombre de clientes en base a su direccion")
    print ("0. Cerrar aplicación")

    numero = int (input()) #Pedimos numero al usuario
    if (numero == 1):
        insertCliente()
    elif (numero == 2):
        insertProducto()
    elif (numero == 3):
        insertClientePedidosProductos()
    elif (numero == 4):
        insertClientePedidosProductosSelectCliente()
    elif (numero == 5):
       consultaClientePorId()
    elif (numero == 6):
        consultaProductosCompradosCliente()
    elif (numero == 7):
        consultaDatosProductosPrecio()
    elif (numero == 8):
        actualizarPrecioProducto()
    elif (numero == 9):
        borrarPedidoFecha ()
    elif (numero == 10):
        consultaEjercicio4 (input ("Dame el nombre de una direccion"))
    elif (numero == 11):
        actualizacionNombreCliente()
    else:
        print ("Número incorrecto")



cluster.shutdown() #cerramos conexion