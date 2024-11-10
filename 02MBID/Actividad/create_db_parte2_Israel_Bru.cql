-- Create a keyspace
CREATE KEYSPACE IF NOT EXISTS israelbru
WITH  REPLICATION = {
	'class' : 'SimpleStrategy',
	'replication_factor' : 1
};

-- CREAMOS LAS TABLAS DE CONSULTA (parte 1)

-- Create a table1 -> Paciente
CREATE TABLE IF NOT EXISTS israelbru.tabla1  (
	paciente_dni int,
	paciente_nombre text,
	paciente_fecha_nac date,
    paciente_direccion text,
    paciente_tlf text,
    paciente_alergias set<text>,
    PRIMARY KEY (paciente_nombre, paciente_dni)
);

-- Create a table2 -> Cita por medico_dni
-- Relacion tiene
CREATE TABLE IF NOT EXISTS israelbru.tabla2  (
	medico_dni int,
    cita_id int,
	cita_fecha_hora date,
    cita_motivo text,
    PRIMARY KEY (medico_dni, cita_id)
);

-- Create a table3 -> tratamientos por paciente_dni 
CREATE TABLE IF NOT EXISTS israelbru.tabla3  (
	paciente_dni int,
	cita_id int,
    tratamiento_id int,
	tratamiento_descripcion text,
    tratamiento_costo text,
    PRIMARY KEY (paciente_dni, cita_id, tratamiento_id)
) ;

-- Create a table4 -> cuantas citas tiene un paciente
CREATE TABLE IF NOT EXISTS israelbru.tabla4  (
	paciente_dni int,
	num_citas_paciente counter,
    PRIMARY KEY (paciente_dni)
);

-- Create a table5 -> todas las asociaciones entre recetas y medicamentos
-- Relaión contiene incluida
CREATE TABLE IF NOT EXISTS israelbru.tabla5  (
    receta_id int,
    receta_fecha_emision date,
    medicamento_codigo int,
    medicamento_nombre text,
    medicamento_dosis text,
    PRIMARY KEY (receta_fecha_emision, receta_id, medicamento_codigo)
);

-- Create a table6 -> pacientes que tienen una alergia en concreto 
CREATE TABLE IF NOT EXISTS israelbru.tabla6  (
	paciente_dni int,
    paciente_nombre text,
    paciente_alergia text,
    PRIMARY KEY (paciente_alergia, paciente_dni)
);

-- Creamos las entidades para la parte 3 de SoportePaciente
CREATE TABLE IF NOT EXISTS israelbru.SoportePaciente  (
	paciente_dni int,
	cita_id int,
	PRIMARY KEY (cita_id, paciente_dni)
);

CREATE TABLE IF NOT EXISTS israelbru.SoporteMedico  (
	medico_dni int ,
    cita_id int,
	PRIMARY KEY (cita_id, medico_dni)
);

-- Almacenamos las especialidades de cada medico
CREATE TABLE IF NOT EXISTS israelbru.Medico  (
	medico_dni int,
    medico_nombre text,
    medico_fecha_nac date,
    medico_tlf text,
    medico_especialidades set<text>,
    PRIMARY KEY (medico_dni)
);