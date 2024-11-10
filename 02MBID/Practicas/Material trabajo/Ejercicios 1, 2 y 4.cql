CREATE TABLE practica2.ejercicio1 (
	id_pedido int,
	id_producto int,
	pedido_fecha date,
	PRIMARY KEY (id_pedido, id_producto)
);
CREATE TABLE practica2.ejercicio2 (
	propiedad text,
	producto_id int,
	PRIMARY KEY (propiedad, producto_id)
);

CREATE TABLE practica2.soporte5 (
	direccion text,
	id int,
	PRIMARY KEY (direccion, id)
);
