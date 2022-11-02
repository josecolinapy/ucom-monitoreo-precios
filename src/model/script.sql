/* ---------------------------------------------------- */
/*  Generated by Enterprise Architect Version 15.2 		*/
/*  Created On : 01-nov.-2022 00:30:38 				*/
/*  DBMS       : PostgreSQL 						*/
/* ---------------------------------------------------- */

/* Drop Sequences for Autonumber Columns */

DROP SEQUENCE IF EXISTS productos_precios_historico_id_seq
;

/* Drop Tables */

DROP TABLE IF EXISTS productos_precios CASCADE
;

DROP TABLE IF EXISTS productos_precios_historico CASCADE
;

/* Create Tables */

CREATE TABLE productos_precios
(
	id bigint NOT NULL,
	nombre text NOT NULL,
	precio numeric(15) NOT NULL   DEFAULT 0,
	categoria varchar(150) NULL,
	unidad_medida varchar(150) NULL,
	tamanho varchar(50) NULL,
	entidad_comercial varchar(50) NULL,
	etiqueta text NULL,
	codigo_barra varchar(50) NULL
)
;

CREATE TABLE productos_precios_historico
(
	id bigint NOT NULL   DEFAULT NEXTVAL(('"productos_precios_historico_id_seq"'::text)::regclass),
	fecha_registro timestamp with time zone NOT NULL,
	fecha_recibido timestamp with time zone NOT NULL,
	nombre text NOT NULL,
	precio numeric(15) NOT NULL   DEFAULT 0,
	categoria varchar(150) NULL,
	unidad_medida varchar(150) NULL,
	tamanho varchar(50) NULL,
	entidad_comercial varchar(50) NULL,
	etiqueta text NULL,
	codigo_barra varchar(50) NULL
)
;

/* Create Primary Keys, Indexes, Uniques, Checks */

ALTER TABLE productos_precios ADD CONSTRAINT "PK_productos_precios"
	PRIMARY KEY (id)
;

CREATE INDEX idx_nombre_prod_precios ON productos_precios (nombre ASC)
;

ALTER TABLE productos_precios_historico ADD CONSTRAINT "PK_productos_precios_historico"
	PRIMARY KEY (id,fecha_registro,fecha_recibido)
;

CREATE INDEX idx_nombre ON productos_precios_historico (nombre ASC)
;

/* Create Table Comments, Sequences for Autonumber Columns */

CREATE SEQUENCE productos_precios_historico_id_seq INCREMENT 1 START 1
;
