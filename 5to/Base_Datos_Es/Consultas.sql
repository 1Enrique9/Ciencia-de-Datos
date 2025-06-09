CREATE TABLE ESTADO(
       ides INT PRIMARY KEY, 
	   nes VARCHAR(100)
);
COPY ESTADO(ides, nes)
FROM 'D:/Ciencia de Datos/5to/Base_Datos_Es/Estado.csv'
DELIMITER ','
CSV HEADER;
select * from ESTADO
CREATE TABLE MUNICIPIO(
       idmun INT, 
	   nmun VARCHAR(100),
	   lat NUMERIC,
	   lon NUMERIC,
	   dh INT, 
	   ides INT NOT NULL,
	   PRIMARY KEY(idmun, ides),
	   FOREIGN KEY (ides) REFERENCES ESTADO(ides)
);
COPY MUNICIPIO(idmun, nmun, lat, lon, dh, ides)
FROM 'D:/Ciencia de Datos/5to/Base_Datos_Es/Municipios.csv'
DELIMITER ','
CSV HEADER;
select * from MUNICIPIO

CREATE TABLE CLIMA(
       dloc DATE,
	   cc NUMERIC,
	   desciel VARCHAR(30),
	   dirvienc VARCHAR(20),
	   dirvieng NUMERIC, 
	   ndia VARCHAR(20), 
	   prec NUMERIC,
	   probprec INT, 
	   tmax NUMERIC, 
	   tmin NUMERIC, 
	   velvien NUMERIC, 
	   raf INT, 
	   idmun INT NOT NULL, 
	   ides INT NOT NULL, 
	   PRIMARY KEY(idmun, ides, dloc),
	   FOREIGN KEY (ides) REFERENCES ESTADO(ides),
	   FOREIGN KEY (idmun, ides) REFERENCES MUNICIPIO(idmun, ides)
);

ALTER TABLE CLIMA DROP raf;
copy CLIMA(cc, desciel, dirvienc, dirvieng, dloc, ndia, prec, probprec, tmax, tmin, velvien, idmun, ides)
FROM 'D:/Ciencia de Datos/5to/Base_Datos_Es/clima.csv'
DELIMITER ','
CSV HEADER;
select * from CLIMA

SELECT c1.idmun, c1.ides, c1.dloc AS fecha_hoy, c2.dloc AS fecha_mañana, c1.tmin AS tmin_hoy,
       c2.tmin AS tmin_mañana, (c1.tmin - c2.tmin) AS descenso
FROM CLIMA c1 JOIN CLIMA c2 ON c1.idmun = c2.idmun  AND c1.ides = c2.ides  AND c2.dloc = c1.dloc + INTERVAL '1 day'
WHERE c1.tmin > c2.tmin  ORDER BY descenso DESC LIMIT 5;

WITH ranking AS (
    SELECT
        ides,
        idmun,
        tmax,
        DENSE_RANK() OVER (PARTITION BY ides ORDER BY tmax DESC) AS rank
    FROM
        CLIMA
)
SELECT
    ides,
    idmun,
    tmax
FROM
    ranking
WHERE
    rank = 4;



