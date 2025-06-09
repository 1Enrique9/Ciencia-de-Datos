filename = 'windVectors - windVectors.csv'; 
data = readtable(filename);

% Columnas de interés
latitudes = data.latitude;
longitudes = data.longitude;
direcciones = data.dir; 
velocidades = data.speed; 

% Convertimos la dirección del viento a componentes U (este-oeste) y V (norte-sur)
radianes = deg2rad(direcciones);
U = velocidades .* cos(radianes); 
V = velocidades .* sin(radianes); 

% Límites del mapa basados en los datos
latlim = [min(latitudes) max(latitudes)];
lonlim = [min(longitudes) max(longitudes)]; 

figure('Position', [100, 100, 1200, 800]); % Ajusta el tamaño de la figura 
ax = worldmap(latlim, lonlim); % Define los límites del mapa
countries = shaperead('landareas.shp', 'UseGeoCoords', true);

% Límites de los países
geoshow(ax, countries, 'DisplayType', 'polygon', 'FaceColor', [0.9 0.9 0.9], 'EdgeColor', 'k');

% Graficar los vectores de viento usando quiverm con un factor de escala
scale_factor = 2; 
quiverm(latitudes, longitudes, U, V, 'Scale', scale_factor, 'Color', 'r');
title('Vectores de viento en Noreste de Europa');
xlabel('Longitud');
ylabel('Latitud');