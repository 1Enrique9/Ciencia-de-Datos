elevations = imread('elevations.tif');
elevations = double(elevations);

% Obtener las dimensiones de la imagen
[rows, cols] = size(elevations);

% Definir las dimensiones reales del área (en metros o kilómetros)
% Estos valores deben ser obtenidos de la información geográfica del área
real_width = 20000;  % Ancho real en metros (por ejemplo, 20 km)
real_height = 20000; % Alto real en metros (por ejemplo, 20 km)
max_elevation = 5500; % Elevación máxima en metros (por ejemplo, 5500 m para el Popo)

% Escalar los ejes X e Y
x = linspace(0, real_width, cols);
y = linspace(0, real_height, rows);
[x, y] = meshgrid(x, y);

% Escalar 
elevations = elevations * (max_elevation / max(elevations(:)));

% Graficar la superficie 
figure;
surf(x, y, elevations, 'EdgeColor', 'none'); % Superficie suave
hold on;

% Curvas de nivel en 3D
contour_levels = 20; 
contour3(x, y, elevations, contour_levels, 'LineWidth', 2, 'LineColor', 'k'); 

xlabel('X (km)');
ylabel('Y (km)');
zlabel('Elevación (km)');
title('Popocatépetl e Iztaccíhuatl');
colorbar;
shading interp;

% Ajusta la vista 
axis equal; 
view(3);  
hold off;
