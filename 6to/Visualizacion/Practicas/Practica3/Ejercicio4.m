% Elipsoide
a = 6;  % eje en x
b = 6;  % eje en y 
c = 7;  % eje en z

% Coordenadas esféricas
theta = linspace(0, 2*pi, 100);
phi = linspace(0, pi, 100);
[theta, phi] = meshgrid(theta, phi);

% Coordenadas cartesianas
x = a * sin(phi) .* cos(theta);
y = b * sin(phi) .* sin(theta);
z = c * cos(phi);

% Grafica
figure;
surf(x, y, z);
axis equal;
xlabel('X');
ylabel('Y');
zlabel('Z');
title('Elipsoide de Revolución');
colormap('winter');
shading interp;
colorbar;

% Hiperboloide
a = 6;  % Eje en x
b = 6;  % Eje en y
c = 7;  % Eje en z

% Coordenadas cilíndricas
theta = linspace(0, 2*pi, 100);
v = linspace(-5, 5, 100);
[theta, v] = meshgrid(theta, v);

% Coordenadas cartesianas
x = a * cosh(v) .* cos(theta);
y = b * cosh(v) .* sin(theta);
z = c * sinh(v);

% Grafica
figure;
surf(x, y, z, z);  % z, nos dice la asignacion de colores 
axis equal;
xlabel('X');
ylabel('Y');
zlabel('Z');
title('Hiperboloide de Revolución');
colormap('winter');  
shading interp;
colorbar;  