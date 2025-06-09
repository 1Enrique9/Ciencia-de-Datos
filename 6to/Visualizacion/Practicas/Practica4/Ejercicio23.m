data = imread('elevations.tif');
data = double(data); % Para un mejor calculo 

% Normalizamos entre 0 y 1 de esta manera podemos escalar mejor la imagen y
% manejar mejor los datos 
minE = min(data(:));
maxE = max(data(:));
data_norm = (data - minE) / (maxE - minE);

% Pasamos a kilomettros los pixeles 
mindata = min(data_norm(:));
maxdata = max(data_norm(:));
min_ejez_km_ = 2; 
max_ejez_km_ = 5.4;
data_norm_ejez_km = min_ejez_km_ + (data_norm - mindata) / (maxdata- mindata) * (max_ejez_km_ - min_ejez_km_);
numx = size(data_norm,2);
numy = size(data_norm,1);
x = 0:15:(numx-1)*15;
y = 0:15:(numy-1)*15;

% Por ultimo pasamos las coordenadas a kilómetros, para que a la hora de
% graficar se vea de mejor manera y no tengamos el problema de que se ven
% muy picudos 
x_km = x / 1000;
y_km = y / 1000;
[X, Y] = meshgrid(x_km, y_km);
 
%Grafica
figure;
set(gcf, 'Position', [150, 150, 1300, 800]);

%------------------------------------------------------------
% Subplot 1
subplot(1,2,1);
s1 = surf(X, Y, data_norm_ejez_km, 'EdgeColor', 'none', 'FaceColor', 'interp');
hold on;
colormap(parula); 
shading interp;
lightangle(-45, 30);
camlight('headlight');
lighting gouraud;
contour3(X, Y, I_norm_z_km, 20, 'k'); % Curvas de nivel

title('Vista 1','FontSize',16);
zlabel('Nivel (km)', 'FontSize', 14);
view(-33,2);
axis equal;
grid on;
set(gca, 'XTick', [], 'YTick', [], 'XColor', 'none', 'YColor', 'none');

%------------------------------------------------------------
% Subplot 2
subplot(1,2,2);
s2 = surf(X, Y, data_norm_ejez_km, 'EdgeColor', 'none', 'FaceColor', 'interp');
hold on;
colormap(jet); 
shading interp;
lightangle(-45, 30);
camlight('headlight');
lighting gouraud;
contour3(X, Y, I_norm_z_km, 20, 'k'); % Curvas de nivel

title('Vista 2','FontSize',16);
zlabel('Nivel (km)', 'FontSize', 14);
view(33,0);
axis equal;
grid on;
set(gca, 'XTick', [], 'YTick', [], 'XColor', 'none', 'YColor', 'none');


