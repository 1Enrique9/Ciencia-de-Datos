elevations = double(imread('elevations.tif')); 

% Suavizamos la imagen y reducir ruido
sigma = 2;
elevations = imgaussfilt(elevations, sigma);

% Normalizamos la imagen entre 0 y 1 
elevations = (elevations - min(elevations(:))) / (max(elevations(:)) - min(elevations(:)));

% Límites del color para mejorar el contraste
cmin = 0.1; 
cmax = 0.9; 

% Mostrar la imagen con 16 niveles de color
figure;
imagesc(elevations);
colormap(turbo(16));
colorbar;
caxis([cmin cmax]); 
axis equal; % Proporción de aspecto
axis off; 
title('Mapa de elevaciones');
shading interp; % Suaviza las transiciones de color
