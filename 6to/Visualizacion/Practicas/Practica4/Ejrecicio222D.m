elevaciones = imread('elevations.tif');
figure;

% Curvas de nivel 
contour(elevaciones);
title('Curvas de Nivel en volcanes');
xlabel('Eje X');
ylabel('Eje Y');
colorbar;
