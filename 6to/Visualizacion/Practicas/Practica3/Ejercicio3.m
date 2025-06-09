x = linspace(-pi, pi, 100);
y = linspace(-pi, pi, 100);
[X, Y] = meshgrid(x, y);
% Función Z que se nos pide 
Z = sin(X) .* cos(Y);

figure;

% Vista 3D
subplot(2, 2, 1);
surf(X, Y, Z);
colormap(jet); 
caxis([-1, 1]); 
title('Vista 3D');
xlabel('x');
ylabel('y');
zlabel('f(x, y)');
shading interp;

% Plano XY
subplot(2, 2, 2);
surf(X, Y, Z);
view(0, 90);
colormap(jet); 
caxis([-1, 1]); 
title('Plano XY');
xlabel('x');
ylabel('y');
shading interp;

% Plano XZ
subplot(2, 2, 3);
surf(X, Y, Z);
view(0, 0);
colormap(jet); 
caxis([-1, 1]); 
title('Plano XZ');
xlabel('x');
zlabel('f(x, y)');
shading interp;

% Plano YZ
subplot(2, 2, 4);
surf(X, Y, Z);
view(90, 0);
colormap(jet); 
caxis([-1, 1]); 
title('Plano YZ');
ylabel('y');
zlabel('f(x, y)');
shading interp;

% Barra de colores general(Posicion y tamaño)
colorbar('Position', [0.93 0.1 0.02 0.8]); 
