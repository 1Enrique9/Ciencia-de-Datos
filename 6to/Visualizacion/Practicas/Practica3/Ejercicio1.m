earth = readtable('earth-temperatures - earth-temperatures.csv'); disp(earth(1:5, :));
% Años
years = earth.Year;

% Temperaturas de la Tierra
temp_tierra = earth.Land;

% Temperaturas de los océanos
temp_oce = earth.Ocean;

%Grafico de linea 
figure;
plot(years, temp_tierra,'r', 'LineWidth', 2, 'DisplayName', 'Tierra');
hold on;
plot(years, temp_oce,'c', 'LineWidth', 2, 'DisplayName', 'Océanos');
xlabel('Año');
ylabel('°C');
title('Temperatura a lo largo de los años ');
legend('show');
grid on;
xlim([min(years), max(years)]); % Limitar del ptimer año al ultimo del dataset 
hold off;

