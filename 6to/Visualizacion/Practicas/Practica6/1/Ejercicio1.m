data = readtable('temperatureAnomalies.csv');
dates = data.Date;
anomalies = data.Anomaly;
dates = datetime(dates, 'InputFormat', 'yyyy-MM-dd');

% Grafica
figure;
plot(dates, anomalies, '-', 'LineWidth', 1.5);
ylabel('°C');
title('Variaciones a lo largo de la historia');
grid on;
legend('Temperatura', 'Location', 'northwest');