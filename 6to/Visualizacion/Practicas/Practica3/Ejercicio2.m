int = linspace(0, 25*pi, 1000);
% Funciones x1(t) y x2(t)
x_1 = (25/2) * exp(-int/25) + (25/2) * exp(-3*int/25);
x_2 = 25 * exp(-int/25) + 25 * exp(-3*int/25);
% Grafica 
figure;
plot(int, x_1, 'LineWidth', 1.5, 'MarkerSize', 5, 'DisplayName', 'x_1'); 
hold on;
plot(int, x_2, 'LineWidth', 1.5, 'MarkerSize', 5, 'DisplayName', 'x_2');
hold off;
xlabel('Tiempo');
ylabel('Amplitud');
title('Gráfica  en el intervalo [0, 25\pi]');
legend('show');
grid on;
