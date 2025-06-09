clc; clear; close all;
data = load('curve_tab.txt');
X = data(:,1);
Y = data(:,2);

% Generar más puntos 
ti = linspace(1, length(X), length(X) * 5); 
X_interp = interp1(1:length(X), X, ti, 'spline');
Y_interp = interp1(1:length(Y), Y, ti, 'spline');

% Derivadas con SplineDerivativeT
[dxdt, dydt, dxxdt2, dyydt2] = SplineDerivativeT(X_interp,Y_interp);

% Curvatura
curvature = abs(dxdt .* dyydt2 - dydt .* dxxdt2) ./ (dxdt.^2 + dydt.^2).^(3/2);

% Suavizado para quitar ruido en la curvatura
curvature = smoothdata(curvature, 'movmean', 10);
curvature = curvature / max(curvature);

% Grafica
figure;
scatter(X_interp, Y_interp, 10, curvature, 'filled');
colormap(jet);
colorbar;
caxis([0 1]);
title('Niveles de Curvatura entre puntos');
axis equal;

grid on;

function [dxdt, dydt, dxxdt2, dyydt2] = SplineDerivativeT(X,Y)
    t = cumtrapz(sqrt(gradient(X).^2 + gradient(Y).^2));
    fx = spline(t,X);
    fy = spline(t,Y);
    
    d1fx = differentiate(fx);
    d1fy = differentiate(fy);
    d2fx = differentiate(d1fx);
    d2fy = differentiate(d1fy);
    
    ti = linspace(min(t),max(t),length(X));
    
    dxdt = ppval(d1fx,ti);
    dydt = ppval(d1fy,ti);
    dxxdt2 = ppval(d2fx,ti);
    dyydt2 = ppval(d2fy,ti);
    
    dxdt(abs(dxdt)<1e-12) = 0;
    dydt(abs(dydt)<1e-12) = 0;
    dxxdt2(abs(dxxdt2)<1e-12) = 0;
    dyydt2(abs(dyydt2)<1e-12) = 0;
end

function ppdf = differentiate(ppf)
    ppdf = ppf;
    ppdf.order = ppdf.order - 1;
    ppdf.coefs = ppdf.coefs(:,1:end-1) .* (ppdf.order:-1:1);
end

