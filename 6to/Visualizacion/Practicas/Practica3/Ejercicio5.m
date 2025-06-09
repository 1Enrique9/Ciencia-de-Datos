data = load('MRI.mat');
% Verificar si la variable 'Vol' existe(Este paso lo agrege por que lo hice en el Matlab en linea y me cargaba con D 
% como dimension pero luego lo hice localmente por que no sabia como guardarlo y me decia que no existia la variable)
if isfield(data, 'Vol')
    Vol = data.Vol; 
else
    error('La variable Vol no existe en MRI.mat.');
end

% Extraemos los datos de la imagen
if isfield(Vol, 'D')
    D = squeeze(Vol.D); % Eliminamos dimensiones que no necesitemos
else
    error('No se encontró la variable D dentro de Vol.');
end

% Animación
figure;
colormap("gray"); %Mapa de colores 

for k = 1:size(D,3)
    imagesc(D(:,:,k)); % Mostramos cada corte
    axis equal off;
    title(['Corte ', num2str(k)]);
    pause(0.1); % Pausa para una mejor visualizacion de cada imagen 
end

