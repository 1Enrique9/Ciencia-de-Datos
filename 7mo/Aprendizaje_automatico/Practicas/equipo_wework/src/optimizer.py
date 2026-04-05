import numpy as np
from .orchestrator import evaluate_svm, evaluate_rf, evaluate_mlp

# Kernel RBF
def rbf_kernel(x1, x2, length_scale=1.0):
    """
    Calcula la similitud entre dos puntos (o matrices de puntos).
    Fórmula: exp(-||x1 - x2||^2 / (2 * l^2)).
    """
    sq_dist = np.sum(x1**2, 1).reshape(-1, 1) + np.sum(x2**2, 1) - 2 * np.dot(x1, x2.T)
    return np.exp(-0.5 / length_scale**2 * sq_dist)

# Ajuste del GP
def fit_gp(X, y, length_scale=1.0, noise=1e-6):
    """
    Ajusta el GP resolviendo el sistema lineal (K + noise*I)^-1 * y.
    Retorna: Kernel matriz inversa (K_inv) y datos de entrenamiento para usar en predicción.
    """
    K = rbf_kernel(X, X, length_scale)
    K_noisy = K + noise * np.eye(len(X))
    
    alpha = np.linalg.solve(K_noisy, y)
    return K_noisy, alpha, X, y

# Predicción del GP
def gp_predict(X_train, y_train, X_test, length_scale=1.0, noise=1e-6):
    """
    Predice media y varianza para nuevos puntos X_test.
    Mu = K_star.T * alpha 
    Sigma = K_star_star - K_star.T * K_inv * K_star
    """
    K_inv, alpha, _, _ = fit_gp(X_train, y_train, length_scale, noise)
    
    # Similitud de entrenamiento a prueba
    K_star = rbf_kernel(X_train, X_test, length_scale)
    
    
    K_star_star = rbf_kernel(X_test, X_test, length_scale)
    
    # Media
    mu = K_star.T.dot(alpha)
    
    # Varianza
    v = np.linalg.solve(K_inv, K_star)
    sigma_sq = np.diag(K_star_star) - np.diag(K_star.T.dot(v))
    
    return mu.flatten(), sigma_sq

# Adquisición UCB
def acquisition_ucb(mu, sigma, kappa=2.0):
    """
    Calcula el score UCB. Queremos MINIMIZAR el RMSE, pero UCB suele ser para maximización.
    Ojo: En regresión de error (RMSE), buscamos el valor más BAJO.
    Para adaptar la lógica estándar de BO (maximizar utilidad):
    Podemos trabajar con el negativo del RMSE (-RMSE) para MAXIMIZAR.
    
    Si usamos -RMSE:
    UCB = mu(neg_rmse) + kappa * sigma
    """
    return mu + kappa * sigma

def optimize_model(model_name, n_init=3, n_iter=10):
    """
    Ciclo principal de BO.
    """
    grids = {
        'svm': {
            'C': [0.1, 1, 10, 100],
            'gamma': [0.001, 0.01, 0.1, 1]
        },
        'rf': {
            'n_estimators': [10, 20, 50, 100],
            'max_depth': [2, 4, 6, 8]
        },
        'mlp': {
            'hidden_layer_sizes_idx': [0, 1, 2, 3], 
            'alpha': [1e-4, 1e-3, 1e-2]
        }
    }
    
    mlp_layers = {0: (16,), 1: (32,), 2: (64,), 3: (32, 16)}

    param_grid = grids[model_name]
    keys = list(param_grid.keys())
    
    from itertools import product
    grid_values = list(product(*[param_grid[k] for k in keys]))
    X_grid = np.array(grid_values) 
    
    initial_indices = np.random.choice(len(X_grid), n_init, replace=False)
    X_observed = X_grid[initial_indices]
    
    y_observed = []
    
    def evaluate_real(params_array):
        p1, p2 = params_array
        if model_name == 'svm':
            metric = evaluate_svm(C=p1, gamma=p2)
        elif model_name == 'rf':
            metric = evaluate_rf(n_estimators=p1, max_depth=p2)
        elif model_name == 'mlp':
            layers = mlp_layers[int(p1)] 
            metric = evaluate_mlp(hidden_layer_sizes=layers, alpha=p2)
        
        return -metric 
    
    for x in X_observed:
        y_observed.append(evaluate_real(x))
    
    y_observed = np.array(y_observed)

    history = []

    for i in range(n_iter):
        X_obs_norm = (X_observed - X_grid.mean(0)) / (X_grid.std(0) + 1e-9)
        X_grid_norm = (X_grid - X_grid.mean(0)) / (X_grid.std(0) + 1e-9)
        
        mu, sigma_sq = gp_predict(X_obs_norm, y_observed, X_grid_norm)
        sigma = np.sqrt(np.maximum(sigma_sq, 1e-9))
        

        ucb_values = acquisition_ucb(mu, sigma, kappa=2.0)
        
        best_idx = np.argmax(ucb_values)
        next_x = X_grid[best_idx]
        
        next_y = evaluate_real(next_x)
    
        X_observed = np.vstack([X_observed, next_x])
        y_observed = np.append(y_observed, next_y)
        
        history.append({
            'params': next_x,
            'rmse': -next_y
        })

    best_idx = np.argmax(y_observed)
    best_params_arr = X_observed[best_idx]
    best_metric = -y_observed[best_idx]
    
    if model_name == 'mlp':
        final_params = (mlp_layers[int(best_params_arr[0])], best_params_arr[1])
    else:
        final_params = tuple(best_params_arr)

    return final_params, best_metric, history