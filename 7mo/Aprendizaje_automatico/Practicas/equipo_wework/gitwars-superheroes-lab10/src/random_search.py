import numpy as np
from itertools import product
from .orchestrator import evaluate_svm, evaluate_rf, evaluate_mlp

def random_search(model_name, n_iter=13):
    grids = {
        'svm': {'p1': [0.1, 1, 10, 100], 'p2': [0.001, 0.01, 0.1, 1]},
        'rf':  {'p1': [10, 20, 50, 100], 'p2': [2, 4, 6, 8]},
        'mlp': {'p1': [0, 1, 2, 3],      'p2': [1e-4, 1e-3, 1e-2]}
    }
    mlp_archs = {0: (16,), 1: (32,), 2: (64,), 3: (32, 16)}
    
    vals = list(grids[model_name].values())
    X_grid = np.array(list(product(*vals)))
    
    idxs = np.random.choice(len(X_grid), min(n_iter, len(X_grid)), replace=False)
    chosen = X_grid[idxs]
    
    best_rmse = float('inf')
    best_params = None
    
    for p in chosen:
        if model_name == 'svm':   res = evaluate_svm(p[0], p[1])
        elif model_name == 'rf':  res = evaluate_rf(p[0], p[1])
        elif model_name == 'mlp': res = evaluate_mlp(mlp_archs[int(p[0])], p[1])
        
        if res < best_rmse:
            best_rmse = res
            best_params = p
            
    if model_name == 'mlp':
        return (mlp_archs[int(best_params[0])], best_params[1]), best_rmse
    return tuple(best_params), best_rmse