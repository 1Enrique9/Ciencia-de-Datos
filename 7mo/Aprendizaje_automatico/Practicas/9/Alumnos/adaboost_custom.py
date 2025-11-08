import numpy as np
from sklearn.tree import DecisionTreeClassifier

class SimpleAdaBoost:
    def __init__(self, base_estimator=None, n_estimators=50):
        self.n_estimators = n_estimators
        self.base_estimator = base_estimator or DecisionTreeClassifier(max_depth=1)
        self.alphas = []
        self.models = []
        self.errors = [] 

    def fit(self, X, y):
        n_samples = X.shape[0]
        D = np.ones(n_samples) / n_samples

        for t in range(self.n_estimators):
            h_t = self.base_estimator.__class__(**self.base_estimator.get_params())
            h_t.fit(X, y, sample_weight=D)

            y_pred = h_t.predict(X)
            err_t = np.sum(D * (y_pred != y)) / np.sum(D)
            err_t = max(err_t, 1e-10)  # evita dividir entre cero

            alpha_t = 0.5 * np.log((1 - err_t) / err_t)

            # Actualización de pesos
            D *= np.exp(-alpha_t * y * y_pred)
            D /= np.sum(D)

            # Guardar resultados
            self.models.append(h_t)
            self.alphas.append(alpha_t)
            self.errors.append(err_t)  

            print(f"Iter {t+1}/{self.n_estimators}: ε={err_t:.4f}, α={alpha_t:.4f}")

        return self

    def predict(self, X, return_scores=False):
        pred = np.zeros(X.shape[0])
        for alpha, model in zip(self.alphas, self.models):
            pred += alpha * model.predict(X)

        if return_scores:
            return pred

        y_pred = np.sign(pred)
        y_pred[y_pred == 0] = 1
        y_pred = (y_pred + 1) / 2
        return y_pred.astype(int)
