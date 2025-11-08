from sklearn.ensemble import AdaBoostClassifier
from sklearn.tree import DecisionTreeClassifier

def train_adaboost_sklearn(X_train, y_train, n_estimators=50, learning_rate=1.0):
    """
    Entrena un AdaBoostClassifier de scikit-learn con árbol débil de profundidad 1.
    Devuelve el modelo entrenado.
    """
    base = DecisionTreeClassifier(max_depth=1, random_state=42)

    ada = AdaBoostClassifier(
        estimator=base,
        n_estimators=n_estimators,
        learning_rate=learning_rate,
        algorithm="SAMME",  # evita la advertencia de SAMME.R
        random_state=42
    )
    ada.fit(X_train, y_train)
    return ada