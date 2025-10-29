import numpy as np
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor


class SimpleRandomForest:
    def __init__(self, base_model='classifier', n_estimators=10, max_features='sqrt', random_state=None):
        self.n_estimators = n_estimators
        self.max_features = max_features
        self.random_state = random_state
        self.trees = []
        self.features_indices = []
        self.is_classifier = base_model == 'classifier'

        if self.is_classifier:
            self.model_class = DecisionTreeClassifier
        else:
            self.model_class = DecisionTreeRegressor

        np.random.seed(self.random_state)

    def _bootstrap_sample(self, X, y):
        n_samples = X.shape[0]
        indices = np.random.choice(n_samples, size=n_samples, replace=True)
        return X[indices], y[indices]

    def _get_feature_subset(self, X):
        n_features = X.shape[1]
        if self.max_features == 'sqrt':
            n_sub_features = int(np.sqrt(n_features))
        elif self.max_features == 'log2':
            n_sub_features = int(np.log2(n_features))
        elif isinstance(self.max_features, int):
            n_sub_features = self.max_features
        else:
            n_sub_features = n_features

        feature_idx = np.random.choice(n_features, n_sub_features, replace=False)
        return feature_idx

    def fit(self, X, y):
        self.trees = []
        self.features_indices = []

        for i in range(self.n_estimators):
            X_sample, y_sample = self._bootstrap_sample(X, y)
            feat_idx = self._get_feature_subset(X_sample)

            tree = self.model_class(random_state=self.random_state)
            tree.fit(X_sample[:, feat_idx], y_sample)

            self.trees.append(tree)
            self.features_indices.append(feat_idx)

    def predict(self, X):
        all_preds = []

        for tree, feat_idx in zip(self.trees, self.features_indices):
            preds = tree.predict(X[:, feat_idx])
            all_preds.append(preds)

        all_preds = np.array(all_preds)

        if self.is_classifier:
            # Votación mayoritaria
            from scipy.stats import mode
            y_pred, _ = mode(all_preds, axis=0)
            return y_pred.flatten()
        else:
            # Promedio para regresión
            return np.mean(all_preds, axis=0)
