#!/bin/
HEAD
# Guardar nombre de la rama actual (ej. main)
original_branch=$(git rev-parse --abbrev-ref HEAD)

# Crear y cambiar a una rama temporal
git checkout -b temp-push-branch

# Obtener los commits pendientes de subir
commits=$(git rev-list origin/$original_branch..HEAD --reverse)
echo "Se encontraron $(echo "$commits" | wc -l) commits por subir."

# Iterar sobre cada commit
for commit in $commits; do
    echo "Aplicando commit $commit..."

    # Resetear la rama temporal a ese commit
    git reset --hard $commit

    # Intentar hacer push
    if git push origin HEAD:$original_branch; then
        echo "✅ Commit $commit subido exitosamente."
    else
        echo "⚠️ Error al subir commit $commit. Ignorando y continuando..."
    fi
done

# Cambiar de vuelta a la rama original
git checkout $original_branch

# Fusionar solo los commits que sí lograron subir
git pull origin $original_branch

echo "🚀 Todos los commits posibles fueron enviados a '$original_branch'. Revisa los errores arriba para corregir los fallidos."

# Obtiene la lista de commits que aún no han sido enviados al remoto
COMMITS=$(git log origin/main..HEAD --pretty=format:"%H" | tac)
TOTAL=$(echo "$COMMITS" | wc -l)
echo "Se encontraron $TOTAL commits por subir."

# Crea una nueva rama temporal para empujar los commits uno por uno
git checkout -b temp-push-branch

WARNINGS=()

for COMMIT in $COMMITS; do
    echo "Aplicando commit $COMMIT..."

    # Resetea al commit anterior (soft reset)
    git reset --soft $COMMIT^
    git commit -C $COMMIT

    # Intenta hacer push del commit actual
    if git push origin HEAD:main; then
        echo "✔ Push exitoso del commit $COMMIT"
    else
        echo "❌ Error al hacer push del commit $COMMIT"
        WARNINGS+=("$COMMIT")
    fi
done

# Cambiar de nuevo a main
git checkout main

echo "-------------------------------"
echo "✔ Proceso completado."
echo "Commits con errores al hacer push:"
for warn in "${WARNINGS[@]}"; do
    echo "⚠ Commit fallido: $warn"
done

temp-push-branch
