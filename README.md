
# Rendu Exam Spark

## Exercice 1

Pour l'Exercice 1, j'ai réalisé deux classements:
1. Classement des vidéos des 10 chaînes les plus populaires triées par ratio like-dislike pour chaque chaîne.
2. Classement des vidéos des 10 chaînes les plus populaires triées par ratio like-dislike pour toutes les chaînes combinées.

## Exercice 2

Pour l'Exercice 2, j'ai supprimé les valeurs négatives des différences de `running_total` car j'essayais de reproduire l'exemple donné dans l'examen.

## Fichiers

1. `main.py`: Ce fichier contient le code en Python et Spark.
   - Pour exécuter ce fichier: `python main.py`
2. `app.py`: Ce fichier utilise Streamlit pour une meilleure visualisation. Il utilise aussi Pandas pour afficher les résultats (aucun calcul n'est fait avec Pandas).
   - Pour lancer l'application Streamlit: `streamlit run app.py`

## Aperçu

### Top 10 Channels with Most Trending Videos
![Top 10 Channels](./1)

### Running Total Differences
![Running Total Differences](./2)

### Salary Differences by Department
![Salary Differences by Department](./3)

### Pivot Data
![Pivot Data](./4)
