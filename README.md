# Analyzer

L'analyzer se décompose en plusieurs étapes:
- recensement des fichiers pour chaque année et chaque marche
- recensement des compagnies en omettant les compagnies qui apparaissent et disparaissent le même jour et insertion des valeurs dans la table correspondante
- recensement des valeurs de stocks et insertion des valeurs dans la table correspondante après nettoyage
- remplissage de la table daystocks à partir des valeurs de la table stocks 

# Dashboard

Nous avons décidé d'opter pour un style minimaliste pour le tableau de bord, ce qui offre une navigation claire et précise entre chaque module. L'objectif était de ne pas surcharger l'écran avec des données analytiques afin de ne pas perturber l'utilisateur. En adoptant ce design épuré, nous visons à faciliter l'accès aux informations essentielles tout en maintenant une expérience utilisateur intuitive et agréable. Les éléments superflus ont été supprimés, ce qui permet aux utilisateurs de se concentrer sur les données importantes et de naviguer facilement à travers les différentes sections du tableau de bord.

Il contient les sections, **Cours de l'action**, **Bandes de Bollinger**, **Données brutes**, **YTD**. Nous avons décidé d'ajouter le YTD puisqu'il nous semblait intéressant de connaître l'augmentation (en %) depuis le début de l'année actuelle du montant de l'action choisie.

## Spécificités

Chaque section contient un bandeau de sélection permettant à l'utilisateur de choisir :
- le marché
- la companie
- la date de début et de fin (qui est initialisé avec les min et max des companies choisies)
- le type de graphique (pour le cours de l'action uniquement)

Ensuite, le graphique correspondant s'affiche pour visualiser les données saisies dans la section précédente. Si les informations saisies ne renvoient aucune donnée, le tableau de bord vous en informera.

Enfin, une section informative affiche les symboles des entreprises sélectionnées, même si elles n'ont aucune donnée, permettant à l'utilisateur de rechercher ces informations sur Internet en utilisant les symboles fournis.

# Comment utiliser ?
