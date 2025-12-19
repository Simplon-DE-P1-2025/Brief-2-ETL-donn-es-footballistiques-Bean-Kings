# Brief-2-ETL-donn-es-footballistiques-Bean-Kings
Second brief data ingénieur p1

## Choix de technologie: 
Au vu de la demande client sur le livrable attendu : 
```
- Une table/collection finale contient exactement les colonnes/champs suivants  sur laquelle vous sortirez quelques KPI !
id_match : identifiant séquentiel (1 = premier match historique ; dernier = finale 2022 France–Argentine)
home_team : équipe 1
away_team : équipe 2
home_result : buts équipe 1
away_result : buts équipe 2
result : vainqueur (home_team / away_team) ou "draw"
date : date du match
round : tour (poules, 8e, 1/4, 1/2, finale, etc.)
city : ville
edition : édition (ex: “1930”, “2014”, “2022”, etc.)
```
Nous remarquons que certaines entités se démarquent (équipes, lieux, dates ...) se qui se prête bien a un modèle entité-relation.
## Schema de base de données
### MCD
![Mcd](/asset/mcd_etl.jpg)
### MLD
![Mcd](/asset/mld_etl.jpg)
## Diagram de déploiement
![Deploiement](/asset/deployment_diagram.png)
## Utilisation de l'etl (docker)
L'application se lance avec docker. Deux volumes sont créés
- un premier pour les fichiers csv et json a utiliser comme source de données
- un second pour les stocker les donnes de la base de données
Pour interagir avec l'application : 
1. s'assurer que docker est installé : [télécharger docker](https://docs.docker.com/desktop/)
2. cloner ce repository
3. dans le terminal entrer la commande suivante pour demarrer le service (lance le script main.py et peuple la base de données avec les data transformées)
```sh
docker compose up --build
```
### Kpi
Les kpi sont trouvable dans le rapport bi joint (dossier asset)
