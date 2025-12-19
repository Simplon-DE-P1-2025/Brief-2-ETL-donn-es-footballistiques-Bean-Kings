# Règles de Nettoyage et Standardisation - FIFA World Cup ETL

Ce document décrit les règles appliquées pour nettoyer et standardiser les données des matchs de la Coupe du Monde pour tous les fichiers source.

---

## 1. Date (`Datetime`)
- **Mise en forme** :
    - Format timestamp ISO 8601 format `YYYY-MM-DD hh:mm:ss`
- **Fusion** : Si la date et l'heure sont séparées, les concaténer.
- **Validation** : Valeurs manquantes autorisées ?

## 2. Étape (`Stage`) -- sera nommé `round` dans la base finale
- **Mise en forme** :
  - Tout en minuscules (`lowercase`).
  - Pas d'espaces superflus (`trim`).
  - Pas de caractères spéciaux (remplacés ou supprimés).
- **Valeurs standardisées** : `group`, `round of 16`, `quarter-final`, `semi-final`, `play-off for third place`, `final`, `final-round` (seulement Caroline 1950 Brazil)
  - Réaliser un check manuel sur les valeurs uniques pour voir s'il reste des erreurs / doublons
- **Exemple** :
  - Entrée : `"ROUND16"`
  - Sortie : `"round of 16"`

## 3. Noms de Villes (`City`)
- **Mise en forme** :
  - Tout en minuscules (`lowercase`).
  - Pas d'espaces superflus (`trim`).
  - Pas de caractères spéciaux (remplacés avec une fonction qui existerait, à chercher).
  - Noms en anglais -> Utiliser `geonamescache`
- **Valeurs manquantes** : Remplacées par `"Unknown"` (avec log).

## 4. Noms équipe domicile (`Home Team Name`)
- Même règles que les villes.
- **Valeurs manquantes** : Remplacées par `"Unknown"` (avec log).

## 5. Nb buts équipe domicile (`Home Team Goals`)
- Les valeurs de `Home Team Goals` doivent être des entiers ≥ 0.
- Remplacer les valeurs manquantes par `None` si match annulé (ou autre subtilité).

## 6. Nb buts équipe extérieur (`Away Team Goals`)
- Les valeurs de `Away Team Goals` doivent être des entiers ≥ 0.
- Remplacer les valeurs manquantes par `None` si match annulé (ou autre subtilité).

## 7. Noms équipe extérieur (`Away Team Name`)
- Même règles que les villes.
- **Valeurs manquantes** : Remplacées par `"Unknown"` (avec log).

## 8. Résultat équipe domicile (`Home Result`)
- Les valeurs de `Home / Away Team Goals` doivent être des entiers ≥ 0.
- Si home/away team > goals que away/home alors `winner`
- Si home/away team = goals que away/home alors `draw`
- Si home/away team < goals que away/home alors `loser`
- Cas spécifiques (pénalties / prolongations) : mettre `draw` si score nul à la fin du temps réglementaire

## 9. Résultat équipe extérieur (`Away Result`)
- Les valeurs de `Home / Away Team Goals` doivent être des entiers ≥ 0.
- Si home/away team > goals que away/home alors `winner`
- Si home/away team = goals que away/home alors `draw`
- Si home/away team < goals que away/home alors `loser`
- Cas spécifiques (pénalties / prolongations) : mettre `draw` si score nul à la fin du temps réglementaire

---

## Gestion des Doublons dans nos DataFrames finalisées
- Ajout manuel de quelques dates dans le dataset 1930-2010 (Caroline)

## Gestion des pays participants
- Liste à laquelle se reporter :
[
  "algeria",
  "angola",
  "argentina",
  "australia",
  "austria",
  "belgium",
  "bolivia",
  "bosnia and herzegovina",
  "brazil",
  "bulgaria",
  "cameroon",
  "canada",
  "chile",
  "china",
  "colombia",
  "costa rica",
  "croatia",
  "cuba",
  "czech republic",
  "czechoslovakia",
  "denmark",
  "dutch east indies",
  "east germany",
  "ecuador",
  "egypt",
  "el salvador",
  "england",
  "france",
  "germany",
  "ghana",
  "greece",
  "haiti",
  "honduras",
  "hungary",
  "iceland",
  "iran",
  "iraq",
  "israel",
  "italy",
  "ivory coast",
  "jamaica",
  "japan",
  "kuwait",
  "mexico",
  "morocco",
  "netherlands",
  "new zealand",
  "nigeria",
  "north korea",
  "northern ireland",
  "norway",
  "panama",
  "paraguay",
  "peru",
  "poland",
  "portugal",
  "qatar",
  "republic of ireland",
  "romania",
  "russia",
  "saudi arabia",
  "scotland",
  "senegal",
  "serbia",
  "serbia and montenegro",
  "slovakia",
  "slovenia",
  "south africa",
  "south korea",
  "soviet union",
  "spain",
  "sweden",
  "switzerland",
  "togo",
  "trinidad and tobago",
  "tunisia",
  "turkey",
  "ukraine",
  "united arab emirates",
  "united states",
  "uruguay",
  "wales",
  "west germany",
  "yugoslavia",
  "zaire"
]